"""Codex capture — the SECOND host on the SAME outbox→ingest→recall/inject spine
(AGENT-INTEGRATION-AUDIT-AND-PLAN.md §4 Phase 4 + §6 "background thinking → CodexRolloutTailer";
capture-spec.md §5.4 names Codex as a later-stage host).

**Codex's REAL capture surface (verified empirically against codex-cli 0.146.0).** Codex exposes
TWO capture channels, and this module wires both onto the unchanged spine:

1. **Rollout transcript files** — ``$CODEX_HOME/sessions/YYYY/MM/DD/rollout-<ts>-<uuid>.jsonl``
   (default ``~/.codex/sessions/...``). Every interactive/exec session appends one JSONL record per
   event; the file is complete and append-stable once written. This is the PRIMARY, always-present
   source — a post-hoc tailer (:class:`CodexRolloutTailer`) reads it exactly like the Phase-0B
   :class:`~mu_client.capture.claude_tailer.ClaudeCodeTranscriptTailer` reads Claude Code's
   transcript: complete-lines-only, byte-offset checkpoint, fail-loud on envelope drift.

2. **The ``notify`` program hook** — codex's ``config.toml`` ``notify = ["<program>"]`` key. On
   ``agent-turn-complete`` codex spawns the program with ONE argv: a JSON string
   ``{"type":"agent-turn-complete","thread-id":..,"turn-id":..,"cwd":..,"client":..,
   "input-messages":[..],"last-assistant-message":..}`` (verified: a live ``codex exec`` run wrote
   exactly this). :class:`CodexNotifyParserV1` normalizes that envelope so the installer-written
   shim (``scripts/hooks/mu_codex_notify.sh``) can pipe it into ``mu capture-once --host codex`` —
   the LIVE push path, analogous to Claude Code's ``Stop`` hook. This is best-effort/final-answer
   only (codex's notify carries the last assistant message, not the whole turn); the rollout tailer
   is the complete, authoritative record.

**REAL rollout record shape (verified on-disk, codex 0.146.0).** Each line is one JSON object
``{"timestamp":..,"type":<top>,"payload":<obj>}``. The ``type``/``payload.type`` pairs we capture
(everything else — ``reasoning`` [encrypted], ``token_count``, ``task_started``/``task_complete``,
``world_state``, ``turn_context``, ``*_output``, ``response_item``/``message`` role-echoes — is
SKIPPED so nothing double-captures and no encrypted CoT leaks):

* ``event_msg`` / ``user_message`` → :attr:`ActivityKind.USER_PROMPT` (``payload.message``)
* ``event_msg`` / ``agent_message`` → :attr:`ActivityKind.ASSISTANT_MSG` (``payload.message``;
  ``payload.phase`` — ``commentary`` mid-turn vs ``final_answer`` — carried as provenance)
* ``response_item`` / ``custom_tool_call`` → :attr:`ActivityKind.TOOL_USE`
  (``payload.name`` + truncated ``payload.input``)
* ``response_item`` / ``function_call`` → :attr:`ActivityKind.TOOL_USE`
  (``payload.name`` + truncated ``payload.arguments``)

**Dedup by construction.** Assistant/user text is captured ONLY from the ``event_msg`` semantic
stream (never the parallel ``response_item``/``message`` role-echo of the same content), and tool
calls ONLY from ``response_item`` — the two streams never overlap, so one turn never yields two
memories for the same text. ``activity_id = sha256(host|session|byte_offset|kind)`` (the SAME
deterministic formula as :mod:`~mu_client.capture.parsers`/``claude_tailer``) makes a re-tail of the
same append-stable bytes an idempotent no-op insert.

**Subagent-attribution + importance reuse (Phase 4 brief).** Codex captures flow through the
UNCHANGED :class:`~mu_client.workers.ingest_client.InProcessLocalIngest`, so the SAME subagent
partition + importance-gate machinery Phase 1.5/0B built applies verbatim — a codex activity that
ever carries ``payload["agent_type"]`` lands in an agent-scoped η partition, and ``importance``
threads straight to the engine's ONE ``DeterministicPromoteStage`` gate. This module adds a capture
SOURCE only; it invents no second pipeline, no second gate (mirrors §3B item 4's scoping).

**SchemaDriftError discipline (per-source, loud).** The rollout JSONL is codex-internal; a line
that is not JSON, not an object, or lacks the ``{type, payload}`` envelope halts THIS source only
(:class:`~mu_client.errors.CaptureSchemaDriftError` + a ``CaptureSourceHalted`` event) — an
UNKNOWN ``payload.type`` inside a valid envelope is SKIPPED, not drift (codex adds event kinds
across versions; skipping the unrecognized is correct, mirroring the Claude tailer's
"non-assistant records are skipped, not drift"). A drift never blocks the notify hook / other
sources.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict

from mu_client.capture.model import ActivityKind, CaptureCheckpoint, HostKind, RawActivity
from mu_client.errors import CaptureSchemaDriftError
from mu_client.observability.events import log_activity_captured, log_capture_source_halted
from mu_client.outbox.sqlite_outbox import SqliteOutbox

if TYPE_CHECKING:
    from mu_client.config import ClientSettings

__all__ = [
    "CodexBackfillResult",
    "CodexNotifyParserV1",
    "CodexParserV1",
    "CodexRolloutTailer",
    "CodexTailResult",
    "backfill_codex",
    "session_id_from_rollout_name",
]

_ROLLOUT_SCHEMA_VERSION = "codex.rollout.v1"
_NOTIFY_SCHEMA_VERSION = "codex.notify.v1"
_HOST_VERSION_FALLBACK = "codex/unknown"

# The rollout filename's trailing UUID is the codex session/thread id — the η.session slot when a
# resume tail starts past the session_meta line (which is line 0 and may be before ``since_byte``).
_ROLLOUT_UUID = re.compile(
    r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
)


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _sha256_of(record: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(record, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def _activity_id(*, session_id: str, source_offset: str, kind: ActivityKind) -> str:
    # SAME formula as capture/parsers.py::_activity_id + claude_tailer (host|session|offset|kind) so
    # a re-tail of the same rollout bytes is an idempotent no-op insert (UNIQUE(activity_id)).
    return _sha256_text(f"{HostKind.CODEX.value}|{session_id}|{source_offset}|{kind.value}")


def _provenance_id(*, session_id: str, source_offset: str, kind: ActivityKind) -> str:
    return "prov_" + _activity_id(
        session_id=session_id, source_offset=source_offset, kind=kind
    )[:24]


def _truncate(text: str, max_chars: int) -> str:
    return text if len(text) <= max_chars else text[:max_chars] + "…"


def session_id_from_rollout_name(path: Path) -> str | None:
    """Extract the codex session/thread UUID from a ``rollout-<ts>-<uuid>.jsonl`` filename — the
    η.session a resume tail keys on before it re-sees the session_meta line. ``None`` if the name
    carries no UUID (a caller-renamed fixture) — the tailer then falls back to the file stem."""
    match = _ROLLOUT_UUID.search(path.name)
    return match.group(1) if match is not None else None


class CodexParserV1:
    """Codex rollout LINE → :class:`RawActivity` | ``None`` (the pure, per-record mapping;
    :class:`CodexRolloutTailer` owns the file/offset/drift loop around it). ``None`` = "this line
    carries no memory-worthy capture" (control/noise/encrypted/role-echo) — the load-bearing rule
    that codex's ~90%-noise rollout stream does not each-line become a memory."""

    host = HostKind.CODEX
    schema_version = _ROLLOUT_SCHEMA_VERSION

    def __init__(self, *, tool_input_max_chars: int = 500) -> None:
        self._tool_input_max_chars = tool_input_max_chars

    def map_line(
        self,
        record: Mapping[str, Any],
        *,
        session_id: str,
        host_version: str,
        cwd: str | None,
        source_offset: str,
    ) -> RawActivity | None:
        """Map one already-JSON-parsed rollout record to a :class:`RawActivity`, or ``None`` to
        skip it. ``session_id``/``host_version``/``cwd`` are the session-scoped stamps the tailer
        resolved from the session_meta line; ``source_offset`` is the record's byte offset (unique,
        stable ⇒ idempotent ``activity_id``)."""
        payload = record.get("payload")
        if not isinstance(payload, dict):
            # Every real rollout line is {timestamp,type,payload:obj}; a non-object payload is an
            # envelope violation (drift), not a skippable unknown kind.
            raise self._drift(record)
        top = record.get("type")
        ptype = payload.get("type")

        kind: ActivityKind
        text: str | None
        extra: dict[str, str | int | bool | None]
        if top == "event_msg" and ptype == "user_message":
            kind = ActivityKind.USER_PROMPT
            text = _nonempty(payload.get("message"))
            extra = {}
        elif top == "event_msg" and ptype == "agent_message":
            kind = ActivityKind.ASSISTANT_MSG
            text = _nonempty(payload.get("message"))
            extra = {"phase": _opt_str(payload.get("phase"))}
        elif top == "response_item" and ptype in ("custom_tool_call", "function_call"):
            kind = ActivityKind.TOOL_USE
            name = _opt_str(payload.get("name")) or "unknown_tool"
            raw_input = payload.get("input") if ptype == "custom_tool_call" else payload.get(
                "arguments"
            )
            body = _truncate(str(raw_input), self._tool_input_max_chars) if raw_input else ""
            text = f"{name}: {body}" if body else None
            extra = {
                "tool_name": name,
                "call_id": _opt_str(payload.get("call_id")),
                "tool_kind": str(ptype),
            }
        else:
            # reasoning (encrypted), token_count, task_*, world_state, turn_context, *_output,
            # response_item/message role-echoes, mcp_tool_call_end, sub_agent_activity, session_meta
            # — a known-or-unknown NON-captured kind inside a valid envelope. Skip, never drift.
            return None

        if text is None:
            return None  # a captured kind whose text slot was empty/absent — nothing to remember.

        return RawActivity(
            activity_id=_activity_id(
                session_id=session_id, source_offset=source_offset, kind=kind
            ),
            host=self.host,
            host_version=host_version,
            schema_version=self.schema_version,
            kind=kind,
            session_id=session_id,
            cwd=cwd,
            occurred_at=_parse_ts(record.get("timestamp")),
            text=text,
            content_hash=_sha256_text(text),
            source_offset=source_offset,
            provenance_id=_provenance_id(
                session_id=session_id, source_offset=source_offset, kind=kind
            ),
            payload=extra,
        )

    def _drift(self, record: Mapping[str, object]) -> CaptureSchemaDriftError:
        return CaptureSchemaDriftError(
            host=self.host.value,
            source_id=f"codex_rollout:{self.schema_version}",
            detected_keys=sorted(str(k) for k in record),
            expected_schema=[self.schema_version],
            raw_sample_sha256=_sha256_of(record),
        )


class CodexNotifyParserV1:
    """Codex ``notify`` ``agent-turn-complete`` envelope → :class:`RawActivity` (the LIVE push path;
    implements the :class:`~mu_client.capture.parsers.HostSchemaParser` protocol so
    ``mu capture-once --host codex`` selects it through the shared
    :class:`~mu_client.capture.parsers.ParserRegistry`). Captures the turn's FINAL assistant answer
    (``last-assistant-message``) — the user prompt is already captured by the rollout tailer; the
    notify hook's value is the low-latency final-answer push, mirroring Claude Code's ``Stop``."""

    host = HostKind.CODEX
    schema_version = _NOTIFY_SCHEMA_VERSION
    _EVENT = "agent-turn-complete"

    def matches(self, record: Mapping[str, object]) -> bool:
        return record.get("type") == self._EVENT

    def parse(self, *, record: Mapping[str, object], event_id: str) -> RawActivity:
        if record.get("type") != self._EVENT:  # matches() gated this; defence in depth
            raise CaptureSchemaDriftError(
                host=self.host.value,
                source_id=self.host.value,
                detected_keys=sorted(str(k) for k in record),
                expected_schema=[self._EVENT],
                raw_sample_sha256=_sha256_of(record),
            )
        # codex uses hyphenated serde keys: thread-id / turn-id / last-assistant-message.
        session_id = _opt_str(record.get("thread-id")) or _opt_str(record.get("turn-id")) or ""
        cwd = record.get("cwd")
        text = _nonempty(record.get("last-assistant-message"))
        kind = ActivityKind.ASSISTANT_MSG
        return RawActivity(
            activity_id=_activity_id(
                session_id=session_id, source_offset=event_id, kind=kind
            ),
            host=self.host,
            host_version=_HOST_VERSION_FALLBACK,
            schema_version=self.schema_version,
            kind=kind,
            session_id=session_id,
            cwd=str(cwd) if cwd is not None else None,
            occurred_at=datetime.now(UTC),
            text=text,
            content_hash=_sha256_text(text) if text is not None else None,
            source_offset=event_id,
            provenance_id=_provenance_id(
                session_id=session_id, source_offset=event_id, kind=kind
            ),
            payload={"turn_id": _opt_str(record.get("turn-id")), "client": _opt_str(
                record.get("client")
            )},
        )


def _nonempty(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value
    return None


def _opt_str(value: Any) -> str | None:
    return str(value) if value is not None else None


def _parse_ts(value: Any) -> datetime:
    """Parse a rollout ``timestamp`` (ISO-8601, e.g. ``2026-08-10T04:40:47.123Z``); fall back to
    now(UTC) — provenance only, never load-bearing for correctness."""
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return datetime.now(UTC)
    return datetime.now(UTC)


class CodexTailResult(BaseModel, frozen=True):
    """The append-stable outcome of one :meth:`CodexRolloutTailer.tail` pass."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    activities: list[RawActivity]
    end_offset: int  # byte offset of the last COMPLETE line consumed — the resume checkpoint
    records_scanned: int
    session_id: str  # resolved session id these activities were stamped with


class CodexRolloutTailer:
    """Reads a codex rollout JSONL and yields :class:`RawActivity` items (see module docstring).
    Pure/synchronous beyond reading the given file — the async :func:`backfill_codex` wraps
    :meth:`tail` in ``asyncio.to_thread`` and owns the outbox write, exactly like the Phase-0B
    Claude tailer."""

    host = HostKind.CODEX
    schema_version = _ROLLOUT_SCHEMA_VERSION

    def __init__(self, *, tool_input_max_chars: int = 500) -> None:
        self._parser = CodexParserV1(tool_input_max_chars=tool_input_max_chars)

    def tail(
        self,
        rollout_path: Path,
        *,
        session_id: str | None = None,
        since_byte: int = 0,
    ) -> CodexTailResult:
        """Read complete lines from ``since_byte`` onward and map each to a capture (or skip).
        ``session_id`` (explicit > session_meta line > filename UUID > file stem) is the η.session
        every activity is stamped with; a trailing byte-run with no newline is a half-written record
        left for the next tail (transcript-lag guard)."""
        path = rollout_path.expanduser()
        raw = path.read_bytes()

        resolved_session = (
            session_id or session_id_from_rollout_name(path) or path.stem
        )
        host_version = _HOST_VERSION_FALLBACK
        cwd: str | None = None

        activities: list[RawActivity] = []
        records_scanned = 0
        cursor = since_byte
        end_offset = since_byte
        while cursor < len(raw):
            nl = raw.find(b"\n", cursor)
            if nl == -1:
                break  # half-written trailing record — leave for the next tail
            line = raw[cursor:nl]
            line_start = cursor
            cursor = nl + 1
            end_offset = cursor
            if not line.strip():
                continue
            record = self._load_record(line)
            records_scanned += 1

            # session_meta (line 0) carries the session id, cwd and cli_version — read them to stamp
            # every subsequent activity, then skip it (control, no text of its own).
            if record.get("type") == "session_meta":
                meta = record.get("payload")
                if isinstance(meta, dict):
                    resolved_session = (
                        session_id
                        or _opt_str(meta.get("session_id"))
                        or _opt_str(meta.get("id"))
                        or resolved_session
                    )
                    cwd = _opt_str(meta.get("cwd")) or cwd
                    host_version = "codex/" + (_opt_str(meta.get("cli_version")) or "unknown")
                continue

            activity = self._parser.map_line(
                record,
                session_id=resolved_session,
                host_version=host_version,
                cwd=cwd,
                source_offset=str(line_start),
            )
            if activity is not None:
                activities.append(activity)

        return CodexTailResult(
            activities=activities,
            end_offset=end_offset,
            records_scanned=records_scanned,
            session_id=resolved_session,
        )

    def _load_record(self, line: bytes) -> dict[str, Any]:
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise CaptureSchemaDriftError(
                host=self.host.value,
                source_id=f"codex_rollout:{self.schema_version}",
                detected_keys=[],
                expected_schema=[self.schema_version],
                raw_sample_sha256=hashlib.sha256(line).hexdigest(),
            ) from exc
        if not isinstance(record, dict) or "type" not in record:
            raise CaptureSchemaDriftError(
                host=self.host.value,
                source_id=f"codex_rollout:{self.schema_version}",
                detected_keys=sorted(str(k) for k in record) if isinstance(record, dict) else [
                    type(record).__name__
                ],
                expected_schema=[self.schema_version],
                raw_sample_sha256=hashlib.sha256(line).hexdigest(),
            )
        return record


class CodexBackfillResult(BaseModel, frozen=True):
    """Content-free summary of one :func:`backfill_codex` run (counts only — never text)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    appended: int
    end_offset: int
    since_byte: int
    records_scanned: int
    session_id: str
    halted: bool = False  # True ⇒ SchemaDriftError halted THIS source (never blocks other sources)


def _source_id(session_id: str) -> str:
    """The per-session resume key in the outbox ``checkpoints`` table — one cursor per rollout so
    every session backfills independently and idempotently."""
    return f"codex_rollout:{session_id}"


async def backfill_codex(
    settings: ClientSettings,
    *,
    rollout_path: Path,
    session_id: str | None = None,
) -> CodexBackfillResult:
    """Tail ``rollout_path`` from its persisted checkpoint and append every capture to the SAME
    durable outbox the hook/notify capture writes (``mu flush`` / the daemon worker then drives them
    into the real stores through :class:`~mu_client.workers.ingest_client.InProcessLocalIngest`).
    Resumes from the per-session ``byte_offset`` checkpoint, advancing it in the SAME transaction as
    the last append (capture-spec.md §8.3) so a re-run only re-reads append-stable bytes and
    ``UNIQUE(activity_id)`` makes any overlap a no-op.

    A :class:`~mu_client.errors.CaptureSchemaDriftError` halts THIS source only, loud
    (``CaptureSourceHalted`` event), reported as ``halted=True`` — a rollout-shape drift never
    blocks the notify hook or another session's tail (mirrors :func:`backfill_thinking`)."""
    tailer = CodexRolloutTailer(tool_input_max_chars=settings.capture.tool_outcome_max_chars)
    resolved = session_id or session_id_from_rollout_name(rollout_path.expanduser()) or (
        rollout_path.expanduser().stem
    )
    outbox = SqliteOutbox(settings.outbox.outbox_path)
    await outbox.open()
    try:
        checkpoint = await outbox.load_checkpoint(_source_id(resolved))
        since = checkpoint.byte_offset if checkpoint is not None else 0
        try:
            result = await asyncio.to_thread(
                tailer.tail, rollout_path, session_id=session_id, since_byte=since
            )
        except CaptureSchemaDriftError as exc:
            log_capture_source_halted(
                host=tailer.host.value,
                schema_version=tailer.schema_version,
                raw_sample_sha256=exc.raw_sample_sha256,
            )
            return CodexBackfillResult(
                appended=0,
                end_offset=since,
                since_byte=since,
                records_scanned=0,
                session_id=resolved,
                halted=True,
            )

        appended = 0
        for index, activity in enumerate(result.activities):
            checkpoint_update = (
                CaptureCheckpoint(
                    source_id=_source_id(result.session_id),
                    file_path=str(rollout_path.expanduser()),
                    byte_offset=result.end_offset,
                    updated_at=datetime.now(UTC),
                )
                if index == len(result.activities) - 1
                else None
            )
            await outbox.append(activity, checkpoint=checkpoint_update)
            log_activity_captured(
                activity_id=activity.activity_id,
                host=activity.host.value,
                kind=activity.kind.value,
                session_id=activity.session_id,
            )
            appended += 1
        return CodexBackfillResult(
            appended=appended,
            end_offset=result.end_offset,
            since_byte=since,
            records_scanned=result.records_scanned,
            session_id=result.session_id,
        )
    finally:
        await outbox.aclose()
