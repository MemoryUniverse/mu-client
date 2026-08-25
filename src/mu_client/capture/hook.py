"""``mu capture-once`` — the ONE hook entrypoint Claude Code invokes (capture-spec.md §2.2/§3;
daemon-app-skeleton-spec.md §6). Reads the hook's stdin JSON, tries the resident daemon's IPC
socket first (fast path — the daemon parses/appends/recalls with an already-warm engine), and
falls back to a DIRECT SQLite-WAL outbox append whenever the daemon does NOT durably take the
record — no daemon listening, the socket call timing out, or the daemon REFUSING (``503
shutting_down`` during its ordered shutdown, ``401 foreign_uid``, ``413 request_too_large``, an
empty reply): a refusal is handled exactly like unreachable, because the durability boundary is
BEFORE the host is acked (``outbox/sqlite_outbox.py``) — pure daemonless durability, no live
injection this invocation (capture-spec.md §2.3:
a documented capability tier, never a silent gap). On outbox-unreachable (locked/corrupt WAL) the
envelope spools to disk and the process exits 0 regardless — **never blocks/fails the host turn**.

**Deviation (recorded):** capture-spec.md's Half A is a tiny Go binary with its own monotonic
``event_id`` counter (§3); this stage has no Go binary (see ``daemon/app.py``'s scope note), so
``event_id = sha256(raw_stdin_bytes)`` — see :mod:`mu_client.capture.parsers`'s module docstring
for the idempotency trade-off this implies. **Also deviation:** loading the real embedder per
hook invocation to serve daemonless injection would be a RAM/latency disaster on a shared box with
hundreds of tool-call hooks per session (this stage's own RAM-AWARE constraint) — so the direct-
append fallback NEVER constructs :class:`~mu_client.host.LocalMemoryHost`; injection is available
ONLY when a resident daemon (already holding the warm engine) is reachable, exactly capture-spec's
own "daemon = liveness optimization" framing (§0).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import uuid
from pathlib import Path
from typing import Any

import structlog

from mu_client.capture.claude_tailer import backfill_thinking
from mu_client.capture.codex import CodexNotifyParserV1
from mu_client.capture.model import HostKind
from mu_client.capture.parsers import ClaudeCodeParserV1, ParserRegistry
from mu_client.config import ClientSettings
from mu_client.errors import CaptureSchemaDriftError, OutboxCorruptionError
from mu_client.outbox.sqlite_outbox import SqliteOutbox

__all__ = ["capture_once", "replay_spool"]

_log = structlog.get_logger("mu.client.capture.hook")


def _build_registry(settings: ClientSettings) -> ParserRegistry:
    """The ONE parser set the daemonless capture/replay paths share: Claude Code hook envelopes +
    the Codex ``notify`` ``agent-turn-complete`` envelope (Phase 4). The registry keys parsers by
    host, so ``--host codex`` only ever tries the codex parser and vice-versa."""
    registry = ParserRegistry()
    registry.register(
        ClaudeCodeParserV1(tool_outcome_max_chars=settings.capture.tool_outcome_max_chars)
    )
    registry.register(CodexNotifyParserV1())
    return registry


_DUAL_PURPOSE_EVENTS = frozenset({"UserPromptSubmit", "SessionStart"})
# Hook events on which a completed turn's reasoning is append-stable in the transcript, so the
# Phase 0B tailer can safely backfill it (capture-spec.md §6: "read completed turns only").
_THINKING_BACKFILL_EVENTS = frozenset({"Stop", "SubagentStop", "PreCompact", "SessionEnd"})


async def capture_once(settings: ClientSettings, *, host: HostKind, raw: bytes) -> dict[str, Any]:
    """Returns the hook JSON response body to print on stdout. Exit code is ALWAYS 0 at the CLI
    layer (:mod:`mu_client.cli`) — capture never fails/blocks the host turn."""
    record: dict[str, Any] = json.loads(raw)
    event_id = hashlib.sha256(raw).hexdigest()
    event = str(record.get("hook_event_name", ""))

    daemon_response = await _try_daemon(settings, host=host, record=record, event_id=event_id)
    if daemon_response is not None:
        await _maybe_backfill_thinking(settings, host=host, event=event, record=record)
        return daemon_response
    await _direct_append_or_spool(settings, host=host, record=record, event_id=event_id, raw=raw)
    await _maybe_backfill_thinking(settings, host=host, event=event, record=record)
    return _hook_output(event, additional_context=None)


async def _maybe_backfill_thinking(
    settings: ClientSettings, *, host: HostKind, event: str, record: dict[str, Any]
) -> None:
    """Phase 0B trigger (capture-spec.md §6): on a turn-completing hook event, backfill the
    session's REASONING (thinking blocks + mid-turn intermediate messages) from the transcript the
    hook payload points at (``transcript_path``) into the SAME durable outbox. OFF unless
    ``MU_CAPTURE__THINKING_BACKFILL_ENABLED`` is set; Claude-Code-only; and — like all capture — it
    NEVER blocks or fails the host turn: any error is swallowed to a content-free log line, because
    a reasoning backfill is a best-effort enrichment, never a correctness dependency."""
    if not settings.capture.thinking_backfill_enabled:
        return
    if host is not HostKind.CLAUDE_CODE or event not in _THINKING_BACKFILL_EVENTS:
        return
    transcript_path = record.get("transcript_path")
    if not isinstance(transcript_path, str) or not transcript_path:
        return
    session_id = record.get("session_id")
    try:
        await backfill_thinking(
            settings,
            transcript_path=Path(transcript_path),
            session_id=str(session_id) if session_id else None,
        )
    except (OSError, OutboxCorruptionError, sqlite3.Error) as exc:
        # Best-effort: a missing/locked transcript or outbox never blocks the host turn.
        _log.info("thinking_backfill_skipped", event=event, error=type(exc).__name__)


# --------------------------------------------------------------------------------- daemon fast path
#: Daemon capture replies that mean **the daemon has durably taken ownership of this record**, and
#: only those. Returning non-``None`` for anything else is what silently DROPPED captures: the
#: caller reads "not ``None``" as "durably captured" and skips :func:`_direct_append_or_spool`
#: entirely, so a refusal became a lost activity with no error anywhere.
#:
#: * ``200`` — appended to the daemon's outbox (``ipc.py::_route_capture``).
#: * ``422`` — schema drift: the daemon ``quarantine_raw``'d the RAW envelope into its own
#:   dead-letter table BEFORE replying, so the record IS retained on disk. Spooling it a second
#:   time here would duplicate it into a spool file that :func:`replay_spool` can never drain
#:   (it still fails to parse, by definition) — retention without a leak, so the daemon owns it.
#:
#: Everything else is a REFUSAL where the daemon holds nothing — ``401 foreign_uid``,
#: ``413 request_too_large``, ``503 shutting_down``, ``404 unknown_route``, an empty/unparseable
#: reply — and the correct response to a refusal is identical to the daemon being unreachable:
#: fall through and spool. A ``503`` during every shutdown window is the likeliest of these.
_DAEMON_ACCEPTED_STATUSES = frozenset({200, 422})


def _daemon_accepted(response: dict[str, Any]) -> bool:
    """Did the daemon durably take ownership? See :data:`_DAEMON_ACCEPTED_STATUSES`."""
    return response.get("status") in _DAEMON_ACCEPTED_STATUSES


async def _try_daemon(
    settings: ClientSettings, *, host: HostKind, record: dict[str, Any], event_id: str
) -> dict[str, Any] | None:
    """``None`` means **the daemon did not durably capture this record** — for ANY reason
    (unreachable, timed out, refused, replied with nothing) — and the caller must fall through to
    :func:`_direct_append_or_spool`. Non-``None`` is a promise the activity is already durable."""
    socket_path = settings.ipc.socket_path.expanduser()
    timeout_s = settings.ipc.socket_timeout_s
    if not socket_path.exists():
        return None
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_unix_connection(
                path=str(socket_path), limit=settings.ipc.max_request_bytes
            ),
            timeout=timeout_s,
        )
    except (OSError, TimeoutError):
        return None  # DAEMON_UNREACHABLE_SPOOLED path — caller falls through to direct append
    try:
        capture_response = await _rpc(
            reader,
            writer,
            {"route": "capture", "host": host.value, "record": record, "event_id": event_id},
            timeout_s=timeout_s,
        )
    except (OSError, TimeoutError, ValueError) as exc:
        # OSError: the daemon closed/reset mid-exchange (e.g. it answered 413 and hung up while we
        # were still writing an oversized record). ValueError: an unparseable reply, or a REPLY
        # past our own read limit. TimeoutError: no reply inside the socket budget. All three are
        # "not durably captured" — content-free log line, then spool.
        _log.info("daemon_capture_rpc_failed", error=type(exc).__name__)
        return None
    finally:
        writer.close()
    if not _daemon_accepted(capture_response):
        _log.info("daemon_capture_refused", status=capture_response.get("status"))
        return None

    event = str(record.get("hook_event_name", ""))
    if event not in _DUAL_PURPOSE_EVENTS:
        return _hook_output(event, additional_context=None)

    # Capture is DURABLE from here on, so every failure below degrades to "no injection this
    # invocation" (capture-spec.md §2.3) and must NEVER return None — that would re-append the
    # very record the daemon just accepted.
    session_id = str(record.get("session_id", ""))
    query = record.get("prompt") if event == "UserPromptSubmit" else None
    try:
        reader2, writer2 = await asyncio.wait_for(
            asyncio.open_unix_connection(
                path=str(socket_path), limit=settings.ipc.max_request_bytes
            ),
            timeout=timeout_s,
        )
    except (OSError, TimeoutError):
        return _hook_output(event, additional_context=None)
    try:
        recall_resp = await _rpc(
            reader2,
            writer2,
            {"route": "recall", "session_id": session_id, "query": query},
            timeout_s=timeout_s,
        )
    except (OSError, TimeoutError, ValueError) as exc:
        _log.info("daemon_recall_rpc_failed", error=type(exc).__name__)
        return _hook_output(event, additional_context=None)
    finally:
        writer2.close()
    body = recall_resp.get("body") if recall_resp.get("status") == 200 else None
    return _hook_output(event, additional_context=body or None)


async def _rpc(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    payload: dict[str, Any],
    *,
    timeout_s: float,
) -> dict[str, Any]:
    """One newline-delimited-JSON round trip. RETURNS the parsed reply body — the caller MUST
    inspect it (see :func:`_daemon_accepted`); a reply that never came (connection closed with no
    line) or is not a JSON object yields ``{}``, which carries no ``status`` and therefore fails
    every acceptance check.

    ``timeout_s`` bounds the WHOLE exchange (write + drain + read), not just the read: draining a
    multi-hundred-KiB record into a daemon that has stopped reading it (exactly what happens when
    the daemon answers 413 and hangs up) blocks in ``drain()``, which an unbounded write would
    never leave — and a hook that never returns blocks the host turn."""

    async def _exchange() -> dict[str, Any]:
        writer.write((json.dumps(payload) + "\n").encode("utf-8"))
        await writer.drain()
        line = await reader.readline()
        if not line:
            return {}
        parsed = json.loads(line)
        return parsed if isinstance(parsed, dict) else {}

    return await asyncio.wait_for(_exchange(), timeout=timeout_s)


# --------------------------------------------------------------------------------- daemonless path
async def _direct_append_or_spool(
    settings: ClientSettings, *, host: HostKind, record: dict[str, Any], event_id: str, raw: bytes
) -> None:
    registry = _build_registry(settings)
    try:
        parser = registry.select(host, record)
        activity = parser.parse(record=record, event_id=event_id)
    except CaptureSchemaDriftError:
        _spool(settings, host, raw)
        return

    outbox = SqliteOutbox(settings.outbox.outbox_path)
    try:
        await outbox.open()
        await outbox.append(activity)
    except (OutboxCorruptionError, sqlite3.Error):
        _spool(settings, host, raw)
    finally:
        await outbox.aclose()


def _spool(settings: ClientSettings, host: HostKind, raw: bytes) -> None:
    """``DAEMON_UNREACHABLE_SPOOLED`` — write ``{host, raw}`` (host tagged so :func:`replay_spool`
    can pick the right parser without re-guessing) to ``~/.memory-universe/spool/*.json``."""
    spool_dir = settings.capture.spool_dir.expanduser()
    spool_dir.mkdir(parents=True, exist_ok=True)
    envelope = {"host": host.value, "raw": raw.decode("utf-8", errors="replace")}
    (spool_dir / f"{uuid.uuid4().hex}.json").write_text(json.dumps(envelope), encoding="utf-8")


async def replay_spool(settings: ClientSettings, outbox: SqliteOutbox) -> int:
    """Ingest every spooled envelope into the (now-open) outbox, idempotently
    (``UNIQUE(activity_id)`` — capture-spec.md §8.3). A file that STILL fails to parse (schema
    drift persists) is left in place rather than silently dropped; everything else is removed
    once durably appended. Returns the count of files successfully replayed."""
    spool_dir = settings.capture.spool_dir.expanduser()
    if not spool_dir.is_dir():
        return 0
    registry = _build_registry(settings)
    replayed = 0
    for path in sorted(spool_dir.glob("*.json")):
        envelope = json.loads(path.read_text(encoding="utf-8"))
        host = HostKind(envelope["host"])
        raw = envelope["raw"].encode("utf-8")
        record: dict[str, Any] = json.loads(raw)
        event_id = hashlib.sha256(raw).hexdigest()
        try:
            parser = registry.select(host, record)
            activity = parser.parse(record=record, event_id=event_id)
        except CaptureSchemaDriftError:
            continue  # still drifted — leave it for a future replay / manual inspection
        await outbox.append(activity)
        path.unlink()
        replayed += 1
    return replayed


def _hook_output(event: str, *, additional_context: str | None) -> dict[str, Any]:
    """The real Claude Code hook stdout contract (host-capture-integration-devdoc.md §2.1): exit 0
    with ``{}`` (no injection) or ``{"hookSpecificOutput": {...}}``."""
    if additional_context is None:
        return {}
    return {"hookSpecificOutput": {"hookEventName": event, "additionalContext": additional_context}}
