"""``ClaudeCodeTranscriptTailer`` — the Phase 0B background-thinking backfill source
(capture-spec.md §6, AGENT-INTEGRATION-AUDIT-AND-PLAN.md §4 Phase 0B + §6 "background thinking").

**Why this exists.** Claude Code hooks expose the human turn, the agent's *final* answer, and
tool/subagent activity — but **NO** chain-of-thought and **NO** mid-turn intermediate messages
(host-capture-integration-devdoc.md §0; capture-spec.md §6). The agent's *reasoning* — the
decisions, findings and conclusions it reached on the way to that final answer — lives only in the
Claude Code **session transcript JSONL** (``~/.claude/projects/<slug>/<session>.jsonl``; a hook
payload also hands us its path as ``transcript_path``). This tailer reads that transcript and
backfills the reasoning the hook fleet structurally cannot see, feeding it through the SAME
outbox→ingest spine the hook capture uses (:class:`~mu_client.outbox.sqlite_outbox.SqliteOutbox`
→ :class:`~mu_client.workers.ingest_client.InProcessLocalIngest`) so it lands in the 3-tier store
like any other memory.

**Real transcript shape (verified against Claude Code 2.1.226 on-disk).** Each line is one JSON
record. Reasoning lives in ``type == "assistant"`` records under ``message.content`` — a list of
typed blocks:

* ``{"type": "thinking", "thinking": "<plaintext extended-thinking>", "signature": "<...>"}``
* ``{"type": "text", "text": "<assistant prose>"}``
* ``{"type": "tool_use", ...}``

**Honest boundary (capture-spec.md §6 F1).** Extended-thinking is persisted *plaintext* only when
the session recorded it; on inspected 2026-08 transcripts every ``thinking`` block carried only a
``signature`` with an **empty** ``thinking`` string (encrypted/redacted), so the plaintext-thinking
path yields **nothing** on those sessions — never an error, exactly as F1 requires. The
**intermediate-assistant-message** path (a ``text`` block in the SAME assistant record as a
``tool_use`` block — provably mid-turn, never the final Stop answer) IS persisted plaintext and is
the reliably-available reasoning source. When neither is present the tailer yields an empty result.

**SIGNAL vs NOISE — reuse the importance gate, do NOT invent a second one.** Raw CoT is ~90% noise
(capture-spec.md §0); we never persist it wholesale. Two-layer discipline, each layer a mechanism
that already exists in this codebase — no new gate:

1. **Salient-slice extraction** (:func:`extract_reasoning_signal`, the same *category* of filter as
   ``ClaudeCodeParserV1``'s kind-gate/salient-slice, capture-spec.md §7.1): a reasoning text is
   split into sentences and only those bearing a **decision/finding/conclusion** marker survive;
   pure boilerplate ("Let me read the file.", "Okay.", "Now I'll look at X.") matches nothing and
   is **dropped** — it never becomes a ``RawActivity``, so noise lines never each become a memory.
2. **Importance = the engine's ONE promotion gate.** Every surviving candidate is assigned an
   ``importance`` in [0,1] (a strong DECISION → ``decision_importance`` ≥ the engine's
   ``importance_promote`` 0.6 → promotes STM→MTM; a weaker FINDING → ``finding_importance`` < 0.6 →
   STM-only). That number threads straight to ``DeterministicPromoteStage``'s existing
   ``importance >= importance_promote`` check (mu-engine ``pipelines/concrete/ingest.py``) — the
   SAME gate ``LocalMemory.add(importance_score=...)`` already exposes. We do not add a competing
   threshold.

**Reasoning-only by construction (capture-spec.md §6 F3 dual-source dedup).** This tailer emits
ONLY reasoning kinds the hook fleet omits — thinking blocks and mid-turn intermediate messages —
and NEVER a ``USER_PROMPT`` / ``Stop`` final answer / ``TOOL_USE``. A single assistant record whose
content is text-only (no ``tool_use``) is treated as the turn's *final* answer, which the ``Stop``
hook already captures, and is **skipped** — so hooks + tailer on one session never double-capture.

**Transcript-lag guard (capture-spec.md §6).** ``transcript_path`` is written asynchronously and
may lag; the tailer reads **complete lines only** (a trailing byte-run with no newline is a
half-written record — left for the next tail) and resumes from a persisted ``byte_offset``
checkpoint, so it only ever backfills append-stable, completed turns.

**SchemaDriftError discipline (capture-spec.md §6).** The transcript JSONL is an app-internal
format; on a record that *claims* to be an assistant turn but whose shape we cannot parse (bad
JSON, non-list ``content``, a block with no ``type``) the tailer halts **this source only**, loud
(:class:`~mu_client.errors.CaptureSchemaDriftError` + a ``CaptureSourceHalted`` event), and that
failure never blocks the hook fleet's primary capture.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from datetime import UTC, datetime
from enum import StrEnum
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
    "BackfillResult",
    "ClaudeCodeTranscriptTailer",
    "ExtractedSignal",
    "SignalKind",
    "TailResult",
    "backfill_thinking",
    "extract_reasoning_signal",
]

_SCHEMA_VERSION = "claude_code.transcript.v1"
_HOST_VERSION_FALLBACK = "claude_code/unknown"

# The ONE attribution tag every background-thinking memory carries (payload["source"]) so a stored
# reasoning memory is distinguishable from a top-level turn — read by InProcessLocalIngest to
# prefix the stored text with "[thinking] " (mirrors the existing "[subagent:...]" attribution).
THINKING_SOURCE = "thinking"

# A sentence must be at least this long to be a candidate — a bare "Okay." / "Done." fragment is
# never a memory even if it trips a marker substring (defensive against noise, not a second gate).
_MIN_SENTENCE_CHARS = 20


class SignalKind(StrEnum):
    """The salience class :func:`extract_reasoning_signal` assigns a surviving sentence — maps 1:1
    to an importance band the engine's promotion gate then acts on (see module docstring)."""

    DECISION = "decision"  # a choice/conclusion/root-cause → promote-worthy (≥ importance_promote)
    FINDING = "finding"  # an observation/insight worth keeping in STM, not promote-worthy


# Decision/conclusion markers (lowercased substring match) — the agent COMMITTED to something.
# Kept as multi-word phrases where a bare token would over-match ("i'll use" not bare "i'll").
_DECISION_MARKERS: tuple[str, ...] = (
    "decid",  # decide / decided / decision
    "i chose",
    "we chose",
    "chosen",
    "choosing",
    "i'll use",
    "we'll use",
    "will use",
    "going with",
    "let's use",
    "let's go with",
    "i'll go with",
    "because",
    "therefore",
    "hence",
    "the fix is",
    "so the fix",
    "root cause",
    "conclusion",
    "concluded",
    "the plan is",
    "the approach is",
    "the key is",
    "key insight",
    "must use",
)

# Finding markers — an observation/insight, weaker than a commitment.
_FINDING_MARKERS: tuple[str, ...] = (
    "found that",
    "turns out",
    "i notice",
    "i noticed",
    "i see that",
    "this means",
    "it appears",
    "looks like",
    "the issue is",
    "the problem is",
    "note that",
    "importantly",
    "the difference is",
)

_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+|\n+")


class ExtractedSignal(BaseModel, frozen=True):
    """One salient reasoning sentence that survived the noise filter, with its salience class."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    text: str
    kind: SignalKind


def extract_reasoning_signal(text: str) -> list[ExtractedSignal]:
    """Split a raw reasoning text into sentences and keep ONLY those bearing a decision/finding
    marker (the salient-slice noise filter, capture-spec.md §7.1). Returns ``[]`` for pure
    boilerplate — the load-bearing property that "pure-noise lines do NOT each become a memory".

    Deterministic, no LLM (capture is dumb/cheap/deterministic — capture-spec.md §0). A DECISION
    marker wins over a FINDING marker when both are present (the stronger salience class)."""
    out: list[ExtractedSignal] = []
    for raw in _SENTENCE_SPLIT.split(text):
        sentence = raw.strip()
        if len(sentence) < _MIN_SENTENCE_CHARS:
            continue
        low = sentence.lower()
        if any(m in low for m in _DECISION_MARKERS):
            out.append(ExtractedSignal(text=sentence, kind=SignalKind.DECISION))
        elif any(m in low for m in _FINDING_MARKERS):
            out.append(ExtractedSignal(text=sentence, kind=SignalKind.FINDING))
    return out


class TailResult(BaseModel, frozen=True):
    """The append-stable outcome of one :meth:`ClaudeCodeTranscriptTailer.tail` pass."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    activities: list[RawActivity]
    end_offset: int  # byte offset of the last COMPLETE line consumed — the resume checkpoint
    records_scanned: int
    thinking_blocks_seen: int  # provenance: how many thinking blocks (incl. empty/signature-only)
    thinking_blocks_plaintext: int  # of those, how many carried plaintext (F1 persistence rate)


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _activity_id(*, session_id: str, source_offset: str, kind: ActivityKind) -> str:
    # SAME deterministic formula as capture/parsers.py::_activity_id (host|session|offset|kind) so
    # a re-tail of the same transcript is an idempotent no-op insert (UNIQUE(activity_id)).
    return _sha256_text(f"{HostKind.CLAUDE_CODE.value}|{session_id}|{source_offset}|{kind.value}")


def _provenance_id(*, session_id: str, source_offset: str, kind: ActivityKind) -> str:
    full = _activity_id(session_id=session_id, source_offset=source_offset, kind=kind)
    return "prov_" + full[:24]


class ClaudeCodeTranscriptTailer:
    """Reads a Claude Code session transcript JSONL and yields reasoning-only
    :class:`~mu_client.capture.model.RawActivity` items (see module docstring). Pure, synchronous,
    no I/O beyond reading the given file — the async caller (:func:`backfill_thinking`) wraps
    :meth:`tail` in ``asyncio.to_thread`` and owns the outbox write."""

    host = HostKind.CLAUDE_CODE
    schema_version = _SCHEMA_VERSION

    def __init__(self, *, decision_importance: float, finding_importance: float) -> None:
        self._decision_importance = decision_importance
        self._finding_importance = finding_importance

    def _importance_for(self, kind: SignalKind) -> float:
        return (
            self._decision_importance if kind is SignalKind.DECISION else self._finding_importance
        )

    def tail(
        self,
        transcript_path: Path,
        *,
        session_id: str | None = None,
        since_byte: int = 0,
    ) -> TailResult:
        """Read complete lines from ``since_byte`` onward, extract reasoning signal, and return the
        activities plus the new append-stable ``end_offset``. ``session_id`` overrides the record's
        own ``sessionId`` (used as the η.session slot); when neither is present the file stem is
        used as a last resort so an activity is never mis-keyed under an empty session."""
        path = transcript_path.expanduser()
        raw = path.read_bytes()
        activities: list[RawActivity] = []
        records_scanned = 0
        thinking_seen = 0
        thinking_plaintext = 0

        cursor = since_byte
        end_offset = since_byte
        while cursor < len(raw):
            nl = raw.find(b"\n", cursor)
            if nl == -1:
                # A trailing byte-run with no newline = a half-written record (transcript lag).
                # Leave it; the next tail resumes from end_offset and re-reads it once complete.
                break
            line = raw[cursor:nl]
            cursor = nl + 1
            end_offset = cursor
            if not line.strip():
                continue
            record = self._load_record(line, path=path)
            records_scanned += 1
            if record.get("type") != "assistant":
                continue  # user turns, attachments, system/meta records — not our source
            if record.get("isSidechain") is True:
                # Sub-agent reasoning belongs to the SubagentStop capture path (a separate
                # transcript file); skip it here so we never double-attribute it to the main turn.
                continue
            seen, plaintext, acts = self._activities_from_assistant(
                record, session_id=session_id, fallback_session=path.stem, offset=cursor
            )
            thinking_seen += seen
            thinking_plaintext += plaintext
            activities.extend(acts)

        return TailResult(
            activities=activities,
            end_offset=end_offset,
            records_scanned=records_scanned,
            thinking_blocks_seen=thinking_seen,
            thinking_blocks_plaintext=thinking_plaintext,
        )

    # ---------------------------------------------------------------------------- record parsing
    def _load_record(self, line: bytes, *, path: Path) -> dict[str, Any]:
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise self._drift(detected_keys=[], raw=line) from exc
        if not isinstance(record, dict):
            raise self._drift(detected_keys=[type(record).__name__], raw=line)
        return record

    def _activities_from_assistant(
        self,
        record: dict[str, Any],
        *,
        session_id: str | None,
        fallback_session: str,
        offset: int,
    ) -> tuple[int, int, list[RawActivity]]:
        message = record.get("message")
        if not isinstance(message, dict) or "content" not in message:
            raise self._drift(detected_keys=sorted(str(k) for k in record), raw=_dump(record))
        content = message["content"]
        if not isinstance(content, list):
            raise self._drift(
                detected_keys=["message.content:" + type(content).__name__], raw=_dump(record)
            )

        sess = session_id or str(record.get("sessionId") or "") or fallback_session
        host_version = str(record.get("version") or _HOST_VERSION_FALLBACK)
        cwd = record.get("cwd")
        record_uuid = str(record.get("uuid") or offset)
        occurred_at = _parse_ts(record.get("timestamp"))

        # A text block is the turn's FINAL answer (Stop-hook territory) UNLESS the same record also
        # calls a tool — then that text is provably mid-turn narration (reasoning we may keep).
        has_tool_use = any(isinstance(b, dict) and b.get("type") == "tool_use" for b in content)

        thinking_seen = 0
        thinking_plaintext = 0
        acts: list[RawActivity] = []
        for block_index, block in enumerate(content):
            if not isinstance(block, dict) or "type" not in block:
                raise self._drift(detected_keys=["block#" + str(block_index)], raw=_dump(record))
            btype = block.get("type")
            reasoning_text: str | None = None
            origin: str
            if btype == "thinking":
                thinking_seen += 1
                text = str(block.get("thinking") or "")
                if text.strip():
                    thinking_plaintext += 1
                    reasoning_text = text
                    origin = "thinking_block"
                else:
                    continue  # signature-only / empty extended-thinking (F1) — yield nothing
            elif btype == "text" and has_tool_use:
                reasoning_text = str(block.get("text") or "")
                origin = "intermediate_msg"
            else:
                # text-only final answer (Stop owns it), tool_use, tool_result, images — not ours.
                continue
            if reasoning_text is None or not reasoning_text.strip():
                continue
            acts.extend(
                self._activities_from_text(
                    reasoning_text,
                    session_id=sess,
                    host_version=host_version,
                    cwd=cwd,
                    occurred_at=occurred_at,
                    record_uuid=record_uuid,
                    block_index=block_index,
                    origin=origin,
                )
            )
        return thinking_seen, thinking_plaintext, acts

    def _activities_from_text(
        self,
        reasoning_text: str,
        *,
        session_id: str,
        host_version: str,
        cwd: Any,
        occurred_at: datetime,
        record_uuid: str,
        block_index: int,
        origin: str,
    ) -> list[RawActivity]:
        acts: list[RawActivity] = []
        for sig_index, signal in enumerate(extract_reasoning_signal(reasoning_text)):
            # Deterministic, stable per candidate ⇒ a re-tail of the same bytes re-derives the SAME
            # activity_id and the outbox INSERT OR IGNORE makes it a no-op (idempotent backfill).
            source_offset = f"{record_uuid}:{block_index}:{sig_index}"
            kind = ActivityKind.ASSISTANT_MSG
            acts.append(
                RawActivity(
                    activity_id=_activity_id(
                        session_id=session_id, source_offset=source_offset, kind=kind
                    ),
                    host=self.host,
                    host_version=host_version,
                    schema_version=self.schema_version,
                    kind=kind,
                    session_id=session_id,
                    cwd=str(cwd) if cwd is not None else None,
                    occurred_at=occurred_at,
                    text=signal.text,
                    content_hash=_sha256_text(signal.text),
                    source_offset=source_offset,
                    provenance_id=_provenance_id(
                        session_id=session_id, source_offset=source_offset, kind=kind
                    ),
                    importance=self._importance_for(signal.kind),
                    payload={
                        "reasoning": True,
                        "source": THINKING_SOURCE,
                        "signal_kind": signal.kind.value,
                        "origin": origin,
                    },
                )
            )
        return acts

    def _drift(self, *, detected_keys: list[str], raw: bytes) -> CaptureSchemaDriftError:
        return CaptureSchemaDriftError(
            host=self.host.value,
            source_id=f"claude_transcript:{self.schema_version}",
            detected_keys=detected_keys,
            expected_schema=[self.schema_version],
            raw_sample_sha256=hashlib.sha256(raw).hexdigest(),
        )


def _dump(record: dict[str, Any]) -> bytes:
    return json.dumps(record, sort_keys=True, default=str).encode("utf-8")


def _parse_ts(value: Any) -> datetime:
    """Parse the transcript's ISO-8601 ``timestamp`` (e.g. ``2026-08-09T23:45:30.230Z``); fall back
    to now(UTC) when absent/malformed — provenance, never load-bearing for correctness."""
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return datetime.now(UTC)
    return datetime.now(UTC)


class BackfillResult(BaseModel, frozen=True):
    """Content-free summary of one :func:`backfill_thinking` run (counts only — never text)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    appended: int
    end_offset: int
    since_byte: int
    records_scanned: int
    thinking_blocks_seen: int
    thinking_blocks_plaintext: int
    halted: bool = False  # True ⇒ SchemaDriftError halted THIS source (never blocks the host turn)


def _source_id(session_id: str) -> str:
    """The per-session resume key in the outbox ``checkpoints`` table — one cursor per transcript so
    every session backfills independently and idempotently."""
    return f"claude_transcript:{session_id}"


async def backfill_thinking(
    settings: ClientSettings,
    *,
    transcript_path: Path,
    session_id: str | None = None,
) -> BackfillResult:
    """Run the tailer over ``transcript_path`` from its persisted checkpoint and append every
    reasoning candidate to the SAME durable outbox the hook capture writes (``mu flush`` / the
    daemon worker then drives them into the real stores through
    :class:`~mu_client.workers.ingest_client.InProcessLocalIngest`). Resumes from the per-session
    ``byte_offset`` checkpoint and advances it in the SAME transaction as the last append
    (capture-spec.md §8.3), so a re-run only ever re-reads append-stable bytes and
    ``UNIQUE(activity_id)`` makes any overlap a no-op.

    A :class:`~mu_client.errors.CaptureSchemaDriftError` halts THIS source only, loud
    (``CaptureSourceHalted`` event) — it is caught here and reported as ``halted=True`` so a caller
    on the host's Stop/PreCompact path (:mod:`mu_client.capture.hook`) never lets a transcript-shape
    drift block the primary hook capture (capture-spec.md §6)."""
    tailer = ClaudeCodeTranscriptTailer(
        decision_importance=settings.capture.thinking_decision_importance,
        finding_importance=settings.capture.thinking_finding_importance,
    )
    outbox = SqliteOutbox(settings.outbox.outbox_path)
    await outbox.open()
    try:
        sid = session_id or transcript_path.expanduser().stem
        checkpoint = await outbox.load_checkpoint(_source_id(sid))
        since = checkpoint.byte_offset if checkpoint is not None else 0
        try:
            result = await asyncio.to_thread(
                tailer.tail, transcript_path, session_id=session_id, since_byte=since
            )
        except CaptureSchemaDriftError as exc:
            log_capture_source_halted(
                host=tailer.host.value,
                schema_version=tailer.schema_version,
                raw_sample_sha256=exc.raw_sample_sha256,
            )
            return BackfillResult(
                appended=0,
                end_offset=since,
                since_byte=since,
                records_scanned=0,
                thinking_blocks_seen=0,
                thinking_blocks_plaintext=0,
                halted=True,
            )

        appended = 0
        for index, activity in enumerate(result.activities):
            # Advance the resume checkpoint atomically with the LAST append (one fsync'd txn).
            checkpoint_update = (
                CaptureCheckpoint(
                    source_id=_source_id(activity.session_id),
                    file_path=str(transcript_path.expanduser()),
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
        return BackfillResult(
            appended=appended,
            end_offset=result.end_offset,
            since_byte=since,
            records_scanned=result.records_scanned,
            thinking_blocks_seen=result.thinking_blocks_seen,
            thinking_blocks_plaintext=result.thinking_blocks_plaintext,
        )
    finally:
        await outbox.aclose()
