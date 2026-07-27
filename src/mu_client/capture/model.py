"""``RawActivity`` / ``ActivityKind`` / ``HostKind`` — the ONLY types crossing the adapter edge
(capture-spec.md §4.1, verbatim shape). Raw host bytes never cross this edge as a field; every
payload here is either a validated enum, an id, a hash, or a bounded salient-slice ``text`` a
parser has ALREADY filtered (capture-spec.md §7.1's kind-gate + salient-slice discipline lives in
:mod:`mu_client.capture.parsers`, not here — this module is pure data shape).
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

__all__ = ["ActivityKind", "CaptureCheckpoint", "HostKind", "RawActivity"]


class HostKind(StrEnum):
    """capture-spec.md §4.1. Only ``CLAUDE_CODE`` has a live parser this stage (REVIEW-CHECKLIST /
    MVP-BUILD-PLAN scope); ``CODEX``/``CLAUDE_DESKTOP`` are named so the closed enum does not need
    a breaking change when their parsers land."""

    CLAUDE_CODE = "claude_code"
    CODEX = "codex"
    CLAUDE_DESKTOP = "claude_desktop"


class ActivityKind(StrEnum):
    """capture-spec.md §4.1."""

    USER_PROMPT = "user_prompt"  # a human turn
    ASSISTANT_MSG = "assistant_msg"  # a model turn (FINAL text) or a reasoning block (backfill)
    TOOL_USE = "tool_use"  # PostToolUse / command exec / MCP call
    SUBAGENT_RUN = "subagent_run"  # SubagentStop
    FILE_CHANGE = "file_change"  # patch / apply_patch
    TURN_COMPLETE = "turn_complete"  # refresh trigger co-fired by Stop (control only)
    SESSION_META = "session_meta"  # session_start / session_meta (control)
    PRE_COMPACT = "pre_compact"  # promote-before-delete trigger (control)
    SESSION_END = "session_end"  # flush trigger (control)


# Pure-control kinds: capture-spec.md §7.1 kind-gate — these NEVER carry ``text`` and never
# become a MemoryItem; they are triggers/provenance only.
CONTROL_KINDS: frozenset[ActivityKind] = frozenset(
    {
        ActivityKind.TURN_COMPLETE,
        ActivityKind.SESSION_META,
        ActivityKind.SESSION_END,
        ActivityKind.PRE_COMPACT,
    }
)


class RawActivity(BaseModel, frozen=True):
    """The ONLY type crossing the adapter edge (capture-spec.md §4.1). ``content_hash =
    sha256(salient text)`` mirrors ``LocalMemoryClient.content_hash`` so a replay cannot fabricate
    a second reinforcement (capture-spec.md §7.1 layer 3)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    activity_id: str  # sha256(host|session|source_offset|kind) — deterministic dedupe key
    host: HostKind
    host_version: str  # cli_version / app build (provenance + drift signal)
    schema_version: str  # the parser contract that produced this record
    kind: ActivityKind
    session_id: str
    cwd: str | None = None
    occurred_at: datetime
    text: str | None = None  # salient slice only; None for pure-control kinds
    content_hash: str | None = None  # sha256(text); None when text is None
    source_offset: str | None = None  # hook event_id / tailer byte offset (resume + dedupe)
    provenance_id: str = Field(min_length=1)  # "prov_"+sha256(host|session|source_offset|kind)[:24]
    payload: dict[str, str | int | bool | None] = Field(default_factory=dict)


class CaptureCheckpoint(BaseModel, frozen=True):
    """capture-spec.md §4.1 — per-source resume offset (tailers only this stage has none live, but
    the shape is pinned now so a later tailer needs no outbox schema change)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source_id: str
    file_path: str | None = None
    byte_offset: int = 0
    updated_at: datetime
