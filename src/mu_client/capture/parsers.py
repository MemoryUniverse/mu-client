"""``HostSchemaParser`` + ``ParserRegistry`` (capture-spec.md §4.2) — normalize a host envelope
into exactly one :class:`~mu_client.capture.model.RawActivity`, fail-loud on drift (never a
best-effort guess). ``ClaudeCodeParserV1`` implements the §5.1/§8a mapping table for the ONE host
this stage wires (REVIEW-CHECKLIST scope: Claude Code only; Codex/Desktop are a later stage).

**Deviation (recorded):** capture-spec.md's ``event_id`` is minted by the Go hook-client's own
monotonic counter (§3); this stage has no Go binary (out of scope — see module docstring in
``daemon/app.py``), so the daemonless ``mu capture-once`` CLI (:mod:`mu_client.capture.hook`)
mints ``source_offset = sha256(raw_stdin_bytes)`` instead of a counter. This preserves the
load-bearing idempotency property the spec tests for ("a second IDENTICAL invocation is a
no-op insert" — capture-spec.md §13) at the cost of the spec's genuine-reinforcement-on-replay
distinction (a byte-identical hook fire, which is what a host double-fire actually looks like,
now collapses to one row; a content-different second occurrence still gets a new offset and is
captured, matching "a legitimate reinforcement").
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable

from mu_client.capture.model import ActivityKind, HostKind, RawActivity
from mu_client.errors import CaptureSchemaDriftError

__all__ = ["ClaudeCodeParserV1", "HostSchemaParser", "ParserRegistry"]

_HOST_VERSION = "mu-client/0.1.0-hookcli"


@runtime_checkable
class HostSchemaParser(Protocol):
    host: HostKind
    schema_version: str

    def matches(self, record: Mapping[str, object]) -> bool: ...  # cheap signature check

    def parse(self, *, record: Mapping[str, object], event_id: str) -> RawActivity: ...


class ParserRegistry:
    """Strategy + Registry (capture-spec.md §4.2): selects the first registered parser for a host
    whose ``matches()`` accepts the record; raises :class:`CaptureSchemaDriftError` if none does —
    NEVER a best-effort guess at an unrecognized shape."""

    def __init__(self) -> None:
        self._by_host: dict[HostKind, list[HostSchemaParser]] = {}

    def register(self, parser: HostSchemaParser) -> None:
        self._by_host.setdefault(parser.host, []).append(parser)

    def select(self, host: HostKind, record: Mapping[str, object]) -> HostSchemaParser:
        for parser in self._by_host.get(host, []):
            if parser.matches(record):
                return parser
        raise CaptureSchemaDriftError(
            host=host.value,
            source_id=host.value,
            detected_keys=sorted(str(k) for k in record),
            expected_schema=[p.schema_version for p in self._by_host.get(host, [])],
            raw_sample_sha256=_sha256_of(record),
        )


def _sha256_of(record: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(record, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _activity_id(*, host: HostKind, session_id: str, source_offset: str, kind: ActivityKind) -> str:
    return _sha256_text(f"{host.value}|{session_id}|{source_offset}|{kind.value}")


def _provenance_id(
    *, host: HostKind, session_id: str, source_offset: str, kind: ActivityKind
) -> str:
    return (
        "prov_"
        + _activity_id(host=host, session_id=session_id, source_offset=source_offset, kind=kind)[
            :24
        ]
    )


class ClaudeCodeParserV1:
    """Claude Code hook envelope → :class:`RawActivity` (capture-spec.md §5.1/§8a, schema v1 —
    the current ``hook_event_name`` vocabulary as of host-capture-integration-devdoc.md §2.1)."""

    host = HostKind.CLAUDE_CODE
    schema_version = "claude_code.v1"

    _KNOWN_EVENTS = frozenset(
        {
            "UserPromptSubmit",
            "PostToolUse",
            "PostToolUseFailure",
            "SubagentStop",
            "Stop",
            "SessionStart",
            "SessionEnd",
            "PreCompact",
        }
    )

    def __init__(self, *, tool_outcome_max_chars: int = 500) -> None:
        self._tool_outcome_max_chars = tool_outcome_max_chars

    def matches(self, record: Mapping[str, object]) -> bool:
        return (
            "hook_event_name" in record
            and "session_id" in record
            and record.get("hook_event_name") in self._KNOWN_EVENTS
        )

    def parse(self, *, record: Mapping[str, object], event_id: str) -> RawActivity:
        event = record["hook_event_name"]
        if event not in self._KNOWN_EVENTS:  # matches() already gated this; defence in depth
            raise CaptureSchemaDriftError(
                host=self.host.value,
                source_id=self.host.value,
                detected_keys=sorted(str(k) for k in record),
                expected_schema=sorted(self._KNOWN_EVENTS),
                raw_sample_sha256=_sha256_of(record),
            )
        session_id = str(record["session_id"])
        cwd = record.get("cwd")
        kind, text, payload = self._map_event(str(event), record)
        return RawActivity(
            activity_id=_activity_id(
                host=self.host, session_id=session_id, source_offset=event_id, kind=kind
            ),
            host=self.host,
            host_version=_HOST_VERSION,
            schema_version=self.schema_version,
            kind=kind,
            session_id=session_id,
            cwd=str(cwd) if cwd is not None else None,
            occurred_at=datetime.now(UTC),
            text=text,
            content_hash=_sha256_text(text) if text is not None else None,
            source_offset=event_id,
            provenance_id=_provenance_id(
                host=self.host, session_id=session_id, source_offset=event_id, kind=kind
            ),
            payload=payload,
        )

    # ------------------------------------------------------------------------------ per-event map
    def _map_event(
        self, event: str, record: Mapping[str, object]
    ) -> tuple[ActivityKind, str | None, dict[str, str | int | bool | None]]:
        if event == "UserPromptSubmit":
            prompt = str(record.get("prompt", ""))
            # capture-spec.md §7.1 layer 1: a bare slash-command echo is dropped at the kind
            # gate (no expansion available on this stdin-only surface) — text=None means the
            # worker (workers/pool.py) never turns it into a MemoryItem.
            text = None if prompt.strip().startswith("/") else prompt
            return ActivityKind.USER_PROMPT, text, {}
        if event in ("PostToolUse", "PostToolUseFailure"):
            tool_name = str(record.get("tool_name", "unknown_tool"))
            tool_use_id = record.get("tool_use_id")
            is_failure = event == "PostToolUseFailure"
            outcome = record.get("tool_error") if is_failure else record.get("tool_response")
            text = f"{tool_name}: {_truncate(str(outcome), self._tool_outcome_max_chars)}"
            return (
                ActivityKind.TOOL_USE,
                text,
                {
                    "tool_name": tool_name,
                    "tool_use_id": str(tool_use_id) if tool_use_id is not None else None,
                    "tool_error": is_failure,
                },
            )
        if event == "SubagentStop":
            subagent_text = record.get("last_assistant_message")
            return (
                ActivityKind.SUBAGENT_RUN,
                str(subagent_text) if subagent_text else None,
                {"agent_type": str(record.get("agent_type", ""))},
            )
        if event == "Stop":
            # F2 (capture-spec.md §5.1): KEEP the final answer — never control-only.
            final_text = record.get("last_assistant_message")
            return ActivityKind.ASSISTANT_MSG, str(final_text) if final_text else None, {}
        if event == "SessionStart":
            return (
                ActivityKind.SESSION_META,
                None,
                {"source": str(record.get("source", "")), "model": str(record.get("model", ""))},
            )
        if event == "SessionEnd":
            return ActivityKind.SESSION_END, None, {"reason": str(record.get("reason", ""))}
        if event == "PreCompact":
            return ActivityKind.PRE_COMPACT, None, {"trigger": str(record.get("trigger", ""))}
        raise AssertionError(f"unreachable: unmapped known event {event!r}")  # pragma: no cover


def _truncate(text: str, max_chars: int) -> str:
    return text if len(text) <= max_chars else text[:max_chars] + "…"
