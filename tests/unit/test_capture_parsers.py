"""``ClaudeCodeParserV1`` + ``ParserRegistry`` — pure logic, no I/O."""

from __future__ import annotations

import pytest

from mu_client.capture.model import ActivityKind, HostKind
from mu_client.capture.parsers import ClaudeCodeParserV1, ParserRegistry
from mu_client.errors import CaptureSchemaDriftError

pytestmark = pytest.mark.unit


def _registry() -> ParserRegistry:
    reg = ParserRegistry()
    reg.register(ClaudeCodeParserV1())
    return reg


def test_user_prompt_submit_keeps_prompt_verbatim() -> None:
    record = {"hook_event_name": "UserPromptSubmit", "session_id": "s1", "prompt": "hello there"}
    parser = _registry().select(HostKind.CLAUDE_CODE, record)
    activity = parser.parse(record=record, event_id="e1")
    assert activity.kind is ActivityKind.USER_PROMPT
    assert activity.text == "hello there"
    assert activity.content_hash is not None
    assert activity.session_id == "s1"
    assert activity.provenance_id.startswith("prov_")


def test_slash_command_is_dropped_at_the_kind_gate() -> None:
    record = {"hook_event_name": "UserPromptSubmit", "session_id": "s1", "prompt": "/help"}
    activity = _registry().select(HostKind.CLAUDE_CODE, record).parse(record=record, event_id="e1")
    assert activity.kind is ActivityKind.USER_PROMPT
    assert activity.text is None
    assert activity.content_hash is None


def test_stop_keeps_the_final_answer_f2_regression_guard() -> None:
    """capture-spec.md F2: Stop must NOT be control-only — the agent's final answer is KEPT."""
    record = {
        "hook_event_name": "Stop",
        "session_id": "s1",
        "last_assistant_message": "Ada lives in Paris.",
    }
    activity = _registry().select(HostKind.CLAUDE_CODE, record).parse(record=record, event_id="e1")
    assert activity.kind is ActivityKind.ASSISTANT_MSG
    assert activity.text == "Ada lives in Paris."


def test_post_tool_use_drops_raw_blob_keeps_truncated_outcome() -> None:
    huge_response = "x" * 5000
    record = {
        "hook_event_name": "PostToolUse",
        "session_id": "s1",
        "tool_name": "Bash",
        "tool_use_id": "t1",
        "tool_input": {"command": "some secret command echo"},
        "tool_response": huge_response,
    }
    activity = ClaudeCodeParserV1(tool_outcome_max_chars=100).parse(record=record, event_id="e1")
    assert activity.kind is ActivityKind.TOOL_USE
    assert activity.text is not None
    assert len(activity.text) < 200
    assert "x" * 5000 not in activity.text  # the raw blob never appears
    assert activity.payload["tool_error"] is False


def test_control_kinds_never_carry_text() -> None:
    for event, kind in (
        ("SessionStart", ActivityKind.SESSION_META),
        ("SessionEnd", ActivityKind.SESSION_END),
        ("PreCompact", ActivityKind.PRE_COMPACT),
    ):
        record = {"hook_event_name": event, "session_id": "s1"}
        activity = (
            _registry().select(HostKind.CLAUDE_CODE, record).parse(record=record, event_id="e1")
        )
        assert activity.kind is kind
        assert activity.text is None


def test_unknown_event_raises_schema_drift_not_a_guess() -> None:
    record = {"hook_event_name": "SomeFutureEvent", "session_id": "s1"}
    with pytest.raises(CaptureSchemaDriftError) as exc_info:
        _registry().select(HostKind.CLAUDE_CODE, record)
    assert exc_info.value.host == HostKind.CLAUDE_CODE.value
    assert "session_id" in exc_info.value.detected_keys


def test_activity_id_is_deterministic_for_identical_inputs() -> None:
    record = {"hook_event_name": "Stop", "session_id": "s1", "last_assistant_message": "hi"}
    a = _registry().select(HostKind.CLAUDE_CODE, record).parse(record=record, event_id="same")
    b = _registry().select(HostKind.CLAUDE_CODE, record).parse(record=record, event_id="same")
    assert a.activity_id == b.activity_id
    assert a.provenance_id == b.provenance_id
