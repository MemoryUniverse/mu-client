"""``CodexParserV1`` / ``CodexRolloutTailer`` / ``CodexNotifyParserV1`` — the Phase-4 codex capture
surface, over REAL codex-cli 0.146.0 shapes (no mocks). The rollout-line shapes below are verified
against real ``~/.codex/sessions/.../rollout-*.jsonl`` records; the notify envelope is the VERBATIM
JSON a live ``codex exec`` run wrote to its notify program. A genuinely-captured real rollout file
(``tests/fixtures/codex_rollout_real.jsonl``) is also tailed end-to-end.

Proves: user/assistant/tool turns are captured from the right stream (no double-capture), noise/
encrypted/control lines are skipped, byte-offset resume + idempotent activity_id, half-written
trailing line guard, and envelope drift halts the source loud (but an UNKNOWN event kind does not).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mu_client.capture.codex import (
    CodexNotifyParserV1,
    CodexRolloutTailer,
    session_id_from_rollout_name,
)
from mu_client.capture.model import ActivityKind, HostKind
from mu_client.errors import CaptureSchemaDriftError

pytestmark = pytest.mark.unit

_FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "codex_rollout_real.jsonl"
_SESSION = "019fe954-5a48-7992-885e-ede757dbd3eb"


# ------------------------------------------------------------------------- real rollout line shapes
def _session_meta(*, session_id: str = _SESSION) -> dict[str, object]:
    return {
        "timestamp": "2026-08-10T04:40:47.000Z",
        "type": "session_meta",
        "payload": {
            "session_id": session_id,
            "id": session_id,
            "cwd": "/home/user/project",
            "originator": "codex_exec",
            "cli_version": "0.146.0",
            "source": "exec",
        },
    }


def _event_msg(ptype: str, **fields: object) -> dict[str, object]:
    return {
        "timestamp": "2026-08-10T04:40:51.000Z",
        "type": "event_msg",
        "payload": {"type": ptype, **fields},
    }


def _response_item(payload: dict[str, object]) -> dict[str, object]:
    return {"timestamp": "2026-08-10T04:40:52.000Z", "type": "response_item", "payload": payload}


def _write(path: Path, records: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(r) + "\n" for r in records), encoding="utf-8")


def _tailer() -> CodexRolloutTailer:
    return CodexRolloutTailer(tool_input_max_chars=500)


# ------------------------------------------------------------------------------- CodexRolloutTailer
def test_user_and_assistant_turns_captured_from_event_msg_stream(tmp_path: Path) -> None:
    path = tmp_path / f"rollout-x-{_SESSION}.jsonl"
    _write(
        path,
        [
            _session_meta(),
            _event_msg("task_started"),  # skipped
            _response_item({"type": "message", "role": "user", "content": []}),  # role-echo skipped
            _event_msg("user_message", message="deploy target is staging-eu-west", images=[]),
            _event_msg("agent_message", message="Understood — staging-eu-west it is.",
                       phase="final_answer", memory_citation=None),
            _event_msg("token_count"),  # skipped
        ],
    )
    result = _tailer().tail(path)
    kinds = [(a.kind, a.text) for a in result.activities]
    assert kinds == [
        (ActivityKind.USER_PROMPT, "deploy target is staging-eu-west"),
        (ActivityKind.ASSISTANT_MSG, "Understood — staging-eu-west it is."),
    ]
    # session id + cwd + host_version resolved from the session_meta line.
    assert all(a.session_id == _SESSION for a in result.activities)
    assert all(a.host is HostKind.CODEX for a in result.activities)
    assert result.activities[0].cwd == "/home/user/project"
    assert result.activities[1].host_version == "codex/0.146.0"
    assert result.activities[1].payload["phase"] == "final_answer"


def test_tool_calls_captured_from_response_item_stream(tmp_path: Path) -> None:
    path = tmp_path / f"rollout-x-{_SESSION}.jsonl"
    _write(
        path,
        [
            _session_meta(),
            _response_item(
                {
                    "type": "custom_tool_call",
                    "id": "ctc_1",
                    "status": "completed",
                    "call_id": "call_1",
                    "name": "exec",
                    "input": "const r = await tools.exec_command({\"cmd\":\"ls\"});",
                }
            ),
            _response_item(
                {
                    "type": "function_call",
                    "id": "fc_1",
                    "call_id": "call_2",
                    "name": "apply_patch",
                    "arguments": "{\"patch\": \"*** Begin Patch\"}",
                }
            ),
        ],
    )
    acts = _tailer().tail(path).activities
    assert [a.kind for a in acts] == [ActivityKind.TOOL_USE, ActivityKind.TOOL_USE]
    assert acts[0].text is not None and acts[0].text.startswith("exec: ")
    assert acts[0].payload["tool_name"] == "exec"
    assert acts[0].payload["tool_kind"] == "custom_tool_call"
    assert acts[1].text is not None and acts[1].text.startswith("apply_patch: ")
    assert acts[1].payload["tool_kind"] == "function_call"


def test_encrypted_reasoning_and_control_lines_yield_nothing(tmp_path: Path) -> None:
    path = tmp_path / f"rollout-x-{_SESSION}.jsonl"
    _write(
        path,
        [
            _session_meta(),
            _response_item({"type": "reasoning", "id": "rs_1", "summary": [],
                            "encrypted_content": "gAAAA…"}),
            {"timestamp": "t", "type": "world_state", "payload": {"foo": 1}},
            {"timestamp": "t", "type": "turn_context", "payload": {"bar": 2}},
            _event_msg("token_count"),
            _event_msg("task_complete"),
        ],
    )
    result = _tailer().tail(path)
    assert result.activities == []
    assert result.records_scanned == 6  # every line scanned, none captured


def test_tool_call_truncated_to_budget(tmp_path: Path) -> None:
    path = tmp_path / f"rollout-x-{_SESSION}.jsonl"
    big = "x" * 5000
    _write(
        path,
        [
            _session_meta(),
            _response_item({"type": "custom_tool_call", "call_id": "c", "name": "exec",
                            "input": big}),
        ],
    )
    act = CodexRolloutTailer(tool_input_max_chars=100).tail(path).activities[0]
    assert act.text is not None
    assert len(act.text) < 200  # "exec: " + 100 chars + ellipsis, never the full 5000


def test_retail_is_idempotent_same_activity_ids(tmp_path: Path) -> None:
    path = tmp_path / f"rollout-x-{_SESSION}.jsonl"
    _write(path, [_session_meta(), _event_msg("user_message", message="remember X", images=[])])
    first = _tailer().tail(path).activities
    second = _tailer().tail(path).activities
    assert [a.activity_id for a in first] == [a.activity_id for a in second]


def test_resume_from_byte_offset_only_reads_new_records(tmp_path: Path) -> None:
    path = tmp_path / f"rollout-x-{_SESSION}.jsonl"
    _write(path, [_session_meta(), _event_msg("user_message", message="first", images=[])])
    first = _tailer().tail(path)
    assert len(first.activities) == 1
    with path.open("a", encoding="utf-8") as fh:
        second_line = _event_msg("agent_message", message="second", phase="final_answer")
        fh.write(json.dumps(second_line) + "\n")
    # A resume past line 0 (session_meta) must still stamp the right session (from the filename).
    second = _tailer().tail(path, since_byte=first.end_offset)
    assert len(second.activities) == 1
    assert second.activities[0].text == "second"
    assert second.activities[0].session_id == _SESSION  # recovered from filename, not lost


def test_half_written_trailing_line_left_for_next_tail(tmp_path: Path) -> None:
    path = tmp_path / f"rollout-x-{_SESSION}.jsonl"
    complete = json.dumps(_event_msg("user_message", message="complete turn", images=[]))
    partial = '{"type": "event_msg", "payload": {"type": "user_me'  # truncated, no newline
    meta = json.dumps(_session_meta())
    path.write_text(meta + "\n" + complete + "\n" + partial, encoding="utf-8")
    result = _tailer().tail(path)
    assert len(result.activities) == 1
    assert result.end_offset == len(meta) + 1 + len(complete) + 1  # exactly past the last complete


def test_non_json_line_halts_source_loud(tmp_path: Path) -> None:
    path = tmp_path / f"rollout-x-{_SESSION}.jsonl"
    path.write_text("this is not json\n", encoding="utf-8")
    with pytest.raises(CaptureSchemaDriftError):
        _tailer().tail(path)


def test_missing_envelope_type_halts_source_loud(tmp_path: Path) -> None:
    path = tmp_path / f"rollout-x-{_SESSION}.jsonl"
    path.write_text(json.dumps({"payload": {"type": "user_message"}}) + "\n", encoding="utf-8")
    with pytest.raises(CaptureSchemaDriftError):
        _tailer().tail(path)


def test_unknown_event_kind_is_skipped_not_drift(tmp_path: Path) -> None:
    """A valid {type,payload} envelope with a codex event kind we don't capture is SKIPPED (codex
    adds kinds across versions), never a drift halt."""
    path = tmp_path / f"rollout-x-{_SESSION}.jsonl"
    _write(
        path,
        [
            _session_meta(),
            _event_msg("some_future_event_kind_we_dont_know", data="whatever"),
            _event_msg("user_message", message="still captured after the unknown", images=[]),
        ],
    )
    result = _tailer().tail(path)
    assert len(result.activities) == 1
    assert result.activities[0].text == "still captured after the unknown"


def test_session_id_from_rollout_name() -> None:
    p = Path(f"rollout-2026-08-10T04-40-47-{_SESSION}.jsonl")
    assert session_id_from_rollout_name(p) == _SESSION
    assert session_id_from_rollout_name(Path("no-uuid-here.jsonl")) is None


# ----------------------------------------------------------------- genuinely-captured real rollout
def test_real_captured_rollout_file_tails_the_pong_turn() -> None:
    """The checked-in fixture is a REAL rollout written by a live ``codex exec`` run (a 'pong'
    turn). Tailing it yields exactly the user prompt + the assistant's final answer."""
    assert _FIXTURE.exists(), "real codex rollout fixture missing"
    result = _tailer().tail(_FIXTURE)
    texts = {(a.kind, a.text) for a in result.activities}
    assert (ActivityKind.USER_PROMPT, "Reply with exactly the word: pong") in texts
    assert (ActivityKind.ASSISTANT_MSG, "pong") in texts
    # session id recovered from the real session_meta line, not the filename.
    assert all(a.session_id == _SESSION for a in result.activities)
    assert all(a.host_version == "codex/0.146.0" for a in result.activities)


# ------------------------------------------------------------------------------ CodexNotifyParserV1
_REAL_NOTIFY = {
    "type": "agent-turn-complete",
    "thread-id": _SESSION,
    "turn-id": "019fe954-5aba-78a3-a754-c2e81cc89b5c",
    "cwd": "/home/user/D/abstract_project/mma",
    "client": "codex_exec",
    "input-messages": ["Reply with exactly the word: pong"],
    "last-assistant-message": "pong",
}


def test_notify_parser_matches_and_parses_real_agent_turn_complete() -> None:
    parser = CodexNotifyParserV1()
    assert parser.matches(_REAL_NOTIFY) is True
    assert parser.matches({"type": "task_started"}) is False
    act = parser.parse(record=_REAL_NOTIFY, event_id="ev1")
    assert act.host is HostKind.CODEX
    assert act.kind is ActivityKind.ASSISTANT_MSG
    assert act.text == "pong"
    assert act.session_id == _SESSION  # thread-id
    assert act.cwd == "/home/user/D/abstract_project/mma"
    assert act.payload["turn_id"] == "019fe954-5aba-78a3-a754-c2e81cc89b5c"
    assert act.content_hash is not None


def test_notify_parser_wrong_type_is_drift() -> None:
    parser = CodexNotifyParserV1()
    with pytest.raises(CaptureSchemaDriftError):
        parser.parse(record={"type": "not-a-turn"}, event_id="ev1")


def test_notify_parser_empty_final_message_yields_control_no_text() -> None:
    parser = CodexNotifyParserV1()
    act = parser.parse(
        record={"type": "agent-turn-complete", "thread-id": "s", "last-assistant-message": ""},
        event_id="ev1",
    )
    assert act.text is None  # nothing to remember → ingest skips it, never a fake memory
    assert act.content_hash is None
