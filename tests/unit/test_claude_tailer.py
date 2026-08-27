"""``ClaudeCodeTranscriptTailer`` + ``extract_reasoning_signal`` — pure signal/noise logic and
transcript parsing over REAL-shape Claude Code 2.1.226 fixtures (no mocks, no I/O beyond a temp
transcript file). Proves: decisions/findings survive the noise filter with the right importance
band, pure boilerplate is dropped, the FINAL answer is left to the Stop hook, empty/signature-only
thinking yields nothing (F1), and a drifted record halts the source loud."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mu_client.capture.claude_tailer import (
    THINKING_SOURCE,
    ClaudeCodeTranscriptTailer,
    SignalKind,
    extract_reasoning_signal,
)
from mu_client.capture.model import ActivityKind, HostKind
from mu_client.errors import CaptureSchemaDriftError

pytestmark = pytest.mark.unit

_DECISION_IMPORTANCE = 0.7
_FINDING_IMPORTANCE = 0.55


def _tailer() -> ClaudeCodeTranscriptTailer:
    return ClaudeCodeTranscriptTailer(
        decision_importance=_DECISION_IMPORTANCE, finding_importance=_FINDING_IMPORTANCE
    )


# --------------------------------------------------------------------------- pure signal/noise
def test_decision_sentence_survives_as_decision() -> None:
    signals = extract_reasoning_signal(
        "I've decided to use staging-eu-west because the prod cluster is frozen this week."
    )
    assert [s.kind for s in signals] == [SignalKind.DECISION]
    assert "staging-eu-west" in signals[0].text


def test_finding_sentence_survives_as_finding() -> None:
    signals = extract_reasoning_signal(
        "Looks like the timeout is the real problem here, the retries never fire."
    )
    assert len(signals) == 1
    assert signals[0].kind is SignalKind.FINDING


def test_pure_noise_is_dropped_entirely() -> None:
    """The load-bearing property: boilerplate reasoning yields ZERO candidates, so pure-noise
    lines never each become a memory."""
    noise = (
        "Let me read the file. Okay. Now let me look at the config. "
        "First I need to understand the structure. Let me check the tests."
    )
    assert extract_reasoning_signal(noise) == []


def test_short_marker_fragment_is_not_a_candidate() -> None:
    # "Decided." trips the marker substring but is below the minimum-length floor → dropped.
    assert extract_reasoning_signal("Decided.") == []


def test_decision_beats_finding_when_both_markers_present() -> None:
    signals = extract_reasoning_signal(
        "I notice the cache is stale, therefore I chose to invalidate it on every write."
    )
    assert len(signals) == 1
    assert signals[0].kind is SignalKind.DECISION


# --------------------------------------------------------------------- real-shape transcript parse
def _assistant_record(
    *, blocks: list[dict[str, object]], uuid: str, sidechain: bool = False
) -> dict[str, object]:
    """A REAL Claude Code 2.1.226 assistant transcript record shape (verified on-disk)."""
    return {
        "type": "assistant",
        "uuid": uuid,
        "parentUuid": "p-" + uuid,
        "isSidechain": sidechain,
        "sessionId": "sess-abc",
        "timestamp": "2026-08-09T23:45:30.230Z",
        "cwd": "/home/user/project",
        "version": "2.1.226",
        "gitBranch": "main",
        "requestId": "req-" + uuid,
        "message": {"role": "assistant", "content": blocks},
    }


def _write_transcript(path: Path, records: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(r) + "\n" for r in records), encoding="utf-8")


def test_thinking_block_decision_lands_attributed_as_thinking(tmp_path: Path) -> None:
    path = tmp_path / "sess-abc.jsonl"
    _write_transcript(
        path,
        [
            _assistant_record(
                uuid="u1",
                blocks=[
                    {
                        "type": "thinking",
                        "thinking": "The prod cluster is frozen, so I decided to deploy to "
                        "staging-eu-west instead. Let me open the pipeline config.",
                        "signature": "sig-xyz",
                    },
                    {"type": "tool_use", "id": "t1", "name": "Read", "input": {}},
                ],
            )
        ],
    )
    result = _tailer().tail(path)
    assert len(result.activities) == 1
    act = result.activities[0]
    assert act.host is HostKind.CLAUDE_CODE
    assert act.kind is ActivityKind.ASSISTANT_MSG
    assert act.payload["source"] == THINKING_SOURCE
    assert act.payload["reasoning"] is True
    assert act.payload["origin"] == "thinking_block"
    assert act.importance == _DECISION_IMPORTANCE  # a decision promotes (≥ importance_promote 0.6)
    assert "staging-eu-west" in (act.text or "")
    assert result.thinking_blocks_seen == 1
    assert result.thinking_blocks_plaintext == 1


def test_empty_signature_only_thinking_yields_nothing_f1(tmp_path: Path) -> None:
    """capture-spec.md §6 F1: a signature-only (empty plaintext) thinking block — the real shape
    observed on-disk — yields nothing, never an error."""
    path = tmp_path / "sess-abc.jsonl"
    _write_transcript(
        path,
        [
            _assistant_record(
                uuid="u1",
                blocks=[
                    {"type": "thinking", "thinking": "", "signature": "sig-only"},
                    {"type": "tool_use", "id": "t1", "name": "Bash", "input": {}},
                ],
            )
        ],
    )
    result = _tailer().tail(path)
    assert result.activities == []
    assert result.thinking_blocks_seen == 1
    assert result.thinking_blocks_plaintext == 0


def test_final_answer_text_only_record_is_not_captured(tmp_path: Path) -> None:
    """A text-only assistant record is the turn's FINAL answer — the Stop hook owns it, the tailer
    must NOT re-emit it (F3 dual-source dedup: reasoning-only by construction)."""
    path = tmp_path / "sess-abc.jsonl"
    _write_transcript(
        path,
        [
            _assistant_record(
                uuid="u1",
                blocks=[{"type": "text", "text": "I decided to deploy to staging-eu-west. Done."}],
            )
        ],
    )
    assert _tailer().tail(path).activities == []


def test_intermediate_text_with_tool_use_is_captured(tmp_path: Path) -> None:
    """A text block in the SAME record as a tool_use is provably mid-turn narration → captured."""
    path = tmp_path / "sess-abc.jsonl"
    _write_transcript(
        path,
        [
            _assistant_record(
                uuid="u1",
                blocks=[
                    {"type": "text", "text": "The root cause is a stale lease TTL; patching now."},
                    {"type": "tool_use", "id": "t1", "name": "Edit", "input": {}},
                ],
            )
        ],
    )
    acts = _tailer().tail(path).activities
    assert len(acts) == 1
    assert acts[0].payload["origin"] == "intermediate_msg"
    assert acts[0].importance == _DECISION_IMPORTANCE  # "root cause" is a decision marker


def test_sidechain_records_are_skipped(tmp_path: Path) -> None:
    path = tmp_path / "sess-abc.jsonl"
    _write_transcript(
        path,
        [
            _assistant_record(
                uuid="u1",
                sidechain=True,
                blocks=[
                    {
                        "type": "thinking",
                        "thinking": "I decided to use approach B here.",
                        "signature": "s",
                    },
                    {"type": "tool_use", "id": "t1", "name": "Read", "input": {}},
                ],
            )
        ],
    )
    assert _tailer().tail(path).activities == []


def test_reuse_offset_is_idempotent_activity_id(tmp_path: Path) -> None:
    path = tmp_path / "sess-abc.jsonl"
    _write_transcript(
        path,
        [
            _assistant_record(
                uuid="u1",
                blocks=[
                    {
                        "type": "thinking",
                        "thinking": "I decided to shard by tenant id.",
                        "signature": "s",
                    },
                    {"type": "tool_use", "id": "t1", "name": "Read", "input": {}},
                ],
            )
        ],
    )
    first = _tailer().tail(path).activities
    second = _tailer().tail(path).activities  # a full re-tail derives the SAME ids
    assert [a.activity_id for a in first] == [a.activity_id for a in second]


def test_resume_from_byte_offset_only_reads_new_records(tmp_path: Path) -> None:
    path = tmp_path / "sess-abc.jsonl"
    _write_transcript(
        path,
        [
            _assistant_record(
                uuid="u1",
                blocks=[
                    {"type": "thinking", "thinking": "I decided to use plan A.", "signature": "s"},
                    {"type": "tool_use", "id": "t1", "name": "Read", "input": {}},
                ],
            )
        ],
    )
    first = _tailer().tail(path)
    assert len(first.activities) == 1
    # Append a second turn, resume from the checkpoint — only the NEW record is parsed.
    with path.open("a", encoding="utf-8") as fh:
        fh.write(
            json.dumps(
                _assistant_record(
                    uuid="u2",
                    blocks=[
                        {
                            "type": "thinking",
                            "thinking": "I decided to use plan B.",
                            "signature": "s",
                        },
                        {"type": "tool_use", "id": "t2", "name": "Read", "input": {}},
                    ],
                )
            )
            + "\n"
        )
    second = _tailer().tail(path, since_byte=first.end_offset)
    assert len(second.activities) == 1
    assert "plan B" in (second.activities[0].text or "")


def test_half_written_trailing_line_is_left_for_next_tail(tmp_path: Path) -> None:
    """capture-spec.md §6 transcript-lag guard: a trailing record with no newline is a
    half-written line — skipped, and end_offset stays before it so the next tail re-reads it."""
    path = tmp_path / "sess-abc.jsonl"
    complete = json.dumps(
        _assistant_record(
            uuid="u1",
            blocks=[
                {"type": "thinking", "thinking": "I decided to use plan A.", "signature": "s"},
                {"type": "tool_use", "id": "t1", "name": "Read", "input": {}},
            ],
        )
    )
    partial = '{"type": "assistant", "message": {"content": [{"type": "thin'  # truncated
    path.write_text(complete + "\n" + partial, encoding="utf-8")
    result = _tailer().tail(path)
    assert len(result.activities) == 1
    assert result.end_offset == len(complete) + 1  # exactly past the one complete line


def test_drifted_assistant_record_halts_source_loud(tmp_path: Path) -> None:
    path = tmp_path / "sess-abc.jsonl"
    # An assistant record whose message.content is an object, not the expected list → drift.
    path.write_text(
        json.dumps({"type": "assistant", "uuid": "u1", "message": {"content": {"bad": "shape"}}})
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(CaptureSchemaDriftError):
        _tailer().tail(path)


def test_non_assistant_records_are_skipped_not_drift(tmp_path: Path) -> None:
    path = tmp_path / "sess-abc.jsonl"
    path.write_text(
        json.dumps({"type": "user", "message": {"content": "hi"}})
        + "\n"
        + json.dumps({"type": "attachment", "attachment": {}})
        + "\n",
        encoding="utf-8",
    )
    result = _tailer().tail(path)
    assert result.activities == []
    assert result.records_scanned == 2
