"""Unit tests for the deterministic inject distiller (validation gap D) — isolated, no stores.

Proves the three passes in :mod:`mu_client.inject.distill`: tool-noise filtering (drop
``<ToolName>: …`` capture/output, keep human facts), content dedup, and the promoted/salient-first
ordering (the query-insensitive STM recency floor sinks below the ranked hits). Pure logic — the
real-store proof that the same filter runs over genuine recall hits is the integration test.
"""

from __future__ import annotations

import pytest
from mu_contracts.contracts.recall import RecallItemView
from mu_contracts.domain.model.memory import Tier

from mu_client.inject.distill import NOISE_TOOL_NAMES, distill_items, is_tool_noise

pytestmark = pytest.mark.unit


def _item(
    content: str, *, channel: str = "stm", fused_score: float = 1.0, is_floor: bool = False
) -> RecallItemView:
    tier = {"stm": Tier.STM, "mtm": Tier.MTM, "ltm": Tier.LTM}[channel]
    return RecallItemView(
        memory_id=f"m-{abs(hash(content)) % 10_000}",
        content=content,
        tier=tier,
        channel=channel,
        fused_score=fused_score,
        is_floor=is_floor,
    )


# --------------------------------------------------------------------------------- is_tool_noise
@pytest.mark.parametrize(
    "content",
    [
        'Write: {"file_path": "/app/main.py", "content": "x=1"}',
        "Bash: total 48\ndrwxr-xr-x 2 user user 4096 main.py",
        "Read: file contents here",
        "Edit: replaced foo with bar",
        "Grep: 3 matches in 2 files",
        "TodoWrite: 4 items",
        "mcp__serena__find_symbol: [{...}]",
    ],
)
def test_tool_captures_are_noise(content: str) -> None:
    assert is_tool_noise(content) is True


@pytest.mark.parametrize(
    "content",
    [
        "My deploy target is staging-eu",
        "The on-call engineer is Ada",
        "Note: prefer the staging cluster for canaries",  # 'Note' is not a tool name
        "Decision: we ship on Friday",
        "Meeting at 12:30 tomorrow",  # a timestamp colon, not a tool prefix
        "See http://example.com/docs for details",  # a URL, not a tool prefix
        "plain fact with no colon at all",
    ],
)
def test_human_facts_are_kept(content: str) -> None:
    assert is_tool_noise(content) is False


def test_every_noise_name_is_detected() -> None:
    for name in NOISE_TOOL_NAMES:
        assert is_tool_noise(f"{name}: some captured outcome") is True


# --------------------------------------------------------------------------------- distill_items
def test_distill_filters_noise_keeps_facts() -> None:
    items = [
        _item("My deploy target is staging-eu"),
        _item('Write: {"file_path": "/x.py"}'),
        _item("The on-call engineer is Ada"),
        _item("Bash: total 48"),
    ]
    kept = [it.content for it in distill_items(items)]
    assert kept == ["My deploy target is staging-eu", "The on-call engineer is Ada"]


def test_distill_dedupes_by_normalised_content() -> None:
    items = [
        _item("Ada lives in Paris"),
        _item("ada   lives in paris"),  # whitespace + case variant of the same fact
        _item("Ada lives in Paris"),
    ]
    assert len(distill_items(items)) == 1


def test_distill_sinks_query_insensitive_floor_below_ranked_hits() -> None:
    # A genuinely-ranked promoted hit + a recency-FLOOR STM dump item. The ranked hit injects first
    # even though the floor item came earlier in the input list.
    items = [
        _item("floor recency item", is_floor=True),
        _item("ranked salient fact", channel="mtm", fused_score=0.9, is_floor=False),
    ]
    ordered = [it.content for it in distill_items(items)]
    assert ordered == ["ranked salient fact", "floor recency item"]


def test_distill_preserves_recall_order_within_a_group() -> None:
    # Stable sort: same is_floor group keeps recall's own fused order (input order here).
    items = [_item("first"), _item("second"), _item("third")]
    assert [it.content for it in distill_items(items)] == ["first", "second", "third"]


def test_distill_empty_input_is_empty() -> None:
    assert distill_items([]) == []
