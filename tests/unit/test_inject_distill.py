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
    content: str,
    *,
    channel: str = "stm",
    fused_score: float = 1.0,
    is_floor: bool = False,
    artifact_ref: str | None = None,
    mid: str | None = None,
) -> RecallItemView:
    tier = {"stm": Tier.STM, "mtm": Tier.MTM, "ltm": Tier.LTM}[channel]
    return RecallItemView(
        # `mid` is an explicit override because the default keys on the CONTENT, and a pointer
        # hit's content is empty by definition — two distinct pointers would otherwise share one
        # memory_id and the test would be asserting on an artefact of the helper.
        memory_id=mid or f"m-{abs(hash(content)) % 10_000}",
        content=content,
        tier=tier,
        channel=channel,
        fused_score=fused_score,
        is_floor=is_floor,
        artifact_ref=artifact_ref,
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


def test_a_pointer_hit_survives_distillation_and_dedupes_on_its_artifact_id() -> None:
    """A `kind=reference` hit has NO inline body — that is what makes it a pointer (§6). Keying
    dedup on `content` alone dropped it here before `slab_from_recall_item` could classify it, so
    the whole pointer/hydration path was unreachable through `RecallInjectBridge` (AD-199).

    Two pointers at the SAME artifact are the same body and still collapse; an empty row with no
    pointer at all is still nothing and is still dropped."""
    kept = distill_items(
        [
            _item("", artifact_ref="art-1", mid="p1"),
            _item("", artifact_ref="art-1", mid="p2"),  # same artifact -> same body -> collapses
            _item("", artifact_ref="art-2", mid="p3"),
            _item("", mid="p4"),  # no body and no pointer -> nothing to say
            _item("the on-call is Ada"),
        ]
    )

    assert [(i.content, i.artifact_ref) for i in kept] == [
        ("", "art-1"),
        ("", "art-2"),
        ("the on-call is Ada", None),
    ]


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
