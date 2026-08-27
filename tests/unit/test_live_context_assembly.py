"""The context-aware assembler — §5.1 selection, §5.2 per-section sub-budgets, §5.3 the lean
delta, §5.4 the etag gate, §6 pointers-in-state/bodies-by-id with a NAMED non-hydration marker.

`live-session-context-design.md` §4-§6, ratified as CANONICAL-CONTRACTS.md §7.22.

Isolated logic (the ``unit`` marker permits mocks): the HOST is mocked, the assembler is real. The
distinction that matters in this file is between defects and breaches — the first section is the
only one about a **privacy breach**; everything below it is quality.
"""

from __future__ import annotations

import os
import stat
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock

import pytest
from mu_contracts.contracts.live_context import (
    ContextSlab,
    LiveSessionContext,
    PrivateSlice,
    Section,
    SharedZone,
    ToolTurnState,
    content_hash_of,
)
from mu_contracts.contracts.recall import RecallItemView
from mu_contracts.domain.model.memory import Namespace, Tier, Visibility
from mu_local.views import MemoryListView

from mu_client.config import ClientSettings, InjectSettings
from mu_client.host import LocalMemoryHost
from mu_client.inject.live_context import (
    LiveContextSettings,
    SectionBudgets,
    assemble,
    prompt_tokens,
    slab_from_recall_item,
    update_recalled,
)
from mu_client.inject.recall_bridge import RecallInjectBridge

pytestmark = pytest.mark.unit

_NOW = datetime(2026, 8, 27, 12, 0, tzinfo=UTC)
_SESSION = "s1"


# ------------------------------------------------------------------------------- fixtures
def _listing(*items: RecallItemView) -> MemoryListView:
    return MemoryListView(items=list(items))


def _item(
    content: str,
    *,
    mid: str | None = None,
    is_floor: bool = False,
    score: float = 1.0,
    artifact_ref: str | None = None,
) -> RecallItemView:
    return RecallItemView(
        memory_id=mid or f"m-{abs(hash(content)) % 10_000}",
        content=content,
        tier=Tier.STM,
        channel="stm",
        fused_score=score,
        is_floor=is_floor,
        artifact_ref=artifact_ref,
    )


@pytest.fixture
def client_config() -> ClientSettings:
    return ClientSettings()


@pytest.fixture
async def started_host(
    monkeypatch: pytest.MonkeyPatch, client_config: ClientSettings
) -> LocalMemoryHost:
    fake_memory = AsyncMock()
    monkeypatch.setattr("mu_client.host.LocalMemory", lambda *a, **kw: fake_memory)
    host = LocalMemoryHost(client_config)
    await host.start()
    return host


def _ns(config: ClientSettings, *, user: str | None = None) -> Namespace:
    return Namespace(
        org=config.default_namespace,
        workspace=config.default_workspace,
        user=user or config.default_user,
        session=_SESSION,
        visibility=Visibility.PRIVATE,
    )


def _slab(
    text: str, *, visibility: Visibility, section: Section, salience: float = 1.0
) -> ContextSlab:
    return ContextSlab(
        slab_id=f"m-{abs(hash(text)) % 10_000}",
        content_hash=content_hash_of(text),
        text=text,
        section=section,
        visibility=visibility,
        salience=salience,
        is_floor=section is Section.RECENT,
    )


def _state(config: ClientSettings, **private: PrivateSlice) -> LiveSessionContext:
    return LiveSessionContext(
        namespace=_ns(config), session_id=_SESSION, private=dict(private), updated_at=_NOW
    )


# ============================================================ 1. PRIVACY (the breach, not a bug)
async def test_the_render_never_reaches_another_principals_private_slice(
    client_config: ClientSettings,
) -> None:
    """§1 `:107`: "each participant's host receives ``SharedZone ⊕ private[self]``, never another
    member's slice." A room's state holds EVERY participant's slice on the one object, so the
    single edit that turns this assembler into a cross-principal disclosure is an ordinary-looking
    loop over ``state.private``. There is none, and this is the test that says so."""
    state = _state(
        client_config,
        alice=PrivateSlice(
            recalled=(
                _slab(
                    "alice's salary is 200k",
                    visibility=Visibility.PRIVATE,
                    section=Section.RECALLED_MEMORY,
                ),
            )
        ),
        bob=PrivateSlice(
            recalled=(
                _slab(
                    "bob's medication is X",
                    visibility=Visibility.PRIVATE,
                    section=Section.RECALLED_MEMORY,
                ),
            )
        ),
    )
    assembled = await assemble(
        state,
        principal_id="alice",
        prompt="what do you know",
        budget_chars=10_000,
        settings=LiveContextSettings(),
    )
    assert "alice's salary is 200k" in assembled.body
    assert "bob's medication" not in assembled.body
    assert "bob's medication" not in assembled.full_body


async def test_the_shared_zone_carries_only_shared_content_into_the_block(
    client_config: ClientSettings,
) -> None:
    """§2.3 read from the render side: what lands in ``<session_state>``/``<recent>`` from the
    SHARED zone is shared-visibility content, because the zone cannot hold anything else. The
    private slabs still render — into the OWNER's block, which is a different question from what
    every participant sees."""
    shared_turn = _slab(
        "the team ships Friday", visibility=Visibility.SHARED, section=Section.RECENT
    )
    state = _state(
        client_config,
        alice=PrivateSlice(
            recency_floor=(
                _slab(
                    "alice pinged the CTO privately",
                    visibility=Visibility.PRIVATE,
                    section=Section.RECENT,
                ),
            )
        ),
    ).model_copy(
        update={
            "shared": SharedZone(
                running_summary="planning the release", recent_shared=(shared_turn,)
            )
        }
    )
    assembled = await assemble(
        state,
        principal_id="alice",
        prompt=None,
        budget_chars=10_000,
        settings=LiveContextSettings(),
    )
    # The owner sees both halves merged...
    assert "the team ships Friday" in assembled.body
    assert "alice pinged the CTO privately" in assembled.body
    # ...and the SHARED zone itself — the thing every other participant would read — holds only
    # the shared turn. Nothing in the private slice reached it.
    assert [s.text for s in state.shared.recent_shared] == ["the team ships Friday"]


# ================================================ 2. §5.3 — THE LEAN DELTA (the core mechanism)
async def test_a_fact_already_injected_this_session_is_not_re_sent(
    client_config: ClientSettings,
) -> None:
    """§5.3 arm 1 (`:189`): "A candidate slab whose ``content_hash`` is in ``injected_digest`` is
    **skipped** (it is already in the host's context window from an earlier inject)."

    This is the difference between the object and a cache. An implementation that re-sends
    everything every turn still looks correct in every other test in this file."""
    fact = _slab(
        "the deploy target is staging-eu",
        visibility=Visibility.PRIVATE,
        section=Section.RECALLED_MEMORY,
    )
    slice_ = PrivateSlice(recalled=(fact,)).with_injected((fact.content_hash,), bound=64)
    assembled = await assemble(
        _state(client_config, alice=slice_),
        principal_id="alice",
        prompt="where do we deploy",
        budget_chars=10_000,
        settings=LiveContextSettings(),
    )
    assert "staging-eu" not in assembled.body
    assert assembled.body == "", "the block was not lean — it re-sent what the host already had"
    assert assembled.deduped_count == 1


async def test_a_persona_brief_already_injected_is_not_re_shouted(
    client_config: ClientSettings,
) -> None:
    """§5.3 arm 1 applies to the persona brief like anything else. The brief is the most STABLE
    thing in the block — which makes it the single largest repeated cost if it is exempt, and the
    easiest exemption to leave in by accident because nothing else looks wrong."""
    brief = "terse; prefers Python; deploys on Fridays"
    slice_ = PrivateSlice(persona_brief=brief).with_injected((content_hash_of(brief),), bound=64)
    assembled = await assemble(
        _state(client_config, alice=slice_),
        principal_id="alice",
        prompt="anything",
        budget_chars=10_000,
        settings=LiveContextSettings(),
    )
    assert "<persona>" not in assembled.body
    assert assembled.deduped_count == 1


async def test_a_recalled_fact_colliding_with_the_recency_floor_renders_once(
    client_config: ClientSettings,
) -> None:
    """§5.3 arm 2 (`:190`): a recalled fact whose hash collides with a floor slab is "rendered once
    (in ``<recent>``, verbatim) and dropped from ``<recalled_memory>`` — the model already has it
    as a just-said turn"."""
    text = "we agreed to use FalkorDB"
    state = _state(
        client_config,
        alice=PrivateSlice(
            recency_floor=(_slab(text, visibility=Visibility.PRIVATE, section=Section.RECENT),),
            recalled=(_slab(text, visibility=Visibility.PRIVATE, section=Section.RECALLED_MEMORY),),
        ),
    )
    assembled = await assemble(
        state,
        principal_id="alice",
        prompt="graph store",
        budget_chars=10_000,
        settings=LiveContextSettings(),
    )
    assert assembled.body.count(text) == 1
    assert "<recent>" in assembled.body
    assert "<recalled_memory>" not in assembled.body


async def test_the_second_turn_over_the_bridge_injects_only_the_new_fact(
    started_host: LocalMemoryHost,
) -> None:
    """End to end over the real bridge: turn 1 injects two facts, turn 2's recall returns those
    two PLUS a new one, and the host receives only the new one. This is the owner's requirement —
    "retrieval must be prompt/context-aware and keep the injected context LEAN" — as observable
    behaviour rather than as a field on a DTO."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    bridge = RecallInjectBridge(started_host, settings=InjectSettings())

    recall.return_value = _listing(_item("the on-call is Ada"), _item("deploys go to staging-eu"))
    first = await bridge.render(_SESSION, query="who is on call")
    assert "the on-call is Ada" in first.body and "staging-eu" in first.body

    recall.return_value = _listing(
        _item("the on-call is Ada"),
        _item("deploys go to staging-eu"),
        _item("the release window is Friday 4pm"),
    )
    second = await bridge.render(_SESSION, query="when do we release")
    assert "the release window is Friday 4pm" in second.body
    assert (
        "the on-call is Ada" not in second.body
    ), "turn 2 re-sent a fact the host already had — this is a cache, not the §5.3 delta"
    assert "staging-eu" not in second.body


async def test_a_background_re_warm_does_not_consume_the_hosts_digest(
    started_host: LocalMemoryHost,
) -> None:
    """The digest records what was INJECTED, not what was rendered. A push re-warm renders into
    the cache and never reaches the host, so unioning its hashes would mark facts as "already in
    the host's window" that the host never received — and the user's next real pull would dedup
    them away forever. Silent, permanent context loss; the kind that looks like the feature
    working."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    bridge = RecallInjectBridge(started_host, settings=InjectSettings())
    recall.return_value = _listing(_item("the on-call is Ada"))

    await bridge.render(_SESSION, query="who is on call", for_host=False)
    host_turn = await bridge.render(_SESSION, query="who is on call")
    assert "the on-call is Ada" in host_turn.body


# ================================================================ 3. §5.4 — THE ETAG GATE
async def test_an_unchanged_block_is_not_re_injected_when_the_caller_knows_its_etag(
    started_host: LocalMemoryHost,
) -> None:
    """§5.4 `:195`: "The host hook **skips re-inject when the etag is unchanged**... no re-inject
    churns the host's KV-cache." The etag comes back on the wire today and is discarded
    (``capture/hook.py:204``); this is the daemon-side half of the gate."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    bridge = RecallInjectBridge(started_host, settings=InjectSettings())
    recall.return_value = _listing(_item("the on-call is Ada"))

    first = await bridge.render(_SESSION, query="who is on call")
    assert first.body and first.etag

    # A fresh session id: same content, nothing in its digest, so the block assembles identically.
    again = await bridge.render("s2", query="who is on call", known_etag=first.etag)
    assert again.etag == first.etag
    assert again.body == "", "an unchanged block was re-injected despite a matching etag"


async def test_a_changed_block_is_injected_even_when_an_etag_was_offered(
    started_host: LocalMemoryHost,
) -> None:
    """The gate must not become a mute button: a stale etag from the caller cannot suppress a
    block that genuinely changed."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    bridge = RecallInjectBridge(started_host, settings=InjectSettings())
    recall.return_value = _listing(_item("the on-call is Ada"))
    first = await bridge.render(_SESSION, query="who is on call")

    recall.return_value = _listing(_item("the on-call is Grace"))
    changed = await bridge.render("s2", query="who is on call", known_etag=first.etag)
    assert changed.etag != first.etag
    assert "Grace" in changed.body


# ==================================================== 4. §5.2 — PER-SECTION SUB-BUDGETS
async def test_a_long_persona_cannot_starve_the_recalled_facts(
    client_config: ClientSettings,
) -> None:
    """§5.2 `:185`: sub-budgets exist so "a long persona cannot starve facts and a recall flood
    cannot evict the floor". Under ONE pooled ceiling a 10k-char persona emitted first leaves
    nothing for the answer-bearing band, and the block still looks full."""
    settings = LiveContextSettings()
    state = _state(
        client_config,
        alice=PrivateSlice(
            persona_brief="P" * 5_000,
            recalled=(
                _slab(
                    "the on-call is Ada",
                    visibility=Visibility.PRIVATE,
                    section=Section.RECALLED_MEMORY,
                ),
            ),
        ),
    )
    assembled = await assemble(
        state, principal_id="alice", prompt="who is on call", budget_chars=1_000, settings=settings
    )
    persona_budget = settings.budgets.allocate(1_000)[Section.PERSONA]
    assert assembled.body.count("P") <= persona_budget
    assert "the on-call is Ada" in assembled.body, "a long persona starved the answer-bearing band"


async def test_no_section_exceeds_its_own_sub_budget(client_config: ClientSettings) -> None:
    """The sub-budget is a CEILING, not a hint. A section that overruns its allocation is exactly
    the pooled-ceiling failure the split exists to remove, wearing the split's clothes."""
    settings = LiveContextSettings()
    total = 1_200
    facts = tuple(
        _slab(
            f"salient fact number {i} " + "x" * 60,
            visibility=Visibility.PRIVATE,
            section=Section.RECALLED_MEMORY,
            salience=1.0 - i / 100,
        )
        for i in range(40)
    )
    assembled = await assemble(
        _state(client_config, alice=PrivateSlice(recalled=facts)),
        principal_id="alice",
        prompt="salient",
        budget_chars=total,
        settings=settings,
    )
    section_budget = settings.budgets.allocate(total)[Section.RECALLED_MEMORY]
    body_lines = [
        line for line in assembled.body.splitlines() if line.startswith("- salient fact number")
    ]
    # The top fact is emitted unconditionally (it is "the top fact", never trimmed); every
    # further line must fit inside the section's own allocation.
    assert sum(len(line) for line in body_lines[1:]) <= section_budget
    assert assembled.trimmed is True


async def test_the_recency_floor_is_never_trimmed(client_config: ClientSettings) -> None:
    """§5.2 `:185`: "never the top fact, never an ``is_floor=True`` slab" (recall §1.3/§2.3). The
    floor is the verbatim record of what was just said; trimming it silently rewrites the model's
    own recent history.

    Stated as the PRIORITY it actually is: the floor overdraws its own 25% sub-budget without
    limit and outlives every other section — here it is up against a persona, a summary and ten
    recalled facts, and it is the only thing left standing."""
    floor = tuple(
        _slab(f"turn {i}: " + "f" * 200, visibility=Visibility.PRIVATE, section=Section.RECENT)
        for i in range(6)
    )
    competitors = tuple(
        _slab(
            f"rival fact {i} " + "r" * 200,
            visibility=Visibility.PRIVATE,
            section=Section.RECALLED_MEMORY,
        )
        for i in range(10)
    )
    assembled = await assemble(
        _state(
            client_config,
            alice=PrivateSlice(persona_brief="a" * 400, recency_floor=floor, recalled=competitors),
        ),
        principal_id="alice",
        prompt=None,
        budget_chars=1500,  # holds the floor, nothing like all of it plus the rest
        settings=LiveContextSettings(),
    )
    for slab in floor:
        assert slab.text in assembled.body, "an is_floor slab was trimmed to fit a sub-budget"
    assert "rival fact" not in assembled.body, "the floor gave way before the recalled band did"


async def test_the_floor_yields_to_the_pooled_ceiling_only_after_everything_else_and_says_so(
    client_config: ClientSettings,
) -> None:
    """The half §5.2 leaves undecided, decided in code rather than downstream.

    "Never trimmed" and "per-section ceiling" cannot both be absolute: the host's window is a fixed
    size, and a floor larger than the whole window cannot be delivered whole by anyone. Previously
    the assembler resolved that by ignoring it — 20 floor slabs against a 400-char ceiling
    rendered 4363 chars and reported ``trimmed=False`` — and the bridge then byte-sliced the block
    to length, which cut mid-line, dropped the closing tags, and recorded the cut facts as
    delivered. The rule now: the floor is LAST to be sacrificed, the block always fits, the cut is
    whole lines, and the omission is named in the block itself."""
    floor = tuple(
        _slab(f"turn {i}: " + "f" * 200, visibility=Visibility.PRIVATE, section=Section.RECENT)
        for i in range(20)
    )
    assembled = await assemble(
        _state(client_config, alice=PrivateSlice(recency_floor=floor)),
        principal_id="alice",
        prompt=None,
        budget_chars=400,  # <- far below what the floor needs
        settings=LiveContextSettings(),
    )
    assert len(assembled.body) <= 400, "the pooled inject ceiling was exceeded"
    assert assembled.trimmed is True
    assert "item(s) omitted" in assembled.body, "content was dropped with nothing saying so"
    assert assembled.body.rstrip().endswith("</memory_context>"), "the §4 XML was left unclosed"
    delivered = [line for line in assembled.body.splitlines() if line.startswith("- turn ")]
    for line in delivered:
        assert content_hash_of(line[2:]) in assembled.emitted_hashes
    assert len(assembled.emitted_hashes) == len(
        delivered
    ), "a hash was recorded as injected for a line the host never received"


async def test_the_top_fact_is_never_the_one_trimmed(client_config: ClientSettings) -> None:
    """§5.2: "Trim lowest-salience from the middle band of ``<recalled_memory>`` first; never the
    top fact"."""
    # Deliberately LONGER than the section's own allocation (500 * 0.40 = 200 chars): if the top
    # fact were merely small it would survive any budget rule at all, and the test would prove
    # nothing about "never the top fact".
    top = _slab(
        "THE ANSWER: deploys go to staging-eu " + "t" * 400,
        visibility=Visibility.PRIVATE,
        section=Section.RECALLED_MEMORY,
        salience=9.0,
    )
    middle = tuple(
        _slab(
            f"low-salience filler {i} " + "z" * 300,
            visibility=Visibility.PRIVATE,
            section=Section.RECALLED_MEMORY,
            salience=0.1,
        )
        for i in range(10)
    )
    assembled = await assemble(
        _state(client_config, alice=PrivateSlice(recalled=(top, *middle))),
        principal_id="alice",
        prompt="where do deploys go",
        # Above the pooled ceiling the whole block needs (~511 chars rendered) and far below the
        # section's own allocation for the top fact (600 * 0.40 = 240 < 437). The section overdraw
        # is what this test is about; the pooled ceiling is a separate bound with its own test
        # above, and a budget that cannot hold even the top fact is a misconfiguration, not a
        # trimming rule (§5.2).
        budget_chars=600,
        settings=LiveContextSettings(),
    )
    assert "THE ANSWER: deploys go to staging-eu" in assembled.body
    assert len(assembled.body) <= 600


def test_the_section_fractions_must_sum_to_one() -> None:
    """A split that does not sum to 1 either leaves budget unspendable or lets the sub-budgets
    exceed the pooled ceiling they exist to subdivide."""
    with pytest.raises(ValueError, match="must sum to 1.0"):
        SectionBudgets(persona=0.9, session_state=0.9)


# ============================== 5. §6 — POINTERS IN STATE, BODIES BY ID, NAMED NON-HYDRATION
async def test_a_reference_hit_is_a_pointer_slab_not_an_inlined_body() -> None:
    """§6 `:214-216`: "``ContextSlab.text=None`` + ``artifact_ref`` set = a **pointer slab**: it
    occupies the state as a stub-plus-id, and its body is hydrated **by id at render time under
    budget**." ``artifact_ref`` reaches the surface DTO today and the inject path has never read
    it."""
    slab = slab_from_recall_item(
        _item("", mid="m1", artifact_ref="art-42"), visibility=Visibility.PRIVATE
    )
    assert slab.is_pointer is True
    assert slab.text is None
    assert slab.section is Section.REFERENCES
    assert slab.artifact_ref == "art-42"


async def test_a_budget_forbidden_hydration_emits_a_named_marker_not_a_silent_drop(
    client_config: ClientSettings,
) -> None:
    """CANONICAL-CONTRACTS.md:640 (gap **G5**, not G6 — G6 is reference-aware artifact retention):
    "Recall hydrates the artifact for ``kind=reference`` hits — the render-time, budgeted, by-id
    hydration **with a named non-hydration marker**".

    **A silent drop is indistinguishable from "there was nothing to hydrate."** Both the inline
    marker (which the MODEL sees, so it can ask for the body via the MCP recall tool) and the
    ``DegradedModeEntered`` event (which the OPERATOR sees) are required — either alone leaves one
    of the two audiences unable to tell the difference."""

    class _Hydrator:
        async def get_blob(self, ns: Namespace, artifact_id: str) -> bytes | None:
            return b"A" * 5_000  # far more than any references sub-budget here

    pointer = ContextSlab(
        slab_id="m1",
        content_hash="h1",
        text=None,
        artifact_ref="art-42",
        section=Section.REFERENCES,
        visibility=Visibility.PRIVATE,
    )
    degrades: list[tuple[str, str]] = []
    # budget_chars=10_000 => references sub-budget 400 chars: comfortably above
    # ``reference_min_chars`` (so the read IS attempted) and far below the 5_000-byte body (so the
    # BUDGET branch is what refuses it, not the "too small to bother" floor below).
    assembled = await assemble(
        _state(client_config, alice=PrivateSlice(recalled=(pointer,))),
        principal_id="alice",
        prompt="show me the doc",
        budget_chars=10_000,
        settings=LiveContextSettings(),
        hydrator=_Hydrator(),
        degrade=lambda mode, detail: degrades.append((mode, detail)),
    )
    assert "[reference art-42 not expanded — inject budget]" in assembled.body
    assert [mode for mode, _ in degrades] == ["artifact_hydration_skipped"]
    assert assembled.trimmed is True


async def test_a_sub_budget_too_small_to_hold_any_body_still_emits_the_marker(
    client_config: ClientSettings,
) -> None:
    """The other refusal path: when the ``<references>`` allocation cannot hold a meaningful body
    at all, §6 emits the marker WITHOUT spending a store read to discover it. It must be the same
    named outcome — an operator cannot be left guessing which of two silences they see."""

    class _Hydrator:
        def __init__(self) -> None:
            self.reads = 0

        async def get_blob(self, ns: Namespace, artifact_id: str) -> bytes | None:
            self.reads += 1
            return b"short"

    pointer = ContextSlab(
        slab_id="m1",
        content_hash="h1",
        text=None,
        artifact_ref="art-42",
        section=Section.REFERENCES,
        visibility=Visibility.PRIVATE,
    )
    hydrator = _Hydrator()
    degrades: list[tuple[str, str]] = []
    assembled = await assemble(
        _state(client_config, alice=PrivateSlice(recalled=(pointer,))),
        principal_id="alice",
        prompt="show me the doc",
        budget_chars=200,  # references sub-budget = 8 chars, below reference_min_chars
        settings=LiveContextSettings(),
        hydrator=hydrator,
        degrade=lambda mode, detail: degrades.append((mode, detail)),
    )
    assert "[reference art-42 not expanded — inject budget]" in assembled.body
    assert [mode for mode, _ in degrades] == ["artifact_hydration_skipped"]
    assert hydrator.reads == 0, "a store read was spent on a body that could never fit"


async def test_a_reference_that_fits_is_hydrated_by_id(client_config: ClientSettings) -> None:
    """The other half of §6: within budget, the body IS fetched by id and rendered — the pointer
    is not a permanent excuse not to hydrate."""

    class _Hydrator:
        async def get_blob(self, ns: Namespace, artifact_id: str) -> bytes | None:
            assert artifact_id == "art-42"
            return b"the ADR chose FalkorDB over Neo4j"

    pointer = ContextSlab(
        slab_id="m1",
        content_hash="h1",
        text=None,
        artifact_ref="art-42",
        section=Section.REFERENCES,
        visibility=Visibility.PRIVATE,
    )
    assembled = await assemble(
        _state(client_config, alice=PrivateSlice(recalled=(pointer,))),
        principal_id="alice",
        prompt="which graph store",
        budget_chars=10_000,
        settings=LiveContextSettings(),
        hydrator=_Hydrator(),
    )
    assert "the ADR chose FalkorDB over Neo4j" in assembled.body
    assert assembled.trimmed is False


async def test_an_unwired_hydrator_is_a_named_absence_never_a_silent_one(
    client_config: ClientSettings,
) -> None:
    """No accessor exposes the container's ``ContextRepository`` to the daemon yet, so this is the
    SHIPPED path today. It must be visible as such — "absence is the house rule" means a named
    absence, not an empty ``<references>``."""
    pointer = ContextSlab(
        slab_id="m1",
        content_hash="h1",
        text=None,
        artifact_ref="art-42",
        section=Section.REFERENCES,
        visibility=Visibility.PRIVATE,
    )
    degrades: list[tuple[str, str]] = []
    assembled = await assemble(
        _state(client_config, alice=PrivateSlice(recalled=(pointer,))),
        principal_id="alice",
        prompt="show me the doc",
        budget_chars=10_000,
        settings=LiveContextSettings(),
        hydrator=None,
        degrade=lambda mode, detail: degrades.append((mode, detail)),
    )
    assert "[reference art-42 not expanded — no artifact store wired]" in assembled.body
    assert degrades[0][0] == "artifact_hydration_unavailable"


async def test_the_non_hydration_marker_never_carries_slab_text(
    client_config: ClientSettings,
) -> None:
    """CLAUDE.md rule 3 on the degrade path: the ``detail`` an operator reads is sizes and ids,
    never the artifact body it failed to render."""

    class _Hydrator:
        async def get_blob(self, ns: Namespace, artifact_id: str) -> bytes | None:
            return b"the db password is hunter2 " + b"z" * 5_000

    pointer = ContextSlab(
        slab_id="m1",
        content_hash="h1",
        text=None,
        artifact_ref="art-42",
        section=Section.REFERENCES,
        visibility=Visibility.PRIVATE,
    )
    degrades: list[tuple[str, str]] = []
    await assemble(
        _state(client_config, alice=PrivateSlice(recalled=(pointer,))),
        principal_id="alice",
        prompt="x",
        budget_chars=10_000,  # reaches the BUDGET refusal after a real read (see above)
        settings=LiveContextSettings(),
        hydrator=_Hydrator(),
        degrade=lambda mode, detail: degrades.append((mode, detail)),
    )
    assert degrades and "hunter2" not in repr(degrades)


# ======================================================== 6. §5.1 — PROMPT-AWARE SELECTION
async def test_the_prompt_deprioritises_slabs_it_does_not_touch(
    client_config: ClientSettings,
) -> None:
    """§5.1 `:182`: "the assembler additionally uses the prompt as the **relevance gate for which
    slabs earn a slot**: a slab whose channel/tier the current prompt does not touch is
    deprioritized before budget trimming... the block is assembled *for this prompt*, not a static
    dump." Equal salience, so ONLY the prompt can separate them."""
    relevant = _slab(
        "the postgres connection pool is 20",
        visibility=Visibility.PRIVATE,
        section=Section.RECALLED_MEMORY,
        salience=1.0,
    )
    irrelevant = tuple(
        _slab(
            f"unrelated trivia {i} " + "q" * 200,
            visibility=Visibility.PRIVATE,
            section=Section.RECALLED_MEMORY,
            salience=1.0,
        )
        for i in range(6)
    )
    assembled = await assemble(
        _state(client_config, alice=PrivateSlice(recalled=(*irrelevant, relevant))),
        principal_id="alice",
        prompt="what is the postgres connection pool size",
        budget_chars=600,
        settings=LiveContextSettings(),
    )
    assert "the postgres connection pool is 20" in assembled.body


def test_a_prompt_that_matches_nothing_degrades_to_ranking_order() -> None:
    """The gate is a deprioritization, never a drop (`:182`). A query-less re-warm or an opaque
    prompt must not silently empty the block, nor reshuffle it arbitrarily."""
    assert prompt_tokens(None) == frozenset()
    assert prompt_tokens("a to the of") == frozenset(), "stopword-length tokens manufacture overlap"
    assert "postgres" in prompt_tokens("What is the Postgres pool?")


# ======================================================= 7. §4 — RECALL UPDATES, NOT REPLACES
def test_recall_writes_the_recalled_zone_and_preserves_everything_else(
    client_config: ClientSettings,
) -> None:
    """§4 `:155`: recall "writes the result into the ``recalled`` slot... **preserving** the
    persona brief, recency floor, reasoning register, tool state, and injected digest that are
    already there... recall is one writer among several into a persistent structured state, not a
    from-scratch recompute of the whole block."

    If recall replaced the slice, ``injected_digest`` would be wiped every turn and §5.3 would be
    dead code that still passed its own unit test."""
    reasoning = _slab(
        "decided FalkorDB over Neo4j", visibility=Visibility.PRIVATE, section=Section.REASONING
    )
    before = PrivateSlice(
        persona_brief="terse, prefers Python",
        reasoning_register=(reasoning,),
    ).with_injected(("old-hash",), bound=64)
    state = _state(client_config, alice=before)

    fresh = [slab_from_recall_item(_item("the on-call is Ada"), visibility=Visibility.PRIVATE)]
    after = update_recalled(
        state, principal_id="alice", slabs=fresh, prompt_hash="ph", now=_NOW
    ).slice_for("alice")

    assert [s.text for s in after.recalled] == ["the on-call is Ada"]
    assert after.persona_brief == "terse, prefers Python"
    assert after.reasoning_register == (reasoning,)
    assert after.injected_digest == ("old-hash",)


async def test_a_demoted_memory_loses_its_digest_entry_over_the_real_bus(
    started_host: LocalMemoryHost, client_config: ClientSettings
) -> None:
    """§5.5 row 2 (`:201`) end to end: a demoted fact's ``injected_digest`` entry is cleared, so
    when it is promoted back it can be injected again. Without the clear it would be suppressed
    for the rest of the session — present in the tiers, absent from every block, and invisible."""
    from mu_contracts.domain.events import MemoryDemoted
    from mu_engine.platform.adapters.bus_inproc import InprocBus

    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    bus = InprocBus()
    bridge = RecallInjectBridge(started_host, settings=InjectSettings(), bus=bus)
    ns = _ns(client_config)

    recall.return_value = _listing(_item("the on-call is Ada", mid="m0"))
    first = await bridge.render(_SESSION, query="who is on call")
    assert "the on-call is Ada" in first.body
    # Turn 2 would normally dedup it away — proving the digest holds it.
    assert (await bridge.render(_SESSION, query="who is on call")).body == ""

    await bus.publish(MemoryDemoted(namespace=ns, id="m0", tier=Tier.MTM, retention=0.1))
    await bridge.drain_refreshes()

    reinjected = await bridge.render(_SESSION, query="who is on call")
    assert (
        "the on-call is Ada" in reinjected.body
    ), "a demoted-then-recalled fact stayed suppressed — its digest entry outlived it"
    await bridge.aclose()


# ============================================================ 8. §4 — THE FORMAT INVARIANT
async def test_the_block_is_ordered_named_xml_with_no_volatile_field_at_the_top(
    client_config: ClientSettings,
) -> None:
    """§4 `:161-173` / CANONICAL §7.22. Fixed section sequence; the recency floor at the bottom
    edge; **no timestamp or volatile id at the top** — one there churns the etag every turn and
    destroys the KV-cache reuse §5.4's whole gate exists to buy (F6)."""
    state = _state(
        client_config,
        alice=PrivateSlice(
            persona_brief="terse",
            recalled=(
                _slab(
                    "the on-call is Ada",
                    visibility=Visibility.PRIVATE,
                    section=Section.RECALLED_MEMORY,
                ),
            ),
            recency_floor=(
                _slab(
                    "user asked about on-call",
                    visibility=Visibility.PRIVATE,
                    section=Section.RECENT,
                ),
            ),
        ),
    )
    body = (
        await assemble(
            state,
            principal_id="alice",
            prompt="on-call",
            budget_chars=10_000,
            settings=LiveContextSettings(),
        )
    ).body
    assert body.startswith("<memory_context>\n<persona>")
    assert body.endswith("</memory_context>")
    assert body.index("<persona>") < body.index("<recalled_memory>") < body.index("<recent>")
    assert str(_NOW.year) not in body.splitlines()[1]


async def test_an_empty_section_is_omitted_rather_than_rendered_hollow(
    client_config: ClientSettings,
) -> None:
    """Absence is the house rule. An empty ``<persona></persona>`` spends the host's tokens to say
    nothing, every turn, forever."""
    state = _state(
        client_config,
        alice=PrivateSlice(
            recalled=(
                _slab(
                    "the on-call is Ada",
                    visibility=Visibility.PRIVATE,
                    section=Section.RECALLED_MEMORY,
                ),
            )
        ),
    )
    body = (
        await assemble(
            state,
            principal_id="alice",
            prompt="on-call",
            budget_chars=10_000,
            settings=LiveContextSettings(),
        )
    ).body
    assert "<persona>" not in body and "<references>" not in body and "<reasoning>" not in body


async def test_trimming_spills_the_full_block_rather_than_dropping_it_silently(
    started_host: LocalMemoryHost, tmp_path: Path
) -> None:
    """Per-section sub-budgets replaced blind tail-truncation as the way a too-large block is cut
    down — but capture-spec §7.2's "named degrade, NEVER a silent truncate" is the invariant, not
    the truncation mechanism. Whatever the sub-budgets left out is written to the SAME F4 spill
    file the over-length path always used, and the block points at it."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    bridge = RecallInjectBridge(
        started_host,
        settings=InjectSettings(body_budget_chars=600),
        recall_dir=tmp_path / "recall",
    )
    recall.return_value = _listing(
        *(_item(f"distinct salient fact {i} " + "x" * 200) for i in range(40))
    )
    rendered = await bridge.render(_SESSION, query="salient facts")
    assert len(rendered.body) <= 600
    assert "spilled to" in rendered.body
    spills = list((tmp_path / "recall").glob("*.txt"))
    assert len(spills) == 1
    spilled = spills[0].read_text(encoding="utf-8")
    assert "distinct salient fact 39" in spilled, "a trimmed slab was dropped instead of spilled"


# ============================================ 9. TENANCY OF THE LIVE STATE (CLAUDE.md rule 4)
async def test_two_principals_sharing_a_session_id_get_independent_live_state(
    started_host: LocalMemoryHost,
) -> None:
    """The live state is keyed on the FULL six-slot ``to_prefix()``, never the host-supplied
    session id — the same rule the render cache already enforces (CANONICAL §1 rule 5).

    Session-keying it looks harmless because ``slice_for(principal)`` still separates the two
    principals' slabs. It is not: the SHARED object carries ONE ``namespace``, so whichever
    principal created it decides the η every later §6 hydration reads under — and the second
    principal's reference body is then fetched from the FIRST principal's partition. A
    cross-tenant store read, reached through a correct-looking per-principal render.

    (The digests do NOT merge on session-keying alone — ``slice_for`` still separates them by
    principal — which is exactly what makes this defect quiet: the visible per-principal behaviour
    stays correct while the store reads cross the tenant boundary. The companion test below covers
    the digest separation on its own axis.)"""
    read_under: list[Namespace] = []

    class _Hydrator:
        async def get_blob(self, ns: Namespace, artifact_id: str) -> bytes | None:
            read_under.append(ns)
            return b"the ADR body"

    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    bridge = RecallInjectBridge(started_host, settings=InjectSettings(), hydrator=_Hydrator())
    recall.return_value = _listing(_item("see the ADR doc", mid="m1", artifact_ref="art-1"))

    await bridge.render(_SESSION, user="alice", query="which graph store")
    await bridge.render(_SESSION, user="bob", query="which graph store")

    assert [ns.user for ns in read_under] == ["alice", "bob"], (
        "a reference body was hydrated under another principal's namespace — the live state was "
        "keyed on the bare session id"
    )


async def test_one_principals_digest_never_suppresses_anothers_facts(
    started_host: LocalMemoryHost,
) -> None:
    """§5.3's digest is per ``(principal, session)`` (§1 `:101` "keyed by ``principal_id``"), and
    it has to be: a digest shared across principals makes alice's inject silently blank bob's
    first turn — bob sees an empty block and nothing anywhere says why. Two things must BOTH hold
    for that separation, and this test fails if either is dropped: the live state is keyed on the
    full η, and the slice inside it is keyed on the reading principal."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    bridge = RecallInjectBridge(started_host, settings=InjectSettings())
    recall.return_value = _listing(_item("the on-call is Ada", mid="m1"))

    alice = await bridge.render(_SESSION, user="alice", query="who is on call")
    bob = await bridge.render(_SESSION, user="bob", query="who is on call")

    assert "the on-call is Ada" in alice.body
    assert "the on-call is Ada" in bob.body, "alice's injected_digest blanked bob's first turn"


# ====================== 10. THE DIGEST RECORDS WHAT REACHED THE HOST, NOTHING ELSE (§5.3)
async def test_a_slab_trimmed_by_budget_is_not_recorded_as_injected(
    client_config: ClientSettings,
) -> None:
    """§5.3 unions the EMITTED hashes into ``injected_digest``. Trimming skips from the MIDDLE, so
    "the first N slabs" is not "the slabs that were emitted" — inferring the emitted set from the
    line count marks trimmed slabs as already-injected.

    The consequence is the worst failure available to this mechanism: a fact that never reached
    the host is suppressed for the rest of the session. It is in the tiers, it ranks, and it is
    absent from every block — and because the suppression IS the lean path working, no degrade
    fires and no log says anything."""
    # The trimmed slab must sit in the MIDDLE of the emitted order, not at the end: §5.2 trims
    # "from the middle band ... first", and an emitted set inferred as "the first N" happens to be
    # right whenever the casualty is last. Salience alone orders these (no prompt overlap), so the
    # order is top -> trimmed_away -> tail, and only the middle one falls out.
    top = _slab(
        "highest salience fact",
        visibility=Visibility.PRIVATE,
        section=Section.RECALLED_MEMORY,
        salience=9.0,
    )
    trimmed_away = _slab(
        "the trimmed fact " + "b" * 400,
        visibility=Visibility.PRIVATE,
        section=Section.RECALLED_MEMORY,
        salience=5.0,
    )
    tail = _slab(
        "lowest salience fact",
        visibility=Visibility.PRIVATE,
        section=Section.RECALLED_MEMORY,
        salience=1.0,
    )
    assembled = await assemble(
        _state(client_config, alice=PrivateSlice(recalled=(top, trimmed_away, tail))),
        principal_id="alice",
        prompt="zzz nothing matches",
        budget_chars=500,
        settings=LiveContextSettings(),
    )
    assert assembled.trimmed is True
    assert "the trimmed fact" not in assembled.body
    assert (
        trimmed_away.content_hash not in assembled.emitted_hashes
    ), "a slab that was trimmed away was recorded as injected — it can never be sent again"
    assert top.content_hash in assembled.emitted_hashes
    assert tail.content_hash in assembled.emitted_hashes


async def test_a_reference_that_rendered_only_a_marker_is_not_recorded_as_injected(
    client_config: ClientSettings,
) -> None:
    """Same rule on the §6 path. A marker is the NAME of a body, not the body: recording its hash
    as injected makes the artifact permanently un-hydratable for the session, so the one turn
    where the budget was tight silently costs the reference forever."""
    pointer = ContextSlab(
        slab_id="m1",
        content_hash="h-art",
        text=None,
        artifact_ref="art-42",
        section=Section.REFERENCES,
        visibility=Visibility.PRIVATE,
    )
    assembled = await assemble(
        _state(client_config, alice=PrivateSlice(recalled=(pointer,))),
        principal_id="alice",
        prompt="show me the doc",
        budget_chars=10_000,
        settings=LiveContextSettings(),
        hydrator=None,  # the shipped state: no artifact store wired
    )
    assert "[reference art-42 not expanded" in assembled.body
    assert (
        "h-art" not in assembled.emitted_hashes
    ), "a non-hydrated reference was recorded as injected — its body can never be sent"


# ========== 8. THE DELTA MUST DESCRIBE WHAT WAS DELIVERED (the class that keeps recurring) ======
async def test_a_fact_cut_by_the_pooled_ceiling_is_not_recorded_as_injected(
    started_host: LocalMemoryHost, tmp_path: Path
) -> None:
    """The whole-system version of the invariant, through the REAL bridge.

    §5.3's digest is a claim that the host already has a fact. A fact that was assembled, then cut
    to fit the ceiling, then recorded as injected is suppressed for the rest of the session: it is
    in the tiers, it ranks, it never appears in a block again, and ``HostInjectionSkipped
    (reason="no_delta")`` affirmatively states the host already has it. Six 310-char floor facts at
    a 600-char ceiling used to deliver two and record six."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    bridge = RecallInjectBridge(
        started_host, settings=InjectSettings(body_budget_chars=600), recall_dir=tmp_path
    )
    facts = [_item(f"FLOORFACT{i} " + "y" * 300, mid=f"m{i}", is_floor=True) for i in range(6)]
    recall.return_value = _listing(*facts)

    first = await bridge.render(_SESSION, query="floorfact")
    delivered = {i for i in range(6) if f"FLOORFACT{i}" in first.body}
    assert delivered, "nothing was delivered at all"
    assert len(delivered) < 6, "the fixture no longer exceeds the ceiling; the test proves nothing"

    # Every fact the ceiling cut is still owed to the host, so successive turns keep paying it
    # down until there is genuinely nothing left to say.
    for _ in range(12):
        nxt = await bridge.render(_SESSION, query="floorfact")
        if not nxt.body:
            break
        delivered |= {i for i in range(6) if f"FLOORFACT{i}" in nxt.body}
    assert delivered == set(range(6)), (
        f"facts {sorted(set(range(6)) - delivered)} never reached the host and were deduped away "
        "as already-injected"
    )


async def test_an_over_ceiling_block_is_still_well_formed_xml(
    started_host: LocalMemoryHost, tmp_path: Path
) -> None:
    """§4 / CANONICAL §7.22's ordered-XML FORMAT invariant, on the path that used to break it.

    The body was cut to length with ``body[: budget - len(note)]``, which severed the last line
    mid-word and took ``</recent>`` and ``</memory_context>`` with it — the model received
    unterminated markup on every over-budget render. Harmless when the body was flat bullets; not
    harmless now that §4 made it structured."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    bridge = RecallInjectBridge(
        started_host, settings=InjectSettings(body_budget_chars=600), recall_dir=tmp_path
    )
    recall.return_value = _listing(
        *(_item(f"fact {i} " + "y" * 300, mid=f"m{i}", is_floor=True) for i in range(6))
    )
    rendered = await bridge.render(_SESSION, query="fact")

    body = rendered.body.split("… (full context spilled")[0].rstrip()
    assert body.startswith("<memory_context>")
    assert body.endswith("</memory_context>")
    assert body.count("<recent>") == body.count("</recent>") == 1
    assert len(rendered.body) <= 600, "the pooled ceiling did not bound the delivered body"


# ============ 9. THE WARM READ-MODEL IS A SNAPSHOT, THE HOOK CHANNEL IS A DELTA ================
async def test_the_warm_read_stays_a_full_snapshot_across_repeated_renders(
    started_host: LocalMemoryHost, tmp_path: Path
) -> None:
    """§5.3's delta belongs to the ACCUMULATING channel only.

    ``last_rendered`` is what ``MemoryLifecycleManager.ready_context`` and ``/ready-context`` read,
    and those REPLACE their content on every read — they hold nothing between reads. Caching the
    delta made the second read of a session empty and every read after it emptier, while
    ``ready_context`` reported ``wired=True``: an affirmative claim that the warm cache is live and
    has nothing to say. The hook channel must still go lean, so both are asserted here."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    bridge = RecallInjectBridge(started_host, settings=InjectSettings(), recall_dir=tmp_path)
    recall.return_value = _listing(_item("the deploy target is staging-eu"))

    first = await bridge.render(_SESSION)
    assert "staging-eu" in first.body
    warm_one = bridge.last_rendered(_SESSION)
    assert warm_one is not None and "staging-eu" in warm_one

    second = await bridge.render(_SESSION)
    assert second.body == "", "the accumulating host channel re-sent what the host already had"
    warm_two = bridge.last_rendered(_SESSION)
    assert warm_two == warm_one, "the replacing warm channel went empty on the second read"
    await bridge.render(_SESSION)
    assert bridge.last_rendered(_SESSION) == warm_one


# ===================== 10. THE PERSONA BRIEF, AND THE HALF OF IT THAT WAS LOST ==================
async def test_a_truncated_persona_records_only_what_it_delivered(
    client_config: ClientSettings,
) -> None:
    """The C-class defect inside ``assemble``: ``content_hash_of(slice_.persona_brief)`` was
    appended to ``emitted`` BEFORE the brief was cut to its 15% sub-budget. The full brief was
    recorded as delivered while a prefix reached the host, so turn two deduped the whole
    ``<persona>`` section away and the remainder was never sent on any turn — a private fact
    present in the store, absent from every block, with no degrade and no log."""
    brief = "P" * 300
    slice_ = PrivateSlice(persona_brief=brief)
    state = _state(client_config, alice=slice_)
    degrades: list[tuple[str, str]] = []

    first = await assemble(
        state,
        principal_id="alice",
        prompt=None,
        budget_chars=1000,
        settings=LiveContextSettings(),
        degrade=lambda mode, detail: degrades.append((mode, detail)),
    )
    assert first.trimmed is True
    assert any(mode == "persona_brief_truncated" for mode, _ in degrades), "a silent truncation"
    assert "persona brief truncated" in first.body, "the model was not told the brief was cut"
    assert (
        content_hash_of(brief) not in first.emitted_hashes
    ), "the FULL brief was recorded as delivered when only a prefix was"
    assert content_hash_of("P" * 150) in first.emitted_hashes

    # A bigger ceiling on a later turn delivers the whole brief — it was never suppressed.
    wider = await assemble(
        state.with_slice("alice", slice_.with_injected(first.emitted_hashes, bound=512), now=_NOW),
        principal_id="alice",
        prompt=None,
        budget_chars=4000,
        settings=LiveContextSettings(),
    )
    assert brief in wider.body


# ===================== 11. §5.3 COVERS <session_state> LIKE EVERYTHING ELSE =====================
async def test_the_shared_running_summary_is_not_re_shouted_every_turn(
    client_config: ClientSettings,
) -> None:
    """``state_lines`` were never added to ``seen`` and never appended to ``emitted``, so the
    largest stable block in the state after the persona brief was re-sent verbatim on every single
    turn. §5.3 is "the core lean mechanism"; a section exempt from it is a section that pays the
    cost §5.3 exists to remove — and the persona brief WAS given a hash, so the exemption was an
    inconsistency rather than a decision."""
    state = LiveSessionContext(
        namespace=_ns(client_config),
        session_id=_SESSION,
        shared=SharedZone(
            running_summary="the team is planning the Friday release",
            open_threads=("agent dispatch running",),
        ),
        private={
            "alice": PrivateSlice(
                tool_state=(
                    ToolTurnState(
                        correlation_id="c1", kind="tool_use", label="grep", status="done"
                    ),
                )
            )
        },
        updated_at=_NOW,
    )

    first = await assemble(
        state,
        principal_id="alice",
        prompt=None,
        budget_chars=1000,
        settings=LiveContextSettings(),
    )
    assert "the team is planning the Friday release" in first.body
    assert "agent dispatch running" in first.body
    assert "tool_use grep: done" in first.body
    assert len(first.emitted_hashes) == 3, "session_state lines carried no dedup identity"

    slice_ = state.slice_for("alice").with_injected(first.emitted_hashes, bound=512)
    second = await assemble(
        state.with_slice("alice", slice_, now=_NOW),
        principal_id="alice",
        prompt=None,
        budget_chars=1000,
        settings=LiveContextSettings(),
    )
    assert second.body == "", "the shared summary was re-shouted to a host that already had it"
    assert second.deduped_count == 3
    # …and the REPLACING channel still says everything.
    assert "the team is planning the Friday release" in second.snapshot_body


# ================== 12. THE SPILL IS EXCEPTIONAL, AND IT IS NOT WORLD-READABLE =================
async def test_a_named_non_hydration_does_not_spill_memory_content_to_disk(
    started_host: LocalMemoryHost, tmp_path: Path
) -> None:
    """No hydrator is wired today, so EVERY pointer slab rendered a "not wired" marker and set
    ``trimmed`` — which made the F4 spill fire on the ordinary path of every render carrying one
    reference: real memory content written to disk each time, a filesystem path appended to the
    host block, and an operator told ``mode="body_over_budget_file_spill" chars=162 budget=10000``.
    A named absence is not a trim: no bigger ceiling would have produced that body."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    bridge = RecallInjectBridge(
        started_host, settings=InjectSettings(body_budget_chars=10_000), recall_dir=tmp_path
    )
    recall.return_value = _listing(
        _item("alice's salary is 200000 eur", mid="m1"),
        _item("the design doc", mid="m2", artifact_ref="art-1"),
    )
    rendered = await bridge.render(_SESSION, query="salary")

    assert "not expanded" in rendered.body, "the named non-hydration marker is gone"
    assert "spilled to" not in rendered.body
    assert (
        list(tmp_path.glob("*.txt")) == []
    ), "memory content was written to disk on a routine render"


async def test_the_spill_file_is_not_readable_by_anyone_else(
    started_host: LocalMemoryHost, tmp_path: Path
) -> None:
    """When the spill DOES fire it holds real memory content. Created 0600 inside a 0700 directory,
    by ``os.open`` rather than write-then-chmod — the window between those two is the exposure."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    recall_dir = tmp_path / "recall"
    bridge = RecallInjectBridge(
        started_host, settings=InjectSettings(body_budget_chars=500), recall_dir=recall_dir
    )
    recall.return_value = _listing(
        *(_item(f"distinct salient fact {i} " + "x" * 200, mid=f"m{i}") for i in range(40))
    )
    await bridge.render(_SESSION, query="salient")

    spills = list(recall_dir.glob("*.txt"))
    assert len(spills) == 1
    assert stat.S_IMODE(os.stat(spills[0]).st_mode) == 0o600
    assert stat.S_IMODE(os.stat(recall_dir).st_mode) == 0o700


# ================== 13. THE DIGEST IS A BELIEF ABOUT THE HOST, AND BELIEFS EXPIRE ==============
async def test_a_host_context_reset_makes_the_suppressed_facts_injectable_again(
    started_host: LocalMemoryHost, tmp_path: Path
) -> None:
    """§5.5's ``context_invalidated`` row, at whole-window grain.

    Claude Code compaction preserves ``session_id``, so after a ``PreCompact`` the daemon keeps
    suppressing exactly the facts the model just lost. Without this seam the only recovery is
    ``hot_session_ttl_s`` (1800 s) of idleness or a daemon restart: the lean delta degrades not to
    re-sending everything but to never re-sending anything. REPORTED: the call site
    (``workers/ingest_client.py:93``) is outside this lane's file ownership."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    bridge = RecallInjectBridge(started_host, settings=InjectSettings(), recall_dir=tmp_path)
    recall.return_value = _listing(_item("the deploy target is staging-eu"))

    assert "staging-eu" in (await bridge.render(_SESSION)).body
    assert (await bridge.render(_SESSION)).body == ""

    assert bridge.on_host_context_reset(_SESSION) is True
    assert (
        "staging-eu" in (await bridge.render(_SESSION)).body
    ), "the host's window was rewritten and the daemon kept suppressing what it lost"
    assert bridge.on_host_context_reset("never-rendered") is False


async def test_the_delivered_body_never_exceeds_the_configured_ceiling(
    started_host: LocalMemoryHost, tmp_path: Path
) -> None:
    """``body_budget_chars`` is a bound on what reaches the HOST, notes included.

    The spill note is appended after assembly and is ~90 chars, so the room for it has to be
    reserved BEFORE the ceiling packs the block — otherwise the block fills to the ceiling and the
    note pushes it over, which is exactly what the old byte-slice was there to hide. Sized so the
    ceiling packs tightly (many short facts): with fat lines the leftover slack hides the overrun
    and the test proves nothing."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    bridge = RecallInjectBridge(
        started_host, settings=InjectSettings(body_budget_chars=400), recall_dir=tmp_path
    )
    recall.return_value = _listing(
        *(_item(f"short salient fact {i:02d}", mid=f"m{i}", is_floor=True) for i in range(40))
    )
    rendered = await bridge.render(_SESSION, query="salient")

    assert "spilled to" in rendered.body, "the fixture no longer trims; the test proves nothing"
    assert (
        len(rendered.body) <= 400
    ), f"the host was handed {len(rendered.body)} chars against a 400-char ceiling"
    body = rendered.body.split("… (full context spilled")[0].rstrip()
    assert body.endswith("</memory_context>"), "the block was cut mid-structure"
    for line in body.splitlines():
        assert line.startswith("<") or line.startswith("- "), f"a severed line: {line!r}"
