"""Unit tests for the local-plane MCP tool wrappers + the shared/private guard (Phase 1).

Pure unit scope (mocks permitted, DEV-STANDARDS): a lightweight in-memory ``MemoryVerbs`` stub
drives the SAME wrapper code the real stdio server runs, so we verify the guard boundary, the tier
mapping, the serialisation shape, and the advertised tool set WITHOUT touching real stores. The
real engine + real-store round-trip is the integration test (``tests/integration``) and the VM
acceptance script — this file proves the wiring logic in isolation.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from mu_contracts.contracts.recall import RecallChannels, RecallItemView, RecallResult
from mu_contracts.contracts.views import (
    ConsolidateView,
    ContextView,
    MemoryVerbResult,
    MemoryWriteResult,
)
from mu_contracts.domain.model.memory import Tier
from mu_engine.storage.domain.memory import MemoryTier
from mu_engine.storage.domain.namespace import Namespace, Visibility

from mu_client.mcp import build_server
from mu_client.mcp.guard import SharedPlaneRejectedError, SharedPrivateGuard
from mu_client.mcp.tools import (
    resolve_tier,
    tool_add,
    tool_ask,
    tool_build_context,
    tool_consolidate,
    tool_delete,
    tool_demote,
    tool_get,
    tool_promote,
    tool_recall,
    tool_search,
    tool_update,
)

pytestmark = pytest.mark.unit

_NS = Namespace(
    org="default", workspace="local", user="u1", session="s1", visibility=Visibility.PRIVATE
)


class _StubEngine:
    """A structural ``MemoryVerbs`` — records the calls it received, returns canonical DTOs."""

    def __init__(self) -> None:
        self.add_calls: list[dict[str, object]] = []
        self.recall_calls: list[dict[str, object]] = []
        self.search_calls: list[dict[str, object]] = []
        self.context_calls: list[dict[str, object]] = []
        self.ask_calls: list[dict[str, object]] = []
        self.verb_calls: list[dict[str, object]] = []
        self.get_return_none = False

    def _hit(self) -> RecallItemView:
        return RecallItemView(
            memory_id="mem-123",
            content="Ada lives in Paris",
            tier=Tier.STM,
            channel="stm",
            fused_score=0.9,
        )

    async def add(
        self,
        content: str,
        *,
        user: str = "default",
        session: str | None = None,
        importance_score: float | None = None,
    ) -> MemoryWriteResult:
        self.add_calls.append(
            {"content": content, "user": user, "session": session, "imp": importance_score}
        )
        return MemoryWriteResult(
            memory_id="mem-123",
            content_hash="hash-abc",
            promoted=True,
            tiers_written=("stm", "mtm"),
            namespace=_NS.to_prefix(),
            events_emitted=("MemoryCaptured",),
        )

    async def recall(
        self,
        query: str,
        *,
        user: str = "default",
        session: str | None = None,
        tier: MemoryTier | None = None,
        limit: int = 10,
    ) -> RecallResult:
        self.recall_calls.append({"query": query, "tier": tier, "limit": limit})
        return RecallResult(
            namespace=_NS,
            items=[
                RecallItemView(
                    memory_id="mem-123",
                    content="Ada lives in Paris",
                    tier=Tier.STM,
                    channel="stm",
                    fused_score=0.9,
                )
            ],
            channels_run=RecallChannels(),
            generated_at=datetime.now(UTC),
        )

    async def get(
        self, memory_id: str, *, user: str = "default", session: str | None = None
    ) -> None:
        # Unit scope covers the absent path (found=False); the found=True hydration is proven by the
        # real-store round-trip (a full MemoryResponse only exists for a genuinely stored row).
        return None

    async def consolidate(
        self, *, user: str = "default", session: str | None = None, limit: int = 20
    ) -> ConsolidateView:
        return ConsolidateView(facts_extracted=3, added=2, superseded=1, noop=0)

    async def search(
        self,
        query: str,
        *,
        user: str = "default",
        session: str | None = None,
        tier: MemoryTier | None = None,
        limit: int = 10,
    ) -> RecallResult:
        self.search_calls.append({"query": query, "tier": tier, "limit": limit})
        return RecallResult(
            namespace=_NS,
            items=[self._hit()],
            channels_run=RecallChannels(),
            generated_at=datetime.now(UTC),
        )

    async def context(
        self,
        query: str,
        *,
        user: str = "default",
        session: str | None = None,
        limit: int = 10,
        max_chars: int | None = None,
    ) -> ContextView:
        self.context_calls.append({"query": query, "limit": limit, "max_chars": max_chars})
        return ContextView(text="- Ada lives in Paris", items=[self._hit()], degraded=None)

    async def ask(
        self,
        question: str,
        *,
        user: str = "default",
        session: str | None = None,
        limit: int = 10,
    ) -> str:
        self.ask_calls.append({"question": question, "limit": limit})
        return "Ada lives in Paris."

    async def promote(
        self,
        memory_id: str,
        *,
        to_tier: str,
        user: str = "default",
        session: str | None = None,
    ) -> MemoryVerbResult:
        self.verb_calls.append({"verb": "promote", "memory_id": memory_id, "to_tier": to_tier})
        return MemoryVerbResult(
            memory_id=memory_id,
            verb="promote",
            from_tier="stm",
            to_tier=to_tier,
            tiers_affected=(to_tier,),
        )

    async def demote(
        self,
        memory_id: str,
        *,
        to_tier: str = "stm",
        user: str = "default",
        session: str | None = None,
    ) -> MemoryVerbResult:
        self.verb_calls.append({"verb": "demote", "memory_id": memory_id, "to_tier": to_tier})
        return MemoryVerbResult(
            memory_id=memory_id,
            verb="demote",
            from_tier="mtm",
            to_tier=to_tier,
            tiers_affected=(to_tier, "mtm"),
        )

    async def update(
        self,
        memory_id: str,
        new_content: str,
        *,
        user: str = "default",
        session: str | None = None,
    ) -> MemoryVerbResult:
        self.verb_calls.append(
            {"verb": "update", "memory_id": memory_id, "new_content": new_content}
        )
        return MemoryVerbResult(
            memory_id="mem-new",
            verb="update",
            superseded_id=memory_id,
            tiers_affected=("stm", "mtm"),
        )

    async def delete(
        self, memory_id: str, *, user: str = "default", session: str | None = None
    ) -> MemoryVerbResult:
        self.verb_calls.append({"verb": "delete", "memory_id": memory_id})
        return MemoryVerbResult(
            memory_id=memory_id,
            verb="delete",
            tiers_affected=("stm", "mtm", "ltm"),
            invalidated=True,
        )


# --------------------------------------------------------------------------------- guard
def test_guard_passes_when_no_shared_field_supplied() -> None:
    SharedPrivateGuard().assert_private(
        visibility=None, subject=None, predicate=None, object=None
    )  # no raise


@pytest.mark.parametrize("field", ["visibility", "subject", "predicate", "object"])
def test_guard_rejects_any_shared_field(field: str) -> None:
    with pytest.raises(SharedPlaneRejectedError) as exc:
        SharedPrivateGuard().assert_private(**{field: "some-value"})
    assert field in str(exc.value)


def test_guard_names_every_offending_field() -> None:
    with pytest.raises(SharedPlaneRejectedError) as exc:
        SharedPrivateGuard().assert_private(visibility="shared", subject="Ada")
    message = str(exc.value)
    assert "subject" in message and "visibility" in message


# --------------------------------------------------------------------------------- tier mapping
def test_resolve_tier_maps_names_and_none() -> None:
    assert resolve_tier(None) is None
    assert resolve_tier("stm") is MemoryTier.STM
    assert resolve_tier("mtm") is MemoryTier.MTM
    assert resolve_tier("ltm") is MemoryTier.LTM


def test_resolve_tier_rejects_unknown() -> None:
    with pytest.raises(ValueError, match="unknown tier"):
        resolve_tier("nonsense")


# --------------------------------------------------------------------------------- add
async def test_tool_add_delegates_and_serialises() -> None:
    engine = _StubEngine()
    out = await tool_add(
        engine, SharedPrivateGuard(), content="Ada lives in Paris", user="u1", session="s1"
    )
    assert out["memory_id"] == "mem-123"
    assert out["promoted"] is True
    assert out["tiers_written"] == ["stm", "mtm"]  # JSON-mode dump turns the tuple into a list
    assert engine.add_calls[0]["content"] == "Ada lives in Paris"


async def test_tool_add_rejects_shared_before_touching_engine() -> None:
    engine = _StubEngine()
    with pytest.raises(SharedPlaneRejectedError):
        await tool_add(
            engine,
            SharedPrivateGuard(),
            content="x",
            user="u1",
            session="s1",
            visibility="shared",
        )
    assert engine.add_calls == []  # guard fired BEFORE any engine call


# --------------------------------------------------------------------------------- recall
async def test_tool_recall_resolves_tier_and_serialises() -> None:
    engine = _StubEngine()
    out = await tool_recall(
        engine, SharedPrivateGuard(), query="where is Ada?", user="u1", session="s1", tier="stm"
    )
    assert engine.recall_calls[0]["tier"] is MemoryTier.STM
    assert out["items"][0]["content"] == "Ada lives in Paris"


async def test_tool_recall_rejects_shared() -> None:
    engine = _StubEngine()
    with pytest.raises(SharedPlaneRejectedError):
        await tool_recall(
            engine, SharedPrivateGuard(), query="q", user="u1", session="s1", subject="Ada"
        )
    assert engine.recall_calls == []


# --------------------------------------------------------------------------------- get
async def test_tool_get_absent_returns_found_false() -> None:
    out = await tool_get(_StubEngine(), memory_id="missing", user="u1", session="s1")
    assert out == {"found": False, "memory_id": "missing"}


# --------------------------------------------------------------------------------- consolidate
async def test_tool_consolidate_serialises_report() -> None:
    out = await tool_consolidate(_StubEngine(), user="u1", session="s1")
    assert out == {"facts_extracted": 3, "added": 2, "superseded": 1, "noop": 0}


# --------------------------------------------------------------------------------- search
async def test_tool_search_resolves_tier_and_serialises() -> None:
    engine = _StubEngine()
    out = await tool_search(
        engine, SharedPrivateGuard(), query="where is Ada?", user="u1", session="s1", tier="mtm"
    )
    assert engine.search_calls[0]["tier"] is MemoryTier.MTM
    assert out["items"][0]["content"] == "Ada lives in Paris"


async def test_tool_search_rejects_shared() -> None:
    engine = _StubEngine()
    with pytest.raises(SharedPlaneRejectedError):
        await tool_search(
            engine, SharedPrivateGuard(), query="q", user="u1", session="s1", visibility="shared"
        )
    assert engine.search_calls == []


# --------------------------------------------------------------------------------- build_context
async def test_tool_build_context_serialises_view() -> None:
    engine = _StubEngine()
    out = await tool_build_context(
        engine, SharedPrivateGuard(), query="Ada?", user="u1", session="s1", max_chars=40
    )
    assert out["text"] == "- Ada lives in Paris"
    assert out["items"][0]["content"] == "Ada lives in Paris"
    assert out["degraded"] is None
    assert engine.context_calls[0]["max_chars"] == 40


async def test_tool_build_context_rejects_shared() -> None:
    engine = _StubEngine()
    with pytest.raises(SharedPlaneRejectedError):
        await tool_build_context(
            engine, SharedPrivateGuard(), query="q", user="u1", session="s1", subject="Ada"
        )
    assert engine.context_calls == []


# --------------------------------------------------------------------------------- ask
async def test_tool_ask_wraps_answer() -> None:
    engine = _StubEngine()
    out = await tool_ask(
        engine, SharedPrivateGuard(), question="Where is Ada?", user="u1", session="s1"
    )
    assert out == {"question": "Where is Ada?", "answer": "Ada lives in Paris."}
    assert engine.ask_calls[0]["question"] == "Where is Ada?"


async def test_tool_ask_rejects_shared() -> None:
    engine = _StubEngine()
    with pytest.raises(SharedPlaneRejectedError):
        await tool_ask(
            engine, SharedPrivateGuard(), question="q", user="u1", session="s1", object="Paris"
        )
    assert engine.ask_calls == []


# --------------------------------------------------------------------------------- server registry
async def test_tool_promote_delegates_and_serialises() -> None:
    engine = _StubEngine()
    out = await tool_promote(engine, memory_id="mem-123", to_tier="mtm", user="u1", session="s1")
    assert out["verb"] == "promote" and out["to_tier"] == "mtm"
    assert engine.verb_calls[-1]["verb"] == "promote"


async def test_tool_demote_delegates_and_serialises() -> None:
    engine = _StubEngine()
    out = await tool_demote(engine, memory_id="mem-123", to_tier="stm", user="u1", session="s1")
    assert out["verb"] == "demote" and out["to_tier"] == "stm"


async def test_tool_update_returns_new_memory() -> None:
    engine = _StubEngine()
    out = await tool_update(
        engine, memory_id="mem-123", new_content="Ada lives in Berlin", user="u1", session="s1"
    )
    assert out["verb"] == "update"
    assert out["memory_id"] == "mem-new" and out["superseded_id"] == "mem-123"


async def test_tool_delete_soft_deletes() -> None:
    engine = _StubEngine()
    out = await tool_delete(engine, memory_id="mem-123", user="u1", session="s1")
    assert out["verb"] == "delete" and out["invalidated"] is True


async def test_build_server_advertises_exactly_the_deep_dive_verbs() -> None:
    """The agent-facing surface is the DEEP-DIVE set, not every implemented verb.

    CONTRACT CHANGE, deliberate. This used to assert all 11 verbs. Capture, tier promotion/
    consolidation and context injection are all AUTOMATIC now (capture hooks +
    ``lifecycle/session_save.py``'s session-end and capture-pressure triggers), so the four verbs
    the hooks own are withdrawn from the surface the model sees: an agent that CAN call ``add``
    re-stores what the hook already captured, and one that CAN call ``promote``/``consolidate``
    second-guesses a lifecycle it has no visibility into.

    They remain REAL engine verbs — still implemented, still tested above, still reachable over the
    SDK/HTTP surface and via ``MU_MCP__EXPOSE_AUTOMATIC_TOOLS=true``. Only the agent-facing MCP
    advertisement narrows. Policy detail: ``tests/unit/test_mcp_surface_policy_unit.py``.
    """
    server = build_server()
    tool_names = {tool.name for tool in await server.list_tools()}
    assert tool_names == {
        "recall",
        "search",
        "get",
        "build_context",
        "ask",
        "update",
        "delete",
    }
    # update/delete stay: correcting or retracting a memory is real user intent a hook cannot infer.
    assert {"update", "delete"} <= tool_names
    # the hook-owned verbs must NOT be advertised to the model.
    assert {"add", "consolidate", "promote", "demote"}.isdisjoint(tool_names)


async def test_build_server_registers_the_silent_inject_resource() -> None:
    # Phase 2: the silent auto-inject RESOURCE is a parameterised template (per-session), so it is
    # advertised on the resource-TEMPLATE surface (resources/templates/list), not as a concrete
    # resource. Registration is store-free (the engine only starts inside the lifespan).
    server = build_server()
    templates = {tmpl.uriTemplate for tmpl in await server.list_resource_templates()}
    assert "memory-universe://silent/{session}" in templates
