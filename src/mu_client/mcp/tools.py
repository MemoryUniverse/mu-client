"""Pure MCP tool wrappers — the ONE place each canonical verb's MCP shape lives.

Each function guards the plane boundary (where applicable), delegates to exactly ONE real engine
verb, and serialises the canonical Pydantic result DTO to a JSON-safe ``dict`` for the MCP wire.
No new memory logic — a thin, typed adapter over :class:`~mu_local.local_memory.LocalMemory`
(``mu-engine`` behind it), reachable by unit tests with a structural stub (the :class:`MemoryVerbs`
Protocol) and by the real stdio server with a live ``LocalMemory`` (``server.build_server``).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Protocol

from mu_contracts.contracts.defaults import DEFAULT_CONSOLIDATE_LIMIT, DEFAULT_RECALL_LIMIT
from mu_contracts.contracts.memory import MemoryResponse
from mu_contracts.contracts.recall import RecallResult
from mu_contracts.contracts.views import (
    ConsolidateView,
    ContextView,
    MemoryVerbResult,
    MemoryWriteResult,
)
from mu_contracts.domain.model.pin import PinRequest
from mu_engine.storage.domain.memory import MemoryTier

from mu_client.errors import ServiceNotWiredError
from mu_client.mcp.guard import SharedPrivateGuard
from mu_client.memory_health import local_scope, namespace_for, parse_health_flags

if TYPE_CHECKING:
    from mu_engine.services.health import MemoryHealthService
    from mu_engine.services.pin import PinService

    from mu_client.config import ClientSettings

__all__ = [
    "MemoryVerbs",
    "resolve_tier",
    "tool_add",
    "tool_ask",
    "tool_build_context",
    "tool_consolidate",
    "tool_delete",
    "tool_demote",
    "tool_get",
    "tool_health",
    "tool_pin",
    "tool_promote",
    "tool_recall",
    "tool_search",
    "tool_unpin",
    "tool_update",
]

_TIER_BY_NAME: dict[str, MemoryTier] = {tier.value: tier for tier in MemoryTier}


class MemoryVerbs(Protocol):
    """The exact engine verb subset these tools call — the structural contract ``LocalMemory``
    already satisfies (its real signatures are a superset with extra, defaulted plane fields). Lets
    the unit tests drive the SAME wrapper code with a lightweight stub, no real stores required."""

    async def add(
        self,
        content: str,
        *,
        user: str = ...,
        session: str | None = ...,
        importance_score: float | None = ...,
    ) -> MemoryWriteResult: ...

    async def recall(
        self,
        query: str,
        *,
        user: str = ...,
        session: str | None = ...,
        tier: MemoryTier | None = ...,
        limit: int = ...,
    ) -> RecallResult: ...

    async def get(
        self,
        memory_id: str,
        *,
        user: str = ...,
        session: str | None = ...,
    ) -> MemoryResponse | None: ...

    async def consolidate(
        self,
        *,
        user: str = ...,
        session: str | None = ...,
        limit: int = ...,
    ) -> ConsolidateView: ...

    async def search(
        self,
        query: str,
        *,
        user: str = ...,
        session: str | None = ...,
        tier: MemoryTier | None = ...,
        limit: int = ...,
    ) -> RecallResult: ...

    async def context(
        self,
        query: str,
        *,
        user: str = ...,
        session: str | None = ...,
        limit: int = ...,
        max_chars: int | None = ...,
    ) -> ContextView: ...

    async def ask(
        self,
        question: str,
        *,
        user: str = ...,
        session: str | None = ...,
        limit: int = ...,
    ) -> str: ...

    async def promote(
        self,
        memory_id: str,
        *,
        to_tier: str,
        user: str = ...,
        session: str | None = ...,
    ) -> MemoryVerbResult: ...

    async def demote(
        self,
        memory_id: str,
        *,
        to_tier: str = ...,
        user: str = ...,
        session: str | None = ...,
    ) -> MemoryVerbResult: ...

    async def update(
        self,
        memory_id: str,
        new_content: str,
        *,
        user: str = ...,
        session: str | None = ...,
    ) -> MemoryVerbResult: ...

    async def delete(
        self,
        memory_id: str,
        *,
        user: str = ...,
        session: str | None = ...,
    ) -> MemoryVerbResult: ...


def resolve_tier(tier: str | None) -> MemoryTier | None:
    """Map an MCP ``tier`` string (``"stm"``/``"mtm"``/``"ltm"``) to :class:`MemoryTier`; ``None``
    fuses all three channels. An unknown value fails loud (never silently ignored)."""
    if tier is None:
        return None
    try:
        return _TIER_BY_NAME[tier]
    except KeyError:
        raise ValueError(
            f"unknown tier {tier!r}; expected one of {sorted(_TIER_BY_NAME)} or omit to fuse all"
        ) from None


async def tool_add(
    engine: MemoryVerbs,
    guard: SharedPrivateGuard,
    *,
    content: str,
    user: str,
    session: str | None,
    importance_score: float | None = None,
    visibility: str | None = None,
    subject: str | None = None,
    predicate: str | None = None,
    object: str | None = None,
) -> dict[str, Any]:
    """``add`` — ingest one memory into the caller's private partition (STM durable -> deterministic
    importance-gated STM->MTM promote). Returns the canonical ``MemoryWriteResult`` receipt."""
    guard.assert_private(visibility=visibility, subject=subject, predicate=predicate, object=object)
    result = await engine.add(
        content, user=user, session=session, importance_score=importance_score
    )
    return result.model_dump(mode="json")


async def tool_recall(
    engine: MemoryVerbs,
    guard: SharedPrivateGuard,
    *,
    query: str,
    user: str,
    session: str | None,
    tier: str | None = None,
    limit: int = DEFAULT_RECALL_LIMIT,
    visibility: str | None = None,
    subject: str | None = None,
    predicate: str | None = None,
    object: str | None = None,
) -> dict[str, Any]:
    """``recall`` — federate-live ranked recall (STM floor + MTM dense + LTM graph, fused) over the
    caller's OWN private partition. Returns the canonical ``RecallResult``."""
    guard.assert_private(visibility=visibility, subject=subject, predicate=predicate, object=object)
    result = await engine.recall(
        query, user=user, session=session, tier=resolve_tier(tier), limit=limit
    )
    return result.model_dump(mode="json")


async def tool_get(
    engine: MemoryVerbs,
    *,
    memory_id: str,
    user: str,
    session: str | None,
) -> dict[str, Any]:
    """``get`` — point-get one memory by id from the caller's STM partition. Returns a
    ``{found, memory}`` envelope (``found=False`` when absent — never a fabricated row)."""
    response = await engine.get(memory_id, user=user, session=session)
    if response is None:
        return {"found": False, "memory_id": memory_id}
    return {"found": True, "memory": response.model_dump(mode="json")}


async def tool_consolidate(
    engine: MemoryVerbs,
    *,
    user: str,
    session: str | None,
    limit: int = DEFAULT_CONSOLIDATE_LIMIT,
) -> dict[str, Any]:
    """``consolidate`` — MTM->LTM distillation: extract bi-temporal SPO facts from the recent STM
    window into the LTM graph (invalidate-don't-delete supersession). Returns a ``ConsolidateView``.
    """
    report = await engine.consolidate(user=user, session=session, limit=limit)
    return report.model_dump(mode="json")


async def tool_search(
    engine: MemoryVerbs,
    guard: SharedPrivateGuard,
    *,
    query: str,
    user: str,
    session: str | None,
    tier: str | None = None,
    limit: int = DEFAULT_RECALL_LIMIT,
    visibility: str | None = None,
    subject: str | None = None,
    predicate: str | None = None,
    object: str | None = None,
) -> dict[str, Any]:
    """``search`` — the mem0 muscle-memory alias for ``recall`` (ONE behaviour, spec §CC-6): the
    same federate-live ranked recall (STM floor + MTM dense + LTM graph, fused) over the caller's
    OWN private partition. Returns the canonical ``RecallResult``. Prefer ``recall`` as the
    canonical verb; ``search`` exists so a mem0-shaped caller finds the same tool by its familiar
    name."""
    guard.assert_private(visibility=visibility, subject=subject, predicate=predicate, object=object)
    result = await engine.search(
        query, user=user, session=session, tier=resolve_tier(tier), limit=limit
    )
    return result.model_dump(mode="json")


async def tool_build_context(
    engine: MemoryVerbs,
    guard: SharedPrivateGuard,
    *,
    query: str,
    user: str,
    session: str | None,
    limit: int = DEFAULT_RECALL_LIMIT,
    max_chars: int | None = None,
    visibility: str | None = None,
    subject: str | None = None,
    predicate: str | None = None,
    object: str | None = None,
) -> dict[str, Any]:
    """``build_context`` — assemble a ready-to-inject context window for a task/query by
    DETERMINISTIC concatenation of the ranked recall hits (no LLM synthesis — spec §7 INJECT
    render). Returns the canonical ``ContextView`` (``text`` = the rendered window, ``items`` = the
    hits it was built from, ``degraded`` = a named degrade label or ``None``). ``max_chars`` caps
    the rendered ``text``. Use this when an agent needs relevant background context to work with;
    use ``recall``/``search`` when you want the raw ranked hits, ``ask`` when you want a synthesised
    answer to a question."""
    guard.assert_private(visibility=visibility, subject=subject, predicate=predicate, object=object)
    view = await engine.context(query, user=user, session=session, limit=limit, max_chars=max_chars)
    return view.model_dump(mode="json")


async def tool_ask(
    engine: MemoryVerbs,
    guard: SharedPrivateGuard,
    *,
    question: str,
    user: str,
    session: str | None,
    limit: int = DEFAULT_RECALL_LIMIT,
    visibility: str | None = None,
    subject: str | None = None,
    predicate: str | None = None,
    object: str | None = None,
) -> dict[str, Any]:
    """``ask`` — answer a natural-language QUESTION over the caller's OWN recalled context, via the
    configured LLM's ANSWER task (the local SLM). Returns ``{question, answer}``. Heuristic mode
    (no LLM configured) refuses loudly (``LlmNotConfiguredError``) rather than fabricating a
    degraded answer — surfaced to the MCP caller as a tool error, never a silent empty string. Use
    this when you want a synthesised answer; use ``build_context`` for the raw context window or
    ``recall``/``search`` for the ranked hits."""
    guard.assert_private(visibility=visibility, subject=subject, predicate=predicate, object=object)
    answer = await engine.ask(question, user=user, session=session, limit=limit)
    return {"question": question, "answer": answer}


async def tool_promote(
    engine: MemoryVerbs,
    *,
    memory_id: str,
    to_tier: str,
    user: str,
    session: str | None,
) -> dict[str, Any]:
    """``promote`` — TARGETED single-memory promotion up one tier (``to_tier="mtm"``: STM->MTM, or
    ``to_tier="ltm"``: MTM->LTM). Runs the REAL promotion path on that one resident item
    (``PromotionService`` copy-on-write / ``DistillPipeline`` leg) — a nonexistent id or invalid
    ``to_tier`` fails loud (never a fake success). Returns the canonical ``MemoryVerbResult``."""
    result = await engine.promote(memory_id, to_tier=to_tier, user=user, session=session)
    return result.model_dump(mode="json")


async def tool_demote(
    engine: MemoryVerbs,
    *,
    memory_id: str,
    to_tier: str,
    user: str,
    session: str | None,
) -> dict[str, Any]:
    """``demote`` — TARGETED MTM->STM tier-down (``to_tier="stm"``) of one resident item, reusing
    the real ``DemotionService`` write-ahead-then-remove sequence. Returns ``MemoryVerbResult``."""
    result = await engine.demote(memory_id, to_tier=to_tier, user=user, session=session)
    return result.model_dump(mode="json")


async def tool_update(
    engine: MemoryVerbs,
    *,
    memory_id: str,
    new_content: str,
    user: str,
    session: str | None,
) -> dict[str, Any]:
    """``update`` — SUPERSEDE the memory with ``new_content`` (invalidate-don't-delete): the old
    version is marked superseded by the new one via the real ``invalidate`` machinery. Returns the
    NEW memory (``memory_id`` = new id, ``superseded_id`` = old id) as a ``MemoryVerbResult``."""
    result = await engine.update(memory_id, new_content, user=user, session=session)
    return result.model_dump(mode="json")


async def tool_delete(
    engine: MemoryVerbs,
    *,
    memory_id: str,
    user: str,
    session: str | None,
) -> dict[str, Any]:
    """``delete`` — soft-delete (invalidate-don't-delete): MTM/LTM flip to ``state=expired`` +
    ``invalid_at`` (kept in bi-temporal history, dropped from active recall); STM (ephemeral) is
    evicted. NEVER a hard delete of active data. Returns a ``MemoryVerbResult`` (``invalidated``,
    ``tiers_affected``). A nonexistent id fails loud (404-equivalent), never a fake success."""
    result = await engine.delete(memory_id, user=user, session=session)
    return result.model_dump(mode="json")


# ---- memory-health + pinning (memory-health-pinning-spec.md §7.1 line 332 / §7.2 line 340) ------
# These three do NOT go through :class:`MemoryVerbs`: health and pin are not ``LocalMemory`` verbs
# at all (that class has no ``pin``/``unpin``/``health`` method — mu-core exposes them only as the
# two standalone services). They take the service DIRECTLY, ``| None`` for the case the
# composition root could not build one, and refuse loud in that case rather than answering — the
# same stance ``tool_ask`` takes in heuristic mode.
#
# η is built here, once, from the client's env boundary (``memory_health.namespace_for``), so the
# health page and a ``recall`` by the same caller address the SAME partition; the ``ClientScope``
# the services require is derived from that authorized η (``memory_health.local_scope``).


async def tool_health(
    health: MemoryHealthService | None,
    *,
    settings: ClientSettings,
    user: str,
    session: str | None,
    flags: Sequence[str] | None = None,
    cursor: str | None = None,
) -> dict[str, Any]:
    """``health`` — ONE bounded page of the caller's own memory health (``MemoryHealthView``).

    A READ-ONLY LENS (spec §0): it derives its flags from fields the engine already maintains and
    mutates nothing — no reinforcement, no tier change. Content-free by construction: every field
    of the view is an id, an enum, a number or a timestamp (mu-core did not build the spec's
    ``preview``), so nothing here can carry memory text.
    """
    if health is None:
        raise ServiceNotWiredError("MemoryHealthService")
    ns = namespace_for(settings, user=user, session=session)
    view = await health.assess(
        local_scope(ns), ns, filter_flags=parse_health_flags(flags), cursor=cursor
    )
    return view.model_dump(mode="json")


async def tool_pin(
    pin: PinService | None,
    *,
    settings: ClientSettings,
    memory_id: str,
    user: str,
    session: str | None,
    reason: str | None = None,
) -> dict[str, Any]:
    """``pin`` — mark one memory so the lifecycle may never demote / GC / auto-supersede it.

    Pin is RETENTION, never ACCESS and never RELEVANCE (spec §0, §6.5): it does not widen anyone's
    read set and does not boost the item in recall. ``reason`` is a short NAMED classification the
    owner attaches to their own pin ("policy", "decision"), bounded at 200 chars by ``PinRequest``
    — never a note field and never memory text. Every refusal (not the owner / no such id / not
    pinnable / over the partition's pin bound) propagates as a typed, non-enumerating error.
    """
    if pin is None:
        raise ServiceNotWiredError("PinService")
    ns = namespace_for(settings, user=user, session=session)
    result = await pin.pin(local_scope(ns), ns, PinRequest(memory_id=memory_id, reason=reason))
    return result.model_dump(mode="json")


async def tool_unpin(
    pin: PinService | None,
    *,
    settings: ClientSettings,
    memory_id: str,
    user: str,
    session: str | None,
) -> dict[str, Any]:
    """``unpin`` — release the retention override, letting the normal lifecycle resume.

    Reachable in EVERY state (unlike ``pin``): an item that reached a settled exit while pinned is
    unconditionally GC-ineligible, so unpin must always be able to release it.
    """
    if pin is None:
        raise ServiceNotWiredError("PinService")
    ns = namespace_for(settings, user=user, session=session)
    result = await pin.unpin(local_scope(ns), ns, memory_id)
    return result.model_dump(mode="json")
