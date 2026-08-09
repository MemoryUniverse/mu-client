"""Pure MCP tool wrappers — the ONE place each canonical verb's MCP shape lives.

Each function guards the plane boundary (where applicable), delegates to exactly ONE real engine
verb, and serialises the canonical Pydantic result DTO to a JSON-safe ``dict`` for the MCP wire.
No new memory logic — a thin, typed adapter over :class:`~mu_local.local_memory.LocalMemory`
(``mu-engine`` behind it), reachable by unit tests with a structural stub (the :class:`MemoryVerbs`
Protocol) and by the real stdio server with a live ``LocalMemory`` (``server.build_server``).
"""

from __future__ import annotations

from typing import Any, Protocol

from mu_contracts.contracts.defaults import DEFAULT_CONSOLIDATE_LIMIT, DEFAULT_RECALL_LIMIT
from mu_contracts.contracts.memory import MemoryResponse
from mu_contracts.contracts.recall import RecallResult
from mu_contracts.contracts.views import ConsolidateView, MemoryWriteResult
from mu_engine.storage.domain.memory import MemoryTier

from mu_client.mcp.guard import SharedPrivateGuard

__all__ = [
    "MemoryVerbs",
    "resolve_tier",
    "tool_add",
    "tool_consolidate",
    "tool_get",
    "tool_recall",
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
