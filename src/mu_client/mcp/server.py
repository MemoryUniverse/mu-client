"""``build_server`` — the local-plane MCP stdio host (AGENT-INTEGRATION-AUDIT-AND-PLAN §4 Phase 1).

A `FastMCP` server that exposes the canonical memory verbs as MCP **tools**, each delegating to the
REAL embedded engine (:class:`~mu_local.local_memory.LocalMemory`, hosted by
:class:`~mu_client.host.LocalMemoryHost`) against the caller's OWN real stores — no mock, no fake,
no in-process substitute. The engine is started once at server startup via the FastMCP ``lifespan``
and torn down on shutdown; every tool call reuses that ONE live engine.

Only real verbs are exposed: ``add``/``recall``/``get``/``consolidate``. ``promote``/``demote`` are
honest 501s in the engine and are NOT registered; ``update``/``delete`` have no embedded entry point
today and are omitted (plan §2A). Shared-plane arguments are refused at the surface by
:class:`~mu_client.mcp.guard.SharedPrivateGuard` — this is a PRIVATE-plane server (ADR-0003).
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from mcp.server.fastmcp import FastMCP
from mu_contracts.contracts.defaults import DEFAULT_CONSOLIDATE_LIMIT, DEFAULT_RECALL_LIMIT

from mu_client.config import ClientSettings, get_client_settings
from mu_client.host import LocalMemoryHost
from mu_client.mcp import tools
from mu_client.mcp.guard import SharedPrivateGuard

if TYPE_CHECKING:
    from mu_local.local_memory import LocalMemory

__all__ = ["build_server"]

_SERVER_NAME = "memory-universe-local"
_INSTRUCTIONS = (
    "Memory Universe — local, private memory for this agent. Tools: 'add' remembers a fact, "
    "'recall' retrieves relevant facts, 'get' fetches one memory by id, 'consolidate' distills "
    "recent memories into long-term facts. All memory is PRIVATE to this machine; there is no "
    "shared plane here — do not pass visibility/subject/predicate/object arguments."
)


class _EngineHolder:
    """Holds the ONE live ``LocalMemory`` the lifespan starts, so every tool closure reads the same
    engine. Raising (never returning ``None``) if read before startup keeps the failure honest."""

    def __init__(self) -> None:
        self._memory: LocalMemory | None = None

    def set(self, memory: LocalMemory) -> None:
        self._memory = memory

    def clear(self) -> None:
        self._memory = None

    @property
    def memory(self) -> LocalMemory:
        if self._memory is None:
            raise RuntimeError("MCP engine is not started (FastMCP lifespan did not run)")
        return self._memory


def build_server(*, settings: ClientSettings | None = None) -> FastMCP:
    """Construct the local-plane MCP server. Does NOT touch the stores — the engine starts inside
    the returned server's ``lifespan`` (i.e. when the server runs), so construction is cheap and
    unit-testable. ``settings`` overrides the env boundary (tests, per-run η isolation)."""
    resolved = settings or get_client_settings()
    default_user = resolved.default_user
    holder = _EngineHolder()
    guard = SharedPrivateGuard()

    @contextlib.asynccontextmanager
    async def lifespan(_server: FastMCP) -> AsyncIterator[dict[str, Any]]:
        host = LocalMemoryHost(resolved)
        memory = await host.start()  # REAL LocalMemory over the caller's real stores
        holder.set(memory)
        try:
            yield {}
        finally:
            holder.clear()
            await host.aclose()

    server: FastMCP = FastMCP(_SERVER_NAME, instructions=_INSTRUCTIONS, lifespan=lifespan)

    @server.tool(
        name="add",
        description="Remember a fact in this agent's private memory. Returns a write receipt "
        "(memory_id, content_hash, promoted, tiers_written, namespace).",
    )
    async def add(  # pyright: ignore[reportUnusedFunction] — registered via decorator
        content: str,
        user: str = default_user,
        session: str | None = None,
        importance_score: float | None = None,
        visibility: str | None = None,
        subject: str | None = None,
        predicate: str | None = None,
        object: str | None = None,
    ) -> dict[str, Any]:
        return await tools.tool_add(
            holder.memory,
            guard,
            content=content,
            user=user,
            session=session,
            importance_score=importance_score,
            visibility=visibility,
            subject=subject,
            predicate=predicate,
            object=object,
        )

    @server.tool(
        name="recall",
        description="Retrieve relevant memories for a query (fused STM+MTM+LTM ranked recall). "
        "Optional 'tier' narrows to stm|mtm|ltm.",
    )
    async def recall(  # pyright: ignore[reportUnusedFunction]
        query: str,
        user: str = default_user,
        session: str | None = None,
        tier: str | None = None,
        limit: int = DEFAULT_RECALL_LIMIT,
        visibility: str | None = None,
        subject: str | None = None,
        predicate: str | None = None,
        object: str | None = None,
    ) -> dict[str, Any]:
        return await tools.tool_recall(
            holder.memory,
            guard,
            query=query,
            user=user,
            session=session,
            tier=tier,
            limit=limit,
            visibility=visibility,
            subject=subject,
            predicate=predicate,
            object=object,
        )

    @server.tool(
        name="get",
        description="Fetch one memory by id from this agent's private STM. Returns {found,memory}.",
    )
    async def get(  # pyright: ignore[reportUnusedFunction]
        memory_id: str,
        user: str = default_user,
        session: str | None = None,
    ) -> dict[str, Any]:
        return await tools.tool_get(holder.memory, memory_id=memory_id, user=user, session=session)

    @server.tool(
        name="consolidate",
        description="Distill recent private memories into long-term SPO facts (MTM->LTM). "
        "Returns {facts_extracted, added, superseded, noop}.",
    )
    async def consolidate(  # pyright: ignore[reportUnusedFunction]
        user: str = default_user,
        session: str | None = None,
        limit: int = DEFAULT_CONSOLIDATE_LIMIT,
    ) -> dict[str, Any]:
        return await tools.tool_consolidate(holder.memory, user=user, session=session, limit=limit)

    return server
