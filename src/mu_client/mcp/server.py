"""``build_server`` — the local-plane MCP stdio host (AGENT-INTEGRATION-AUDIT-AND-PLAN §4 Phase 1).

A `FastMCP` server that exposes the canonical memory verbs as MCP **tools**, each delegating to the
REAL embedded engine (:class:`~mu_local.local_memory.LocalMemory`, hosted by
:class:`~mu_client.host.LocalMemoryHost`) against the caller's OWN real stores — no mock, no fake,
no in-process substitute. The engine is started once at server startup via the FastMCP ``lifespan``
and torn down on shutdown; every tool call reuses that ONE live engine.

Only real verbs are exposed: ``add``/``recall``/``get``/``consolidate``/``search``/
``build_context``/``ask`` — each backed by a genuinely-built
:class:`~mu_local.local_memory.LocalMemory` method (no
stub, no 501). ``search`` is the mem0 alias of ``recall``; ``build_context`` renders the
deterministic INJECT context window (``LocalMemory.context``); ``ask`` synthesises an answer via the
configured local SLM (``LocalMemory.ask`` — refuses loudly in heuristic mode, never fabricates).
``promote``/``demote``/``update``/``delete`` are now REAL engine verbs (build-queue §13 item 5) and
ARE registered — targeted single-memory tier moves / supersede / soft-delete over the real
lifecycle + bi-temporal invalidation machinery (``LocalMemory.promote``/``.demote``/``.update``/
``.delete`` -> ``SurfaceFacade``), taking the MCP tool count to 11. Shared-plane arguments are
refused at the surface by
:class:`~mu_client.mcp.guard.SharedPrivateGuard` — this is a PRIVATE-plane server (ADR-0003).

Phase 2 (plan §4) adds the SILENT auto-inject RESOURCE ``memory-universe://silent/{session}`` — an
MCP resource an MCP host can auto-attach WITHOUT the agent calling a tool, rendering the same
DISTILLED recall/inject context (:class:`~mu_client.inject.recall_bridge.RecallInjectBridge` →
:func:`~mu_client.inject.distill.distill_items`) the Claude Code hook emits as ``additionalContext``
(the host auto-attaches the resource; no tool call).
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from mcp.server.fastmcp import FastMCP
from mu_contracts.contracts.defaults import DEFAULT_CONSOLIDATE_LIMIT, DEFAULT_RECALL_LIMIT

from mu_client.config import ClientSettings, get_client_settings
from mu_client.host import LocalMemoryHost
from mu_client.inject.recall_bridge import RecallInjectBridge
from mu_client.mcp import tools
from mu_client.mcp.guard import SharedPrivateGuard

if TYPE_CHECKING:
    from mu_local.local_memory import LocalMemory

__all__ = ["build_server"]

_SERVER_NAME = "memory-universe-local"
_INSTRUCTIONS = (
    "Memory Universe — local, private memory for this agent. Tools: 'add' remembers a fact, "
    "'recall' (alias 'search') retrieves relevant facts as ranked hits, 'build_context' assembles "
    "a ready-to-inject context window for a task, 'ask' answers a question by synthesising over "
    "recalled context (local SLM), 'get' fetches one memory by id, 'consolidate' distills recent "
    "memories into long-term facts, 'promote'/'demote' move one memory between tiers, 'update' "
    "supersedes a memory with new content, 'delete' soft-deletes one (kept in history). Choose "
    "'recall'/'search' for raw hits, 'build_context' for a "
    "context window, 'ask' for a synthesised answer. All memory is PRIVATE to this machine; there "
    "is no shared plane here — do not pass visibility/subject/predicate/object arguments."
)


class _EngineHolder:
    """Holds the ONE live ``LocalMemory`` (tool verbs) and the ONE ``RecallInjectBridge`` (silent
    resource) the lifespan starts, so every tool/resource closure reads the same live engine.
    Raising (never returning ``None``) if read before startup keeps the failure honest."""

    def __init__(self) -> None:
        self._memory: LocalMemory | None = None
        self._bridge: RecallInjectBridge | None = None

    def set(self, memory: LocalMemory, bridge: RecallInjectBridge) -> None:
        self._memory = memory
        self._bridge = bridge

    def clear(self) -> None:
        self._memory = None
        self._bridge = None

    @property
    def memory(self) -> LocalMemory:
        if self._memory is None:
            raise RuntimeError("MCP engine is not started (FastMCP lifespan did not run)")
        return self._memory

    @property
    def bridge(self) -> RecallInjectBridge:
        if self._bridge is None:
            raise RuntimeError("MCP inject bridge is not started (FastMCP lifespan did not run)")
        return self._bridge


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
        # The silent-inject resource renders through the SAME real host as the hook path — the
        # bridge is PULL-only here (no InprocBus wired in the stdio server), which is the honest
        # degrade the bridge already documents; ``bus=None`` needs no new DegradeReason.
        bridge = RecallInjectBridge(host, settings=resolved.inject)
        holder.set(memory, bridge)
        try:
            yield {}
        finally:
            holder.clear()
            await bridge.aclose()  # no-op when no bus was wired (PULL-only)
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

    @server.tool(
        name="search",
        description="Retrieve relevant memories for a query as ranked hits (mem0-style alias of "
        "'recall' — same fused STM+MTM+LTM recall). Optional 'tier' narrows to stm|mtm|ltm. Use "
        "'build_context' instead when you want a ready-to-inject context window, 'ask' for a "
        "synthesised answer.",
    )
    async def search(  # pyright: ignore[reportUnusedFunction]
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
        return await tools.tool_search(
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
        name="build_context",
        description="Assemble a ready-to-inject CONTEXT WINDOW of the memories relevant to a task "
        "or query (deterministic concatenation of the ranked hits — no LLM). Returns "
        "{text, items, degraded}: 'text' is the rendered window, 'items' the hits behind it. "
        "'max_chars' caps the text. Use this to get relevant context for a task; use "
        "'recall'/'search' for raw ranked hits, 'ask' for a synthesised answer to a question.",
    )
    async def build_context(  # pyright: ignore[reportUnusedFunction]
        query: str,
        user: str = default_user,
        session: str | None = None,
        limit: int = DEFAULT_RECALL_LIMIT,
        max_chars: int | None = None,
        visibility: str | None = None,
        subject: str | None = None,
        predicate: str | None = None,
        object: str | None = None,
    ) -> dict[str, Any]:
        return await tools.tool_build_context(
            holder.memory,
            guard,
            query=query,
            user=user,
            session=session,
            limit=limit,
            max_chars=max_chars,
            visibility=visibility,
            subject=subject,
            predicate=predicate,
            object=object,
        )

    @server.tool(
        name="ask",
        description="Answer a natural-language QUESTION over this agent's own recalled memory, "
        "synthesised by the local SLM. Returns {question, answer}. Refuses loudly if no LLM is "
        "configured (never fabricates). Use this for a synthesised answer; use 'build_context' for "
        "the raw context window or 'recall'/'search' for the ranked hits.",
    )
    async def ask(  # pyright: ignore[reportUnusedFunction]
        question: str,
        user: str = default_user,
        session: str | None = None,
        limit: int = DEFAULT_RECALL_LIMIT,
        visibility: str | None = None,
        subject: str | None = None,
        predicate: str | None = None,
        object: str | None = None,
    ) -> dict[str, Any]:
        return await tools.tool_ask(
            holder.memory,
            guard,
            question=question,
            user=user,
            session=session,
            limit=limit,
            visibility=visibility,
            subject=subject,
            predicate=predicate,
            object=object,
        )

    @server.tool(
        name="promote",
        description="Promote one memory UP a tier: to_tier='mtm' moves STM->MTM, to_tier='ltm' "
        "moves MTM->LTM (distilled into long-term facts). Runs the real promotion path on that one "
        "memory. Returns {memory_id, verb, from_tier, to_tier, tiers_affected}. Fails loud on an "
        "unknown id (not in the source tier) or invalid to_tier.",
    )
    async def promote(  # pyright: ignore[reportUnusedFunction]
        memory_id: str,
        to_tier: str,
        user: str = default_user,
        session: str | None = None,
    ) -> dict[str, Any]:
        return await tools.tool_promote(
            holder.memory, memory_id=memory_id, to_tier=to_tier, user=user, session=session
        )

    @server.tool(
        name="demote",
        description="Demote one memory DOWN a tier: MTM->STM (to_tier='stm'). Reuses the real "
        "forgetting-curve demotion (write STM copy, then remove the MTM point). Returns "
        "{memory_id, verb, from_tier, to_tier, tiers_affected}. Fails loud on an unknown id.",
    )
    async def demote(  # pyright: ignore[reportUnusedFunction]
        memory_id: str,
        to_tier: str = "stm",
        user: str = default_user,
        session: str | None = None,
    ) -> dict[str, Any]:
        return await tools.tool_demote(
            holder.memory, memory_id=memory_id, to_tier=to_tier, user=user, session=session
        )

    @server.tool(
        name="update",
        description="Update a memory by SUPERSEDING it with new content (invalidate-don't-delete): "
        "the new version becomes active, the old is marked superseded (kept in history). Returns "
        "the NEW memory {memory_id (new id), verb, superseded_id (old id), tiers_affected}. Fails "
        "loud if the id is not found.",
    )
    async def update(  # pyright: ignore[reportUnusedFunction]
        memory_id: str,
        new_content: str,
        user: str = default_user,
        session: str | None = None,
    ) -> dict[str, Any]:
        return await tools.tool_update(
            holder.memory,
            memory_id=memory_id,
            new_content=new_content,
            user=user,
            session=session,
        )

    @server.tool(
        name="delete",
        description="Delete a memory by SOFT-DELETE (invalidate-don't-delete): it stops appearing "
        "in active recall but stays in bi-temporal history (MTM/LTM flipped to expired + "
        "invalid_at; ephemeral STM evicted). Never a hard delete of active data. Returns "
        "{memory_id, verb, invalidated, tiers_affected}. Fails loud if the id is not found.",
    )
    async def delete(  # pyright: ignore[reportUnusedFunction]
        memory_id: str,
        user: str = default_user,
        session: str | None = None,
    ) -> dict[str, Any]:
        return await tools.tool_delete(
            holder.memory, memory_id=memory_id, user=user, session=session
        )

    @server.resource(
        "memory-universe://silent/{session}",
        name="silent-context",
        title="Silent auto-inject context",
        description=(
            "Relevant DISTILLED memory context for a session, auto-attachable by an MCP host "
            "(Codex/Desktop) with NO explicit tool call (api-mcp-surface-spec §5.2 silent "
            "channel; AGENT-INTEGRATION-AUDIT-AND-PLAN §4 Phase 2). Renders through the SAME "
            "recall/inject bridge as the Claude Code hook additionalContext: tool-capture/output "
            "noise (Write:/Bash:…) filtered, deduped, salient/promoted facts first."
        ),
        mime_type="text/plain",
    )
    async def silent_context(  # pyright: ignore[reportUnusedFunction] — registered via decorator
        session: str,
    ) -> str:
        # Delegates to the SAME distilled render the hook path uses (RecallInjectBridge.render →
        # distill_items). No new memory logic: a read-only, never-mutating projection of the
        # caller's OWN private stores. An empty string = cold (no memories yet for this session).
        rendered = await holder.bridge.render(session)
        return rendered.body

    return server
