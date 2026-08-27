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
    from mu_engine.services.health import MemoryHealthService
    from mu_engine.services.pin import PinService
    from mu_local.local_memory import LocalMemory

__all__ = ["build_server"]

_SERVER_NAME = "memory-universe-local"
_INSTRUCTIONS = (
    "Memory Universe — local, private memory for this agent.\n\n"
    "IMPORTANT — memory is AUTOMATIC here. You do NOT need to call a tool to remember things, to "
    "save them, or to get your usual context. Host hooks already do all three for you: every turn "
    "is captured and stored, relevant memory is injected into your context before you answer, and "
    "the tier lifecycle (promotion/consolidation into long-term memory) runs on its own in the "
    "background. Calling a tool to do any of that would duplicate work already done.\n\n"
    "These tools are a DEEP-DIVE ESCAPE HATCH, for the case where the memory already in your "
    "context is not enough. Reach for them when you need to go deeper or genuinely cannot find a "
    "fact you believe was said before — not as a routine step, and not 'just to be safe'.\n\n"
    "  - 'recall' (alias 'search') — search deeper than what was injected; ranked hits.\n"
    "  - 'build_context' — assemble a larger context window for a specific task.\n"
    "  - 'ask' — get a synthesised answer over recalled memory (local SLM).\n"
    "  - 'get' — fetch one memory by id.\n"
    "  - 'update' / 'delete' — correct or retract a memory the user says is wrong. These are real "
    "user intent that hooks cannot infer, so they ARE yours to call.\n\n"
    "All memory is PRIVATE to this machine; there is no shared plane here — do not pass "
    "visibility/subject/predicate/object arguments."
)

#: Verbs the HOOKS own. Not registered unless `MU_MCP__EXPOSE_AUTOMATIC_TOOLS=true`: capture is
#: done by the capture hooks, and promotion/consolidation by the daemon's lifecycle triggers
#: (`lifecycle/session_save.py`). Offering them to the model invites it to duplicate writes the
#: hook already made and to second-guess a lifecycle it cannot see. Prose alone does not reliably
#: stop that; not offering the tool does.
_AUTOMATIC_TOOL_NAMES = frozenset({"add", "consolidate", "promote", "demote"})

#: memory-health + pinning (memory-health-pinning-spec.md §7). Held off the DEFAULT surface by
#: two INDEPENDENT flags — see below for why they are two and not one, and for the spec clause
#: this deliberately overrides.
#:
#: ⚠ WHAT `remove_tool` ACTUALLY DOES: it deletes the tool from FastMCP's `_tool_manager`, so it
#: leaves `tools/list` AND `tools/call` — `await build_server().call_tool("health", {})` raises
#: `ToolError: Unknown tool: health`. The net effect is identical to never registering it. This
#: file previously described the state as "registered, but not offered by default"; that was
#: inaccurate and is corrected here rather than left as a half-true softener.
#:
#: THE DECISION, AND THE EVIDENCE ON BOTH SIDES — an owner-mandated distinction, so it is argued.
#:
#: The predicate this server applies is "do the HOOKS own this verb?", NOT "is it a write" —
#: `update`/`delete` are writes and ARE offered, because they encode real user intent hooks cannot
#: infer. By that predicate NONE of these three is hook-owned: `health` is read-pure (spec §0
#: "health is a read-only lens"; §5.1 "assessing health has zero lifecycle side-effect"; mu-core's
#: `MemoryHealthService` takes no write port at all, services/health/service.py:86-98), and
#: nothing automatic ever pins (spec §6.1 "no automatic sweep ever passes `force_unpinned`") while
#: the spec frames pinning as an explicitly human act throughout (§0 "a user marks a memory
#: PINNED"; §2.1 `pinned_by`; §6.4 "the user resolves by unpinning").
#:
#: FOR EXPOSING `health` — the spec is NOT silent, and saying it was is the error this comment
#: corrects. §7.1 line 332 reads:
#:     | **MCP tool** | `memory.local.health` | Reachable from inside the agent host where the
#:     user works, alongside `memory.local.status` (§7.15). |
#: In an MCP host the model is the only caller, so "not on the model's tool list" and "not
#: reachable inside the agent host" are the same claim negated. The same formula is a purpose
#: statement elsewhere in the design set (device-registry-sync-design.md:468). §7.2 line 340's MCP
#: row, in contrast, is `| **MCP tool** | `memory.pin` / `memory.unpin` | |` — an EMPTY Notes
#: column. So the two rows are not equally silent, and they are not bundled here.
#:
#: AGAINST, and why OFF still ships:
#:   1. `tests/unit/test_mcp_surface_policy_unit.py:47` asserts the default surface is EXACTLY the
#:      seven deep-dive names. It predates this work and is the owner's ratified statement of this
#:      surface; adding `health` to the default breaks it, and editing it is not mine to do.
#:   2. §7.1:332's companion is absent here. mu-client registers no `status` tool at all, so the
#:      row is describing the design-set-wide MCP surface — api-sdk-mcp-surface-design.md:904 puts
#:      "the 7+ MCP tools + `memory.local.status`" on the frozen contract list — and not this
#:      host's narrowed hook-aware surface. Satisfying it faithfully means registering `status`
#:      too, which belongs to the device-sync/surface subsystem, not to this change.
#:   3. Off → on is a one-line reversible flip; retracting a tool a model has begun calling is not.
#:
#: THIS IS RECORDED AS AN OPEN QUESTION FOR THE OWNER, phrased honestly: *the spec asks for
#: `memory.local.health` to be reachable inside the agent host, and this build does not make it so
#: by default; ratify or reverse.* It is NOT "the spec is silent".
#:
#: NO COMPENSATING PATH — do not let this decision rest on one. `mu health`/`mu pin`/`mu unpin`
#: and the `/health` `/pin` `/unpin` IPC routes cannot answer on a real host either: both engine
#: services need a `MemoryRepository` and mu-core ships only the Protocol
#: (mu-contracts/src/mu_contracts/ports/memory.py:142), so every surface answers its named
#: not-wired degrade. Reported to the owner as the blocking gap; not fixable from this repo.
_HEALTH_TOOL_NAMES = frozenset({"health"})
_PIN_TOOL_NAMES = frozenset({"pin", "unpin"})


class _EngineHolder:
    """Holds the ONE live ``LocalMemory`` (tool verbs) and the ONE ``RecallInjectBridge`` (silent
    resource) the lifespan starts, so every tool/resource closure reads the same live engine.
    Raising (never returning ``None``) if read before startup keeps the failure honest."""

    def __init__(self) -> None:
        self._memory: LocalMemory | None = None
        self._bridge: RecallInjectBridge | None = None
        self._health: MemoryHealthService | None = None
        self._pin: PinService | None = None

    def set(
        self,
        memory: LocalMemory,
        bridge: RecallInjectBridge,
        *,
        health: MemoryHealthService | None = None,
        pin: PinService | None = None,
    ) -> None:
        self._memory = memory
        self._bridge = bridge
        self._health = health
        self._pin = pin

    def clear(self) -> None:
        self._memory = None
        self._bridge = None
        self._health = None
        self._pin = None

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

    #: These two are ``| None`` rather than raise-if-unset, deliberately: unlike ``memory``/
    #: ``bridge``, "absent" is a REAL, currently-permanent state (mu-core ships no
    #: ``MemoryRepository`` — see :mod:`mu_client.memory_health`), not a lifespan ordering bug. The
    #: tool wrappers turn ``None`` into a loud, typed refusal.
    @property
    def health(self) -> MemoryHealthService | None:
        return self._health

    @property
    def pin(self) -> PinService | None:
        return self._pin


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
        # The silent-inject resource renders through the SAME real host as the hook path, and the
        # bridge is wired to THIS process's own real ``InprocBus`` (``host.bus`` ->
        # ``LocalMemory.bus`` -> ``LocalContainer.bus``) — so a mutation made through THIS server's
        # own tools (``update``/``delete``/``pin``/``promote``…) invalidates the warm body this
        # server is about to serve, instead of the pre-review shape where its own writes were
        # invisible to its own cache.
        #
        # STATED, not implied: an ``InprocBus`` is in-process only. A write made HERE still does
        # not reach the DAEMON's bridge in the daemon process, and vice versa — cross-process
        # invalidation needs a real cross-process bus (reported as a design delta). Until then the
        # bound on that window is time, not events: the warm entry carries ``computed_at`` and is
        # marked stale past ``stale_after_s`` and evicted past ``hot_session_ttl_s``.
        bridge = RecallInjectBridge(host, settings=resolved.inject, bus=host.bus)
        # memory-health + pinning (spec §7): taken from the SAME real ``LocalMemory`` this
        # lifespan just started (``LocalMemory.health``/``.pin`` -> ``LocalContainer``), never a
        # second composition root — the same passthrough discipline as ``host.bus`` above.
        # mu-core now implements the ``MemoryRepository`` façade
        # (``mu_engine.services.memory.repository.TieredMemoryRepository``), so with
        # MU_MCP__EXPOSE_HEALTH_TOOL / MU_MCP__EXPOSE_PIN_TOOLS on these three tools ANSWER.
        # Still ``| None``-shaped: on a vector backend with no partition-walk primitive the
        # container builds neither service and the tools keep refusing loudly (ServiceNotWiredError)
        # instead of fabricating a health view or acking a pin no store would persist.
        holder.set(memory, bridge, health=memory.health, pin=memory.pin)
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

    @server.tool(
        name="health",
        description="Report the HEALTH of this agent's private memory: one bounded page of "
        "at-risk memories (stale / low-confidence / conflicting / decaying) plus pinned and "
        "archived markers, with a counts-only summary. A read-only lens — assessing health "
        "changes nothing. Content-free: ids, flags, scores and timestamps, never memory text. "
        "Optional 'flags' narrows to specific categories; 'cursor' continues the previous page.",
    )
    async def health(  # pyright: ignore[reportUnusedFunction]
        user: str = default_user,
        session: str | None = None,
        flags: list[str] | None = None,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        return await tools.tool_health(
            holder.health,
            settings=resolved,
            user=user,
            session=session,
            flags=flags,
            cursor=cursor,
        )

    @server.tool(
        name="pin",
        description="PIN one memory so the lifecycle can never demote, garbage-collect or "
        "auto-supersede it — the user's 'never forget this'. Pin changes RETENTION only: it does "
        "not make the memory more likely to be recalled and does not change who can read it. "
        "Optional 'reason' is a short classification ('policy', 'decision'), never a note. "
        "Returns {memory_id, pinned, pinned_at, version}. Fails loud on an unknown id, an item "
        "that has already left the live set, or the partition's pin limit.",
    )
    async def pin(  # pyright: ignore[reportUnusedFunction]
        memory_id: str,
        reason: str | None = None,
        user: str = default_user,
        session: str | None = None,
    ) -> dict[str, Any]:
        return await tools.tool_pin(
            holder.pin,
            settings=resolved,
            memory_id=memory_id,
            user=user,
            session=session,
            reason=reason,
        )

    @server.tool(
        name="unpin",
        description="UNPIN one memory, releasing the retention override so the normal lifecycle "
        "(demotion, GC, supersession) resumes for it. Works in every state. Returns "
        "{memory_id, pinned, pinned_at, version}. Fails loud on an unknown id.",
    )
    async def unpin(  # pyright: ignore[reportUnusedFunction]
        memory_id: str,
        user: str = default_user,
        session: str | None = None,
    ) -> dict[str, Any]:
        return await tools.tool_unpin(
            holder.pin, settings=resolved, memory_id=memory_id, user=user, session=session
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

    # ---- HOOKS OWN THE ROUTINE PATH -----------------------------------------------------------
    # Capture, tier promotion/consolidation and context injection all happen AUTOMATICALLY via the
    # host hooks + the daemon's lifecycle triggers. The verbs that would duplicate that work are
    # therefore withdrawn from the agent-facing surface by default: an agent that CAN call `add`
    # will re-store what the hook already captured, and one that CAN call `promote`/`consolidate`
    # will second-guess a lifecycle it has no visibility into. Instructions alone do not reliably
    # prevent that — an unoffered tool does. What remains is the deliberate DEEP-DIVE set
    # (recall/search/get/build_context/ask) plus the two verbs that encode real user intent hooks
    # cannot infer (update/delete).
    if not resolved.mcp.expose_automatic_tools:
        for tool_name in sorted(_AUTOMATIC_TOOL_NAMES):
            server.remove_tool(tool_name)

    # ---- memory-health + pinning ------------------------------------------------------------
    # Two INDEPENDENT gates (see _HEALTH_TOOL_NAMES above for the full argument and the spec
    # clause this overrides). `remove_tool` deletes the tool outright — it leaves tools/list AND
    # tools/call — so after this the default surface has no `health`/`pin`/`unpin` at any level.
    if not resolved.mcp.expose_health_tool:
        for tool_name in sorted(_HEALTH_TOOL_NAMES):
            server.remove_tool(tool_name)
    if not resolved.mcp.expose_pin_tools:
        for tool_name in sorted(_PIN_TOOL_NAMES):
            server.remove_tool(tool_name)

    return server
