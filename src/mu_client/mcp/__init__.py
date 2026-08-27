"""``mu_client.mcp`` — the LOCAL-PLANE MCP server surface for Memory Universe.

AGENT-INTEGRATION-AUDIT-AND-PLAN.md §4 Phase 1: a stdio `FastMCP` host that exposes the canonical
memory verbs as MCP **tools** so `claude code`, `codex`, and any MCP client can register MU and
call them. Local plane ONLY — the shared/transfer plane (`mu-server`: rooms, governed transfer) is
PARKED and never reachable from here (ADR-0003; :class:`~mu_client.mcp.guard.SharedPrivateGuard`).

Every tool delegates to the REAL embedded engine (`mu_local.LocalMemory`, hosted by
:class:`~mu_client.host.LocalMemoryHost`) against the caller's OWN real stores — no new memory
logic, no mock, no fake. Only the verbs the engine actually implements today are exposed:
``add``/``recall``/``get``/``consolidate`` plus ``search`` (mem0 alias of ``recall``),
``build_context`` (the deterministic INJECT context window, ``LocalMemory.context``) and ``ask``
(SLM-synthesised Q&A, ``LocalMemory.ask``) — every one backed by a genuinely-built engine method,
verified against real stores + the local SLM. ``promote``/``demote`` are honest 501s in the engine
(``SurfaceVerbNotImplementedError``) and are DELIBERATELY NOT exposed as working tools; ``update``/
``delete`` have no embedded-engine entry point today and are likewise omitted (see the plan §2A).
"""

from __future__ import annotations

from mu_client.mcp.guard import (
    SHARED_PLANE_FIELDS,
    SharedPlaneRejectedError,
    SharedPrivateGuard,
)
from mu_client.mcp.server import build_server

__all__ = [
    "SHARED_PLANE_FIELDS",
    "SharedPlaneRejectedError",
    "SharedPrivateGuard",
    "build_server",
]
