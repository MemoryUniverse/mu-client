"""The local MCP tool surface, **as data** — one source of truth for what this daemon offers.

Why this module exists at all: :mod:`mu_client.consent.capabilities` has to answer *"what can this
device do, and which of those things does a shared agent's grant NOT confer?"* — the "keeps Y
private" half of Decision D4's exposes-vs-keeps-private contract
(``SERVER-AND-COLLAB-DESIGN-REVIEW.md:120``). That answer is only true if it is computed from the
**same** policy that decides which tools ``build_server`` actually offers. Two lists that must
agree is how one of them drifts, and a consent object built on a drifted list tells an owner their
private memory is withheld when it is offered.

So the three withdrawal gates and the registered set live HERE, as plain frozensets over an
``McpSettings``, and :mod:`mu_client.mcp.server` withdraws through :func:`withdrawn_tool_names`
rather than restating them. Neither module imports the other's heavy half: nothing here imports
``FastMCP``, so the consent path costs no MCP server construction.

``REGISTERED_TOOL_NAMES`` is a DECLARATION and could in principle fall behind the ``@server.tool``
decorators. It cannot fall behind silently: ``tests/unit/test_mcp_surface_source_of_truth.py``
enumerates the real FastMCP tool manager and asserts equality under every flag combination, so a
tool added without a row here fails the suite.
"""

from __future__ import annotations

from mu_client.config import McpSettings

__all__ = [
    "AUTOMATIC_TOOL_NAMES",
    "HEALTH_TOOL_NAMES",
    "PIN_TOOL_NAMES",
    "REGISTERED_TOOL_NAMES",
    "offered_tool_names",
    "withdrawn_tool_names",
]

#: Verbs the HOOKS own (``mcp/server.py``'s "division of labour" rule). Withdrawn unless
#: ``MU_MCP__EXPOSE_AUTOMATIC_TOOLS=true``.
AUTOMATIC_TOOL_NAMES: frozenset[str] = frozenset({"add", "consolidate", "promote", "demote"})

#: memory-health (``memory-health-pinning-spec.md`` §7.1). Withdrawn unless
#: ``MU_MCP__EXPOSE_HEALTH_TOOL=true``.
HEALTH_TOOL_NAMES: frozenset[str] = frozenset({"health"})

#: The lifecycle OVERRIDE pair (§7.2). Withdrawn unless ``MU_MCP__EXPOSE_PIN_TOOLS=true``.
PIN_TOOL_NAMES: frozenset[str] = frozenset({"pin", "unpin"})

#: Every tool ``build_server`` registers, before any withdrawal. Kept in decorator order so a
#: reader can diff it against ``mcp/server.py`` by eye.
REGISTERED_TOOL_NAMES: frozenset[str] = frozenset(
    {
        "add",
        "recall",
        "get",
        "consolidate",
        "search",
        "build_context",
        "ask",
        "promote",
        "demote",
        "update",
        "delete",
        "health",
        "pin",
        "unpin",
    }
)


def withdrawn_tool_names(mcp: McpSettings) -> frozenset[str]:
    """The tools ``build_server`` deletes from the tool manager under ``mcp``.

    ``remove_tool`` deletes outright — the withdrawn names leave BOTH ``tools/list`` and
    ``tools/call`` — so "withdrawn" here means genuinely unreachable by the model, which is what
    lets :mod:`mu_client.consent.capabilities` treat the complement as this device's real surface.
    """
    withdrawn: set[str] = set()
    if not mcp.expose_automatic_tools:
        withdrawn |= AUTOMATIC_TOOL_NAMES
    if not mcp.expose_health_tool:
        withdrawn |= HEALTH_TOOL_NAMES
    if not mcp.expose_pin_tools:
        withdrawn |= PIN_TOOL_NAMES
    return frozenset(withdrawn)


def offered_tool_names(mcp: McpSettings) -> frozenset[str]:
    """What an agent host can actually call on this device under ``mcp``."""
    return REGISTERED_TOOL_NAMES - withdrawn_tool_names(mcp)
