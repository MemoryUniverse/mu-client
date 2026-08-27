"""The agent-facing MCP surface — hooks own the routine path, MCP is the deep-dive escape hatch.

This is a PRODUCT RULE, not a style preference, so it is asserted rather than documented: capture,
tier promotion/consolidation and context injection all happen automatically (capture hooks +
`lifecycle/session_save.py`). An agent that CAN call `add` will re-store what the hook already
captured; one that CAN call `promote`/`consolidate` will second-guess a lifecycle it has no
visibility into. Steering a model with prose is unreliable — not offering the tool is not.
"""

from __future__ import annotations

import pytest

from mu_client.config import ClientSettings
from mu_client.mcp.server import build_server

pytestmark = pytest.mark.unit

#: The verbs the HOOKS own. None of these may appear on the default agent-facing surface.
_HOOK_OWNED = {"add", "consolidate", "promote", "demote"}

#: The DELIBERATE deep-dive set: reaching deeper than auto-injected context, plus the two verbs
#: that encode real user intent hooks cannot infer (update/delete).
_DEEP_DIVE = {"recall", "search", "get", "build_context", "ask", "update", "delete"}


async def _tool_names(settings: ClientSettings | None = None) -> set[str]:
    server = build_server(settings=settings)
    return {t.name for t in await server.list_tools()}


async def test_hook_owned_verbs_are_not_offered_to_the_agent_by_default() -> None:
    names = await _tool_names()
    leaked = _HOOK_OWNED & names
    assert leaked == set(), (
        f"{sorted(leaked)} are done automatically by hooks/daemon — offering them to the model "
        "invites duplicate writes and a second capture policy competing with the real one"
    )


async def test_the_deep_dive_escape_hatch_is_still_available() -> None:
    """Withdrawing the automatic verbs must NOT leave the agent unable to go deeper: the whole
    point is that MCP remains available for exactly the case where injected context is not enough.
    """
    assert _DEEP_DIVE <= await _tool_names()


async def test_the_default_surface_is_exactly_the_deep_dive_set() -> None:
    assert await _tool_names() == _DEEP_DIVE


async def test_the_automatic_verbs_can_be_re_exposed_explicitly() -> None:
    """The escape hatch for a host with NO hooks installed (where nothing else would ever capture),
    for a headless SDK-style caller, and for debugging."""
    names = await _tool_names(ClientSettings(mcp={"expose_automatic_tools": True}))
    assert _HOOK_OWNED <= names
    assert _DEEP_DIVE <= names


async def test_instructions_tell_the_agent_memory_is_automatic() -> None:
    """The tool list enforces the rule; the instructions must also EXPLAIN it, or a model will keep
    looking for a 'remember' tool and treat its absence as a missing capability."""
    server = build_server()
    instructions = (server.instructions or "").lower()
    assert "automatic" in instructions
    assert "hook" in instructions
