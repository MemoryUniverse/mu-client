"""``mu_client.mcp.surface`` must describe the surface ``build_server`` ACTUALLY builds.

This file is the reason :mod:`mu_client.consent.capabilities` may be trusted. Decision D4's
exposes-vs-keeps-private contract answers *"what does this device keep private?"* by subtracting the
grant's capabilities from this device's own capability set, and that set is derived from
:func:`mu_client.mcp.surface.offered_tool_names`. If the declaration drifts from the real FastMCP
tool manager — a tool added to ``server.py`` with no row in ``surface.py`` — the contract would tell
an owner a capability is withheld when it is offered. That is a privacy misstatement, so it is
pinned here against the real server rather than against a second list.
"""

from __future__ import annotations

import itertools

import pytest

from mu_client.config import ClientSettings, McpSettings
from mu_client.mcp.server import build_server
from mu_client.mcp.surface import (
    AUTOMATIC_TOOL_NAMES,
    HEALTH_TOOL_NAMES,
    PIN_TOOL_NAMES,
    REGISTERED_TOOL_NAMES,
    offered_tool_names,
    withdrawn_tool_names,
)

pytestmark = pytest.mark.unit

#: Every flag combination. Eight, not one: the whole point of three INDEPENDENT gates is that they
#: compose, and a declaration that happened to be right only on the default surface would be a
#: declaration that is wrong the moment an owner flips a flag.
_ALL_FLAG_COMBOS = tuple(itertools.product((False, True), repeat=3))


def _settings(automatic: bool, health: bool, pin: bool) -> ClientSettings:
    return ClientSettings(
        mcp=McpSettings(
            expose_automatic_tools=automatic, expose_health_tool=health, expose_pin_tools=pin
        )
    )


async def test_registered_tool_names_is_exactly_what_the_server_registers() -> None:
    """``REGISTERED_TOOL_NAMES`` == every tool ``build_server`` registers before withdrawal.

    Read with every gate OPEN, so nothing is withdrawn and the raw registration set is visible.

    **MUTATION:** drop ``"ask"`` from ``REGISTERED_TOOL_NAMES`` -> RED (``ask`` appears in the real
    manager and not in the declaration). Adding a spurious ``"nonexistent"`` -> RED the other way.
    """
    server = build_server(settings=_settings(True, True, True))
    real = {tool.name for tool in await server.list_tools()}
    assert real == set(REGISTERED_TOOL_NAMES)


@pytest.mark.parametrize(("automatic", "health", "pin"), _ALL_FLAG_COMBOS)
async def test_offered_tool_names_matches_the_real_manager_under_every_flag_combination(
    automatic: bool, health: bool, pin: bool
) -> None:
    """The declared OFFERED set equals what a model can really call, for all eight configurations.

    ⚠ **What this test can and CANNOT catch, stated because the first version of it over-claimed.**
    ``build_server`` now withdraws through :func:`withdrawn_tool_names`, so both sides of this
    equality read the SAME gate: mutating the gate moves both and this assertion stays green
    (MEASURED — ``if not mcp.expose_pin_tools`` -> ``if False`` left all 18 tests passing). What it
    genuinely pins is drift in ``REGISTERED_TOOL_NAMES``, which the server does NOT read — dropping
    ``"ask"`` from it takes nine of these RED.

    The gate's BEHAVIOUR is pinned independently by
    :func:`test_each_gate_actually_withdraws_its_own_tools` below, which asserts against the real
    manager without consulting the gate at all. Both are needed; neither is sufficient.
    """
    settings = _settings(automatic, health, pin)
    server = build_server(settings=settings)
    real = {tool.name for tool in await server.list_tools()}
    assert real == set(offered_tool_names(settings.mcp))


@pytest.mark.parametrize(("automatic", "health", "pin"), _ALL_FLAG_COMBOS)
def test_offered_and_withdrawn_partition_the_registered_set(
    automatic: bool, health: bool, pin: bool
) -> None:
    """No tool may be both offered and withdrawn, and none may be neither.

    A gap here would let a capability fall out of BOTH the "exposes" and the "keeps private" halves
    of the D4 contract — invisible in the consent screen either way.

    **MUTATION:** make ``offered_tool_names`` return ``REGISTERED_TOOL_NAMES`` unconditionally ->
    RED (the two sets overlap on every combination that withdraws anything).
    """
    mcp = _settings(automatic, health, pin).mcp
    offered, withdrawn = offered_tool_names(mcp), withdrawn_tool_names(mcp)
    assert offered | withdrawn == REGISTERED_TOOL_NAMES
    assert offered & withdrawn == frozenset()


def test_every_gated_name_is_a_registered_name() -> None:
    """A gate naming a tool that does not exist would silently withdraw nothing.

    **MUTATION:** add ``"promote_all"`` to ``AUTOMATIC_TOOL_NAMES`` -> RED.
    """
    gated = AUTOMATIC_TOOL_NAMES | HEALTH_TOOL_NAMES | PIN_TOOL_NAMES
    assert gated <= REGISTERED_TOOL_NAMES


@pytest.mark.parametrize(
    ("flags", "gated_names"),
    [
        ((False, True, True), AUTOMATIC_TOOL_NAMES),
        ((True, False, True), HEALTH_TOOL_NAMES),
        ((True, True, False), PIN_TOOL_NAMES),
    ],
)
async def test_each_gate_actually_withdraws_its_own_tools(
    flags: tuple[bool, bool, bool], gated_names: frozenset[str]
) -> None:
    """Each gate OFF removes exactly its own tools from the REAL manager, and nothing else.

    This is the assertion that does not read the gate it is testing: the expected sets are the
    literal ``REGISTERED_TOOL_NAMES`` minus the named group, compared against what FastMCP holds.

    **MUTATION:** in ``withdrawn_tool_names``, replace ``if not mcp.expose_pin_tools`` with
    ``if False`` -> RED on the ``PIN_TOOL_NAMES`` case (``pin``/``unpin`` are still callable with
    the flag off). Same for either other gate. VERIFIED RED for all three.
    """
    server = build_server(settings=_settings(*flags))
    real = {tool.name for tool in await server.list_tools()}
    assert real == set(REGISTERED_TOOL_NAMES - gated_names)
