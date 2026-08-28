"""The capability vocabulary is COMPLETE and tracks the real surface (D4's "keeps Y private")."""

from __future__ import annotations

import pytest

from mu_client.config import McpSettings
from mu_client.consent.capabilities import (
    LOCAL_CAPABILITY_PREFIX,
    SHARED_CAPABILITIES,
    TOOL_SUMMARIES,
    CapabilityPlane,
    all_local_capabilities,
    is_local_capability_name,
    known_capabilities,
    local_capabilities,
    local_capability_name,
    unexplained_local_capability,
)
from mu_client.mcp.surface import REGISTERED_TOOL_NAMES, offered_tool_names

pytestmark = pytest.mark.unit


def test_every_registered_tool_has_a_summary_and_no_summary_is_orphaned() -> None:
    """Exact coverage, both directions.

    A MISSING row silently drops a capability out of the "keeps private" list — the owner is told
    less is exposed than really is. A STALE row invents a capability this device does not have —
    the owner is told something is withheld that was never there. Both are misstatements, so the
    assertion is equality and not containment.

    **MUTATION:** delete the ``"ask"`` row from ``TOOL_SUMMARIES`` -> RED.
    **MUTATION:** add ``"teleport": "..."`` to ``TOOL_SUMMARIES`` -> RED.
    """
    assert set(TOOL_SUMMARIES) == set(REGISTERED_TOOL_NAMES)


def test_local_capabilities_are_exactly_the_offered_tools_under_the_local_plane() -> None:
    """One capability per callable tool, all LOCAL, all dotted.

    **MUTATION:** in ``local_capabilities``, iterate ``REGISTERED_TOOL_NAMES`` instead of
    ``offered_tool_names(mcp)`` -> RED (withdrawn tools appear as capabilities this device has).
    """
    mcp = McpSettings()
    capabilities = local_capabilities(mcp)
    assert {c.name for c in capabilities} == {
        local_capability_name(t) for t in offered_tool_names(mcp)
    }
    assert all(c.plane is CapabilityPlane.LOCAL for c in capabilities)
    assert all(c.name.startswith(LOCAL_CAPABILITY_PREFIX) for c in capabilities)


def test_turning_a_flag_on_grows_the_capability_set() -> None:
    """The contract tracks CONFIGURATION, not a snapshot taken at import time.

    An owner who exposes ``pin``/``unpin`` to their agent host has genuinely given this device two
    more things it can do, and the consent screen must say so on the next read.

    **MUTATION:** hardcode ``local_capabilities`` to ignore ``mcp`` (e.g. always pass
    ``McpSettings()``) -> RED.
    """
    default = {c.name for c in local_capabilities(McpSettings())}
    with_pins = {c.name for c in local_capabilities(McpSettings(expose_pin_tools=True))}
    assert with_pins - default == {
        local_capability_name("pin"),
        local_capability_name("unpin"),
    }


def test_the_shared_vocabulary_is_the_one_name_that_exists_in_the_system() -> None:
    """``room.participate`` and nothing invented alongside it.

    Verified against ``mu-server/src/mu_server/agents/bridge.py:140`` and
    ``routes/rooms.py:322``; a grep for other ``room.*`` capability literals across mu-server and
    mu-core returns none. Listing a second, unverified name would make ``unrecognised``
    under-report, which is the failure this vocabulary exists to prevent.

    **MUTATION:** add a ``room.moderate`` entry to ``SHARED_CAPABILITIES`` -> RED.
    """
    assert {c.name for c in SHARED_CAPABILITIES} == {"room.participate"}
    assert all(c.plane is CapabilityPlane.SHARED for c in SHARED_CAPABILITIES)


def test_known_capabilities_spans_both_planes_and_no_name_changes_plane() -> None:
    """The two vocabularies live in one dict, and **a local name must never resolve as SHARED.**

    This is the hazard, stated precisely, because the first version of this test named one that
    cannot happen (changing ``LOCAL_CAPABILITY_PREFIX`` to ``"room."`` yields ``room.recall`` etc.,
    which collide with nothing — MEASURED green). The real danger is a SHARED entry whose name
    equals a local capability's: ``known_capabilities`` builds one dict, so such an entry would
    overwrite the local row, and every plane check downstream — including the one D4's privacy
    sentence rests on — would then classify a private-memory verb as room-only.

    ⚠ **AMENDED**: ``known_capabilities`` now spans EVERY registered local tool, offered or
    withdrawn, so the membership assertion is against ``all_local_capabilities`` rather than
    ``local_capabilities``. That widening is the fix for a false privacy sentence and is pinned by
    ``test_a_withdrawn_tool_is_still_explainable_and_still_local`` below; the hazard THIS test is
    about — a shared name shadowing a local one — is unchanged and still asserted first.

    **MUTATION:** prepend ``Capability(name="memory.local.recall", plane=CapabilityPlane.SHARED,
    surface="room", summary="x")`` to ``SHARED_CAPABILITIES`` -> RED. VERIFIED RED.
    """
    mcp = McpSettings()
    known = known_capabilities(mcp)
    all_local = {c.name for c in all_local_capabilities(mcp)}
    shared = {c.name for c in SHARED_CAPABILITIES}
    assert all_local & shared == set()
    assert set(known) == all_local | shared
    assert len(known) == len(all_local) + len(shared)
    for name in all_local:
        assert known[name].plane is CapabilityPlane.LOCAL


def test_a_withdrawn_tool_is_still_explainable_and_still_local() -> None:
    """**The classification bug, at its source.**

    With the DEFAULT configuration seven of the fourteen registered tools are withdrawn. Deciding
    a capability's PLANE from the offered set therefore dropped ``memory.local.add`` out of the
    vocabulary entirely, which made ``exposed_local`` empty and let the consent screen print *"It
    CANNOT see your private memory"* over a grant conferring a write into it.

    A withdrawn tool must stay LOCAL-plane and stay EXPLAINABLE; only ``offered_here`` changes.

    **MUTATION:** iterate ``offered_tool_names(mcp)`` instead of ``REGISTERED_TOOL_NAMES`` in
    ``all_local_capabilities`` -> RED.
    """
    mcp = McpSettings()
    assert "add" not in offered_tool_names(mcp), "precondition: `add` is withdrawn by default"

    withdrawn = known_capabilities(mcp)[local_capability_name("add")]
    assert withdrawn.plane is CapabilityPlane.LOCAL
    assert withdrawn.offered_here is False
    # ...and it is NOT in the "keeps private" set, because this device cannot actually do it.
    assert local_capability_name("add") not in {c.name for c in local_capabilities(mcp)}


def test_the_plane_is_decided_by_namespace_not_by_the_offered_set() -> None:
    """``is_local_capability_name`` classifies a name this build has never heard of.

    A newer client, another host, or a later build can mint ``memory.local.<verb>`` names this one
    has no row for — the server stores ``allowed_tools`` verbatim with no vocabulary (AD-120), so
    such a name really can arrive. It is a private-memory permission whatever else it is.

    **MUTATION:** make ``is_local_capability_name`` return ``name in known_capabilities(...)`` (the
    old rule) -> RED.
    """
    assert is_local_capability_name("memory.local.recall_v2") is True
    assert is_local_capability_name(local_capability_name("add")) is True
    assert is_local_capability_name("room.participate") is False
    assert is_local_capability_name("vendor.unknown.thing") is False

    unexplained = unexplained_local_capability("memory.local.recall_v2")
    assert unexplained.plane is CapabilityPlane.LOCAL
    assert unexplained.offered_here is None
