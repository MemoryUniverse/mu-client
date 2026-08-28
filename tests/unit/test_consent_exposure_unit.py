"""**Decision D4's privacy sentence is EARNED, not printed.**

``SERVER-AND-COLLAB-DESIGN-REVIEW.md:120`` hands the client one sentence to render — *"This agent
can read/write in this room. It cannot see your private memory. Your commands to it are visible to
everyone in the room."* D4 chose a first-class client consent object over the server's bind/unbind
precisely so an owner can SEE what a share exposes. A sentence printed unconditionally carries no
information about the grant in front of the owner; these tests exist to prove this one is derived
from it, and that it CHANGES when the derivation says it would otherwise be false.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from mu_client.config import McpSettings
from mu_client.consent.capabilities import local_capability_name
from mu_client.consent.exposure import ExposureInvariant, compute_exposure
from mu_client.consent.wire import AgentShareGrantView

pytestmark = pytest.mark.unit

_NOW = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)


def _grant(*capabilities: str, active: bool = True) -> AgentShareGrantView:
    return AgentShareGrantView(
        grant_id="agentshare_deadbeef",
        agent_principal_id="agt-claude",
        room_id="room-42",
        granted_by="prn-owner",
        capabilities=capabilities,
        issued_at=_NOW,
        active=active,
    )


# ==================================================================================================
# The privacy invariant — the one D4's sentence rests on
# ==================================================================================================
def test_a_room_only_grant_holds_the_privacy_invariant_and_withholds_every_local_verb() -> None:
    """The normal case: the grant confers room participation and NOTHING local.

    **MUTATION:** in ``compute_exposure``, compute ``exposed_local`` as ``()`` unconditionally ->
    still green here (this grant has no local capability), which is why the broken-invariant test
    below is the load-bearing one. **MUTATION that DOES bite here:** compute ``withheld_local`` as
    ``()`` -> RED (the owner is told nothing is kept private).
    """
    mcp = McpSettings()
    contract = compute_exposure(_grant("room.participate"), mcp=mcp, locally_revoked=False)

    assert [c.name for c in contract.exposed_shared] == ["room.participate"]
    assert contract.exposed_local == ()
    assert ExposureInvariant.NO_LOCAL_CAPABILITY_EXPOSED in contract.invariants_held
    assert contract.invariants_broken == ()
    # Every offered local verb is named as withheld — the "Y" is enumerated, not summarised.
    assert {c.name for c in contract.withheld_local} == {
        local_capability_name(t)
        for t in ("recall", "search", "get", "build_context", "ask", "update", "delete")
    }


def test_a_grant_naming_a_local_capability_breaks_the_invariant_and_changes_the_sentence() -> None:
    """**The load-bearing test of this whole package.**

    A grant conferring ``memory.local.recall`` means the shared agent CAN read the owner's private
    memory. D4's sentence would then be a lie, and the object must say the opposite thing.

    **MUTATION:** in ``compute_exposure``, replace the ``exposed_local`` comprehension's
    ``CapabilityPlane.LOCAL`` with ``CapabilityPlane.SHARED`` -> RED: the invariant reports HELD,
    ``render()`` prints "It CANNOT see your private memory", and the client tells an owner their
    private memory is safe while the grant reads it. VERIFIED RED.
    """
    contract = compute_exposure(
        _grant("room.participate", local_capability_name("recall")),
        mcp=McpSettings(),
        locally_revoked=False,
    )

    assert [c.name for c in contract.exposed_local] == [local_capability_name("recall")]
    assert ExposureInvariant.NO_LOCAL_CAPABILITY_EXPOSED in contract.invariants_broken
    assert ExposureInvariant.NO_LOCAL_CAPABILITY_EXPOSED not in contract.invariants_held

    rendered = "\n".join(contract.render())
    assert "EXPOSES YOUR PRIVATE MEMORY" in rendered
    assert "CANNOT see your private memory" not in rendered
    # And the capability it exposes is no longer claimed as withheld.
    assert local_capability_name("recall") not in {c.name for c in contract.withheld_local}


def test_the_privacy_sentence_is_rendered_only_when_the_invariant_holds() -> None:
    """The render branch is keyed on the COMPUTED invariant, not on a constant.

    **MUTATION:** in ``AgentExposureContract.render``, change the branch to
    ``if True:`` -> RED (the private-memory-exposing grant above renders the reassurance).
    """
    safe = compute_exposure(_grant("room.participate"), mcp=McpSettings(), locally_revoked=False)
    unsafe = compute_exposure(
        _grant(local_capability_name("delete")), mcp=McpSettings(), locally_revoked=False
    )
    assert any("CANNOT see your private memory" in line for line in safe.render())
    assert not any("CANNOT see your private memory" in line for line in unsafe.render())


# ==================================================================================================
# The recognition invariant — a permission we cannot explain is not silently dropped
# ==================================================================================================
def test_an_unknown_capability_is_reported_not_dropped() -> None:
    """A grant naming something this client has no vocabulary for makes the contract INCOMPLETE,
    and it says so.

    Dropping it would understate the exposure — the same class of error as claiming privacy that is
    not there.

    **MUTATION:** in ``compute_exposure``, set ``unrecognised = ()`` -> RED (the invariant reports
    held and the render loses its warning line). VERIFIED RED.
    """
    contract = compute_exposure(
        _grant("room.participate", "vendor.experimental.thing"),
        mcp=McpSettings(),
        locally_revoked=False,
    )
    assert contract.unrecognised == ("vendor.experimental.thing",)
    assert ExposureInvariant.EVERY_GRANTED_CAPABILITY_RECOGNISED in contract.invariants_broken
    assert any("INCOMPLETE" in line for line in contract.render())


# ==================================================================================================
# Fail-closed liveness — a local cut beats a server that still says ACTIVE
# ==================================================================================================
def test_a_local_revocation_beats_a_server_that_still_reports_active() -> None:
    """``effectively_live`` is ``server_active AND NOT locally_revoked`` — fail-closed.

    This is what makes a revoke whose network leg failed still mean something on this device: the
    tombstone is written first, so the client never presents that share as live again even while
    the server's own record is untouched.

    **MUTATION:** change ``effectively_live`` to ``return self.server_active`` -> RED. VERIFIED RED.
    """
    contract = compute_exposure(
        _grant("room.participate", active=True), mcp=McpSettings(), locally_revoked=True
    )
    assert contract.server_active is True
    assert contract.locally_revoked is True
    assert contract.effectively_live is False
    assert any("WITHDRAWN" in line for line in contract.render())


def test_a_live_unrevoked_grant_is_effectively_live() -> None:
    """The negative control for the test above: fail-closed must not mean always-closed.

    **MUTATION:** change ``effectively_live`` to ``return False`` -> RED.
    """
    contract = compute_exposure(
        _grant("room.participate", active=True), mcp=McpSettings(), locally_revoked=False
    )
    assert contract.effectively_live is True


# ==================================================================================================
# The contract tracks this device, and is content-free
# ==================================================================================================
def test_the_withheld_set_follows_this_device_s_configuration() -> None:
    """Exposing ``pin``/``unpin`` on the MCP surface adds them to what the grant keeps private.

    **MUTATION:** pass ``McpSettings()`` instead of ``mcp`` inside ``compute_exposure`` -> RED.
    """
    grant = _grant("room.participate")
    default = compute_exposure(grant, mcp=McpSettings(), locally_revoked=False)
    with_pins = compute_exposure(
        grant, mcp=McpSettings(expose_pin_tools=True), locally_revoked=False
    )
    assert {c.name for c in with_pins.withheld_local} - {
        c.name for c in default.withheld_local
    } == {local_capability_name("pin"), local_capability_name("unpin")}


def test_the_rendered_contract_carries_no_free_text_from_the_grant() -> None:
    """Content-free (CANONICAL §3 / project rule 3).

    Every rendered line is built from ids, capability names and fixed English. The only values that
    can vary are ids and capability names — never memory content, because this object never touches
    a store. Pinned by feeding a grant whose capability name is hostile and asserting it appears
    ONLY inside the explicit "cannot explain" warning.

    **MUTATION:** interpolate a grant field into a disclosure line -> this stays green (weak), so
    the assertion below is the concrete one: the unrecognised name must not leak into the
    exposes/keeps-private lists it was never resolved into.
    """
    hostile = "a" * 120
    contract = compute_exposure(_grant(hostile), mcp=McpSettings(), locally_revoked=False)
    rendered = contract.render()
    assert sum(hostile in line for line in rendered) == 1
    assert all(hostile not in c.name for c in contract.withheld_local)


# ==================================================================================================
# THE FALSE PRIVACY SENTENCE — the two ways it was printed over a grant that reaches private memory
# ==================================================================================================
def test_a_grant_naming_a_withdrawn_local_tool_still_breaks_the_privacy_invariant() -> None:
    """**The default configuration shipped the lie.**

    ``expose_automatic_tools`` / ``expose_health_tool`` / ``expose_pin_tools`` all default ``False``
    (``config.py:245,286,290``), so 7 of the 14 registered tools are withdrawn. While the plane was
    decided by membership in the OFFERED set, a grant naming ``memory.local.add`` — *"write a new
    memory into your private store"* — fell out of the vocabulary entirely: ``exposed_local`` stayed
    empty, ``NO_LOCAL_CAPABILITY_EXPOSED`` reported HELD, and ``render()`` printed the denial and
    the incompleteness warning side by side.

    The server makes this reachable rather than theoretical: ``assert_consentable_capabilities``
    (``mu-server/src/mu_server/consent/model.py:159-182``) imposes LENGTH only — no vocabulary, no
    prefix check — which is what AD-120 proved end-to-end for ``memory.local.recall``. Only the name
    differs here, and this name is one the default build has switched off.

    **MUTATION:** in ``compute_exposure``, drop the ``is_local_capability_name`` arm (restore the
    known-set-only classification) -> RED.
    **MUTATION:** in ``all_local_capabilities``, iterate ``offered_tool_names(mcp)`` -> RED.
    """
    mcp = McpSettings()
    contract = compute_exposure(
        _grant("room.participate", local_capability_name("add")), mcp=mcp, locally_revoked=False
    )
    assert [c.name for c in contract.exposed_local] == [local_capability_name("add")]
    assert ExposureInvariant.NO_LOCAL_CAPABILITY_EXPOSED in contract.invariants_broken

    rendered = "\n".join(contract.render())
    assert "CANNOT see your private memory" not in rendered
    assert "EXPOSES YOUR PRIVATE MEMORY: memory.local.add" in rendered
    # ...and the honest nuance: conferred, but not exercisable through THIS host today.
    assert "does not offer that tool today" in rendered


def test_a_local_namespace_name_this_build_cannot_explain_is_still_classified_local() -> None:
    """A ``memory.local.*`` verb from a newer client is a private-memory permission regardless.

    The server stores ``allowed_tools`` verbatim (AD-120), so such a name really can arrive. The
    contract reports it BOTH ways — exposed on the local plane, and unrecognised — and neither fact
    is used to suppress the other.

    **MUTATION:** in ``compute_exposure``, ``continue`` on an unknown name without the namespace
    check -> RED.
    """
    contract = compute_exposure(
        _grant("room.participate", "memory.local.recall_v2"),
        mcp=McpSettings(),
        locally_revoked=False,
    )
    assert [c.name for c in contract.exposed_local] == ["memory.local.recall_v2"]
    assert contract.unrecognised == ("memory.local.recall_v2",)
    assert ExposureInvariant.NO_LOCAL_CAPABILITY_EXPOSED in contract.invariants_broken
    assert ExposureInvariant.EVERY_GRANTED_CAPABILITY_RECOGNISED in contract.invariants_broken
    assert "CANNOT see your private memory" not in "\n".join(contract.render())


def test_the_privacy_sentence_needs_both_invariants_not_one() -> None:
    """The denial and the admission of incompleteness were printed together.

    ``NO_LOCAL_CAPABILITY_EXPOSED`` alone says *"nothing I RECOGNISE as local is granted"*. If a
    name is unrecognised this client does not know what plane it acts on, so denying private-memory
    exposure claims knowledge it has just said it lacks — the exact error
    ``EVERY_GRANTED_CAPABILITY_RECOGNISED``'s own docstring calls *"the same class of error as the
    first"*.

    **MUTATION:** in ``_private_memory_lines``, drop the ``EVERY_GRANTED_CAPABILITY_RECOGNISED``
    conjunct -> RED.
    """
    contract = compute_exposure(
        _grant("room.participate", "vendor.unknown.thing"), mcp=McpSettings(), locally_revoked=False
    )
    # Nothing is classifiable as local — the honest answer is "I cannot say", not "it cannot".
    assert contract.exposed_local == ()
    rendered = "\n".join(contract.render())
    assert "CANNOT see your private memory" not in rendered
    assert "CANNOT SAY whether this share reaches your private memory" in rendered
    assert "INCOMPLETE" in rendered


# ==================================================================================================
# The SURVIVOR of a failed revoke — the server-side grant, on the PERSISTENT screen
# ==================================================================================================
def test_a_local_cut_the_server_never_confirmed_warns_that_the_agent_can_still_act() -> None:
    """``SERVER_REVOKE_NOT_CONFIRMED`` lived only on the revoke's transient receipt.

    Close the terminal and the fact was unrecoverable through any client surface, while the durable
    row one column away recorded it. The owner returning to the persistent affordance was told
    "WITHDRAWN" over a grant the server still reports ACTIVE — i.e. an agent that can still read and
    write in the room.

    **MUTATION:** in ``_survivor_lines``, return ``()`` unconditionally -> RED.
    **MUTATION:** in ``_survivor_lines``, branch on ``self.local_cut_server_confirmed is True``
    instead of ``is False`` -> RED (the strong warning is lost).
    """
    contract = compute_exposure(
        _grant("room.participate"),
        mcp=McpSettings(),
        locally_revoked=True,
        local_cut_server_confirmed=False,
    )
    assert contract.effectively_live is False
    rendered = "\n".join(contract.render())
    assert "THE SERVER NEVER CONFIRMED YOUR REVOKE" in rendered
    assert "the agent can still act in this room" in rendered


def test_a_confirmed_cut_over_a_still_active_grant_still_says_the_agent_can_act() -> None:
    """A confirmed revoke plus a server that still reports ACTIVE is a DIFFERENT live grant, not a
    reason for silence. ``server_active`` is the authority on what the agent can do.

    **MUTATION:** in ``_survivor_lines``, guard the whole block on
    ``self.local_cut_server_confirmed is False`` -> RED.
    """
    rendered = "\n".join(
        compute_exposure(
            _grant("room.participate"),
            mcp=McpSettings(),
            locally_revoked=True,
            local_cut_server_confirmed=True,
        ).render()
    )
    assert "still reports this share ACTIVE" in rendered
    assert "NEVER CONFIRMED" not in rendered


def test_a_blanket_cut_that_does_not_cover_this_grant_is_reported_as_ambiguous() -> None:
    """Two very different things produce it and this device cannot tell them apart.

    Either the owner deliberately re-shared (the case the ``issued_at`` comparison exists to
    protect), or this laptop's clock is behind the server's — which is likeliest exactly when a
    blanket cut is written, because that only happens when the server was unreachable. Silently
    rendering the share as live picked one. Saying so picks neither.

    **MUTATION:** in ``render``, drop the ``uncovering_blanket_cut_at`` branch -> RED.
    """
    rendered = "\n".join(
        compute_exposure(
            _grant("room.participate"),
            mcp=McpSettings(),
            locally_revoked=False,
            uncovering_blanket_cut_at=_NOW - timedelta(minutes=1),
        ).render()
    )
    assert "recorded a withdrawal for this agent at" in rendered
    assert "this device's clock disagrees with the server's" in rendered


def test_the_liveness_judgement_survives_model_dump() -> None:
    """The daemon's IPC payload must carry the JUDGEMENT, not just its inputs.

    ``effectively_live``'s docstring makes it the one place the fail-closed resolution is made; as a
    bare ``@property`` ``model_dump`` dropped it and every IPC consumer had to re-derive it, which
    is the drift the docstring forbids.

    **MUTATION:** remove ``@computed_field`` from ``effectively_live`` -> RED.
    """
    dumped = compute_exposure(
        _grant("room.participate"), mcp=McpSettings(), locally_revoked=True
    ).model_dump(mode="json")
    assert dumped["effectively_live"] is False
