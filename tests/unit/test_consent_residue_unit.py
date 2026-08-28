"""Every residue name is explained, and an unknown one is never dropped.

``mu-server/src/mu_server/routes/rooms.py:912-916`` states this as a client obligation: *"a client
that renders 'revoked everywhere' from a ``PARTIAL`` receipt is misreporting it."* The mechanism
that makes the misreport impossible is that nothing is silently discarded on the way from the
server's closed vocabulary to the owner's screen.
"""

from __future__ import annotations

import pytest

from mu_client.consent.residue import ClientCascadeResidue, explain, explain_all
from mu_client.consent.wire import ServerCascadeResidue

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("member", list(ServerCascadeResidue))
def test_every_server_residue_member_has_a_recognised_explanation(
    member: ServerCascadeResidue,
) -> None:
    """Full coverage of ``cascade_names.py``'s nine members.

    A gap means a real server residue renders as "this client does not recognise it" — technically
    honest, practically useless, and a signal the mirror has fallen behind.

    **MUTATION:** delete the ``REVOKE_ACK_NOT_INTAKEN`` row from ``_SERVER_TEXT`` -> RED.
    """
    explanation = explain(member.value)
    assert explanation.recognised is True
    assert explanation.name == member.value
    assert explanation.text


@pytest.mark.parametrize("member", list(ClientCascadeResidue))
def test_every_client_residue_member_has_a_recognised_explanation(
    member: ClientCascadeResidue,
) -> None:
    """**MUTATION:** delete the ``CONTROL_FRAME_NOT_CONSUMED`` row from ``_CLIENT_TEXT`` -> RED."""
    explanation = explain(member.value)
    assert explanation.recognised is True
    assert explanation.text


def test_an_unknown_residue_is_surfaced_as_unrecognised_and_not_reached() -> None:
    """A newer server naming something this client has no sentence for must still be shown.

    **MUTATION:** make ``explain`` return ``None`` for an unknown name and have ``explain_all``
    skip ``None`` -> RED (the name vanishes and a partial revoke reads as clean). VERIFIED RED.
    """
    explanation = explain("some_future_residue")
    assert explanation.recognised is False
    assert explanation.by_design is False
    assert explanation.name == "some_future_residue"
    assert "NOT reached" in explanation.text


def test_only_the_by_design_members_are_marked_by_design() -> None:
    """``POSTED_AGENT_RESULTS_RETAINED`` is a design decision; everything else is a gap.

    Getting this backwards would let :attr:`ClientRevocationOutcome.fully_settled` report a settled
    revoke while real gaps stood open.

    **MUTATION:** flip ``CONTROL_FRAME_NOT_DURABLE``'s first tuple element to ``True`` -> RED.
    """
    by_design = {
        member.value
        for member in (*ServerCascadeResidue, *ClientCascadeResidue)
        if explain(member.value).by_design
    }
    assert by_design == {"posted_agent_results_retained"}


def test_explain_all_preserves_order_and_deduplicates() -> None:
    """The client's own residue must not be duplicated by the server naming the same fact.

    ``POSTED_AGENT_RESULTS_RETAINED`` exists in BOTH vocabularies with the same value, so a merged
    list would show it twice without this.

    **MUTATION:** drop the ``seen`` set from ``explain_all`` -> RED.
    """
    merged = explain_all(
        (
            ClientCascadeResidue.SERVER_REVOKE_NOT_CONFIRMED.value,
            ClientCascadeResidue.POSTED_AGENT_RESULTS_RETAINED.value,
            ServerCascadeResidue.POSTED_AGENT_RESULTS_RETAINED.value,
        )
    )
    assert [e.name for e in merged] == [
        "server_revoke_not_confirmed",
        "posted_agent_results_retained",
    ]


def test_the_client_vocabulary_covers_the_four_daemon_leg_obligations() -> None:
    """CANONICAL §4.3-B5/B7 + X10 give the daemon four duties this build cannot discharge; each is
    named rather than left silent.

    **MUTATION:** delete ``REVOKE_ACK_NOT_EMITTED`` from ``ClientCascadeResidue`` -> RED.
    """
    assert {member.value for member in ClientCascadeResidue} >= {
        "local_subprocess_not_driven_here",  # §4.3-B5 — no HostBridgePort here
        "revoke_ack_not_emitted",  # §4.3-B7 — no ack is sent
        "control_frame_not_consumed",  # §4.2 — no Centrifugo subscription (AD-35)
        "warm_cache_not_grant_scoped",  # X10 — no grant-scoped purge primitive
    }
