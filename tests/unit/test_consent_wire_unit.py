"""**The wire shapes refuse what the client cannot represent honestly.**

``mu_client.consent.wire`` is a client-side redeclaration of models that live in the commercial
``mu-server`` repo (AD-116), and its module docstring already argues for the two deliberate
divergences it makes. These tests pin the three REFUSALS, each of which existed only as a sentence
in a docstring before:

* an **empty** ``capabilities`` set — the server's own comment (``consent/model.py:214``) says it
  reads as UNCAPPED to ``AgentBinding.assert_allows``, i.e. consent to EVERYTHING, and unbounded
  here it rendered as the *least* permissive grant in the vocabulary;
* a **naive** ``issued_at`` — read as LOCAL time by ``astimezone``, which silently changes which
  shares this device reports as withdrawn;
* a **prose** ``reason`` — the 64-character cap was annotated as the thing that kept prose off a
  content-free trust-ledger row, and 62 characters of conversation content fit inside it.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from mu_client.consent.wire import (
    NAMED_REASON_PATTERN,
    AgentShareGrantView,
    RevokeAgentShareBody,
    assert_named_reason,
)
from mu_client.errors import InvalidRevokeReasonError

pytestmark = pytest.mark.unit

_T0 = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)


def _grant_payload(**overrides: object) -> dict[str, object]:
    return {
        "grant_id": "agentshare_deadbeef",
        "agent_principal_id": "agt-claude",
        "room_id": "room-42",
        "granted_by": "prn-owner",
        "capabilities": ("room.participate",),
        "issued_at": _T0,
        "active": True,
    } | overrides


def test_an_empty_capability_set_is_refused_rather_than_rendered_as_the_safest_grant() -> None:
    """The most permissive grant in the vocabulary rendered as the least.

    With no floor, ``compute_exposure`` produced *"EXPOSES (this room): nothing this client
    recognises"* plus *"It CANNOT see your private memory"* and BOTH invariants held — over a set
    the server reads as consent to everything. Unreachable today (the server rejects empty), which
    is exactly why the floor is cheap: a grant this client cannot represent honestly is one it must
    refuse to represent at all.

    **MUTATION:** drop ``Field(min_length=1)`` from ``capabilities`` -> RED.
    """
    with pytest.raises(ValidationError):
        AgentShareGrantView.model_validate(_grant_payload(capabilities=()))
    assert AgentShareGrantView.model_validate(_grant_payload()).capabilities == (
        "room.participate",
    )


def test_a_naive_issued_at_is_refused_rather_than_read_as_local_time() -> None:
    """``AwareDatetime``, not ``datetime``.

    The consent path still survives the refusal: ``revoke`` treats an unreadable grant as a reason
    to widen the cut to BLANKET, never as a reason not to cut
    (``test_an_unreadable_grant_still_cuts_locally``).

    **MUTATION:** change ``issued_at: AwareDatetime`` back to ``datetime`` -> RED.
    """
    with pytest.raises(ValidationError):
        AgentShareGrantView.model_validate(_grant_payload(issued_at=_T0.replace(tzinfo=None)))


def test_a_revoke_reason_must_be_a_name_on_the_wire_body_too() -> None:
    """Defence in depth behind :func:`assert_named_reason`: the body a client PRESENTS is bounded
    by the same rule the edge refuses on, so no path can construct one out of prose.

    **MUTATION:** drop ``pattern=NAMED_REASON_PATTERN`` from ``RevokeAgentShareBody.reason`` -> RED.
    """
    with pytest.raises(ValidationError):
        RevokeAgentShareBody(
            agent_principal_id="agt-claude",
            reason="the user asked me to summarise their therapy notes",
        )
    assert RevokeAgentShareBody(agent_principal_id="agt-claude", reason=None).reason is None
    assert (
        RevokeAgentShareBody(agent_principal_id="agt-claude", reason="policy_change").reason
        == "policy_change"
    )


@pytest.mark.parametrize(
    "reason",
    [
        "the user asked me to summarise their therapy notes",  # prose that FITS in 64 chars
        "User_Revoked",  # a capital is not part of a name here
        "user revoked",
        "",
        "x" * 65,
        "1_leading_digit",
    ],
)
def test_assert_named_reason_refuses_everything_that_is_not_a_name(reason: str) -> None:
    """``trust-ledger-spec.md`` §2 rule 3: an entry carries *"only ids, content hashes, principal
    ids, enums, timestamps, and counts"*.

    **MUTATION:** make ``NAMED_REASON_PATTERN`` ``re.compile(r".*")`` -> RED.
    """
    with pytest.raises(InvalidRevokeReasonError):
        assert_named_reason(reason)


def test_assert_named_reason_accepts_a_name_and_the_absence_of_one() -> None:
    """A reason is OPTIONAL; refusing ``None`` would make the refusal itself a bug.

    **MUTATION:** drop the ``if reason is None: return`` early exit -> RED.
    """
    assert_named_reason(None)
    assert_named_reason("user_revoked")
    assert_named_reason("policy_change")
    assert NAMED_REASON_PATTERN.match("user_revoked") is not None
