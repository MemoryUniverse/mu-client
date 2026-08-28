"""The D4 agent-share wire shapes, **declared here because ``mu-core`` has no home for them.**

⚠ **READ THIS BEFORE ADDING A FIELD.** Every model below is a client-side redeclaration of a
pydantic model that lives inside the COMMERCIAL repo ``mu-server``. That is a boundary smell and it
is reported, not hidden — ``docs/tracking/ARCHITECTURE-DELTAS.md`` carries it, and the exact shapes
this file mirrors are:

===============================  ==========================================================
This module                      The shape it mirrors
===============================  ==========================================================
:class:`AgentShareGrantView`     ``mu-server/src/mu_server/routes/rooms.py:816-828``
:class:`RevokeAgentShareBody`    ``mu-server/src/mu_server/routes/rooms.py:872-886``
:class:`RevocationReceiptView`   ``mu-server/src/mu_server/consent/ledger.py:273-328``
:class:`ServerCascadeResidue`    ``mu-server/src/mu_server/consent/cascade_names.py:46-135``
===============================  ==========================================================

``/home/user/D/mu_project/CLAUDE.md``'s boundary rule says mu-client and mu-server *"talk only over
the versioned wire contracts in mu-core"*. These four are not in mu-core: ``RoomMessage``,
``Addressing``, ``MessageKind``, ``ParticipantKind``, ``GrantState`` and ``ClientScope`` all are,
but the D4 consent projection, the revoke body, the receipt and the residue vocabulary are not. So
this file is the **second unversioned copy** the boundary rule exists to prevent, and it carries the
citation for every field so a reviewer can diff it in one pass. The fix is in mu-core, not here.

--------------------------------------------------------------------------------------------
Two deliberate divergences from a naive mirror
--------------------------------------------------------------------------------------------
1. **``extra="ignore"``, not ``extra="forbid"``.** These are RESPONSES from a server that may be
   newer than this client. Forbidding an unknown field would turn a server-side additive change
   into a hard client failure on the consent path — the one path that must keep working so an owner
   can still revoke. Unknown fields are dropped; unknown *residue names* are NOT (see below).
2. **``unreachable`` stays ``tuple[str, ...]``, never ``tuple[ServerCascadeResidue, ...]``.**
   Parsing residue into a closed enum would make an unrecognised member either a validation error
   or a silent drop. Both are wrong for the exact reason the receipt exists: an unknown residue is
   still *something a revoke did not reach*. It is kept as a raw string and rendered as
   "unrecognised — treat as NOT reached" by :mod:`mu_client.consent.residue`.
"""

from __future__ import annotations

import re
from datetime import datetime
from enum import StrEnum
from typing import Final

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from mu_client.errors import InvalidRevokeReasonError

__all__ = [
    "MAX_REVOKE_REASON_CHARS",
    "NAMED_REASON_PATTERN",
    "NAMED_REASON_RULE",
    "AgentShareGrantView",
    "RevocationReceiptState",
    "RevocationReceiptView",
    "RevokeAgentShareBody",
    "ServerCascadeResidue",
    "assert_named_reason",
]

#: ``routes/rooms.py:886`` — ``reason: str | None = Field(default=None, max_length=64)``. Bounded
#: HERE too so a client can never present a body the server will 422.
#:
#: ⚠ The bound is a LENGTH bound and nothing more. It was previously annotated as the thing that
#: stopped *"prose about the conversation being smuggled onto a content-free ledger row"*, and it
#: does not: 62 characters of conversation content fit inside it, were written to this device's
#: sqlite and POSTed to the server's trust ledger. :data:`NAMED_REASON_PATTERN` is what enforces
#: that sentence.
MAX_REVOKE_REASON_CHARS = 64

#: A revoke reason is a NAME, not a sentence. ``trust-ledger-spec.md`` §2 rule 3: an entry carries
#: *"only ids, content hashes, principal ids, enums, timestamps, and counts — never memory body,
#: prompt, or secret."* A lowercase identifier is an enum-shaped value; anything with a space,
#: capital or punctuation is prose, and prose about why an owner revoked is exactly the class of
#: text that is about a conversation.
NAMED_REASON_PATTERN: Final = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
NAMED_REASON_RULE: Final = (
    "lowercase letters, digits and underscores, 1-64 chars, e.g. user_revoked"
)


def assert_named_reason(reason: str | None) -> None:
    """Refuse a reason that is not a name. ``None`` is always fine — a reason is optional.

    Called at the EDGE (CLI argument parsing, the daemon's IPC route) and again at the top of
    ``AgentShareConsentService.revoke``, so no path can reach the durable local cut with a value
    that would raise mid-ordering. Content-free: it never echoes the rejected string.
    """
    if reason is None:
        return
    if NAMED_REASON_PATTERN.match(reason) is None:
        raise InvalidRevokeReasonError(NAMED_REASON_RULE)


class AgentShareGrantView(BaseModel):
    """``GET /v1/rooms/{room_id}/agent-share/{agent_principal_id}``.

    Mirrors ``routes/rooms.py:816-828``. ``active`` is COMPUTED server-side against the server's
    clock (``routes/rooms.py:849-851``), so this client never recomputes it from ``expires_at``:
    two clocks disagreeing about whether a consent is live is exactly the ambiguity a single
    authority removes.
    """

    model_config = ConfigDict(frozen=True, extra="ignore")

    grant_id: str = Field(min_length=1)
    agent_principal_id: str = Field(min_length=1)
    room_id: str = Field(min_length=1)
    #: D4's ``granted_by`` — the owner who consented.
    granted_by: str = Field(min_length=1)
    #: D4's ``capabilities``. Server-side this is a ``frozenset[str]`` with ``min_length=1``
    #: (``consent/model.py:214``: an empty set reads as UNCAPPED to ``AgentBinding.assert_allows``,
    #: i.e. consent to everything). The server sorts it onto the wire as a list.
    #: ``min_length=1`` mirrors the server's own floor (``consent/model.py:214``) rather than
    #: trusting it. An empty set is not "the safest possible grant": the server's own comment says
    #: it reads as UNCAPPED to ``AgentBinding.assert_allows``, i.e. consent to everything — and
    #: unbounded here it rendered as the LEAST permissive grant in the vocabulary ("nothing this
    #: client recognises", every invariant held). A grant this client cannot represent honestly is
    #: one it must refuse to represent at all.
    capabilities: tuple[str, ...] = Field(min_length=1)
    #: ``AwareDatetime``, not ``datetime``: see
    #: :class:`~mu_client.errors.NaiveConsentTimestampError`. This value is compared against a local
    #: withdrawal instant, and a naive value would be read as LOCAL time — silently shifting which
    #: shares this device reports as withdrawn. Refusing is the only safe reading, and the consent
    #: path still survives it: ``revoke`` treats an unreadable grant as a reason to widen the cut to
    #: BLANKET, never as a reason not to cut.
    issued_at: AwareDatetime
    expires_at: datetime | None = None
    revoked_at: datetime | None = None
    active: bool


class RevokeAgentShareBody(BaseModel):
    """``POST /v1/rooms/{room_id}/agent-share/revoke`` — D4's one-tap revoke
    (``routes/rooms.py:872-886``)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    agent_principal_id: str = Field(min_length=1, max_length=128)
    #: A NAMED reason for an operator (``user_revoked``, ``policy_change``), never prose about the
    #: conversation — it lands on a content-free trust-ledger row. Enforced by
    #: :data:`NAMED_REASON_PATTERN`, not merely by the length cap.
    reason: str | None = Field(
        default=None, max_length=MAX_REVOKE_REASON_CHARS, pattern=NAMED_REASON_PATTERN
    )


class RevocationReceiptState(StrEnum):
    """``consent/ledger.py:265-270``.

    ⚠ ``PARTIAL`` is what this build always produces, **by construction**: ``routes/rooms.py:912``
    — *"there is no ``revoke_ack`` intake, so no confirmation can land. A client that renders
    'revoked everywhere' from a ``PARTIAL`` receipt is misreporting it."* That sentence is a client
    obligation, and :mod:`mu_client.consent.residue` is where it is discharged.
    """

    ISSUED = "issued"
    SETTLED = "settled"
    PARTIAL = "partial"


class ServerCascadeResidue(StrEnum):
    """The server's closed residue vocabulary (``consent/cascade_names.py:46-135``) — *what a
    revoke did NOT reach*.

    Mirrored by VALUE only. Membership here is used to look up an explanation; a value absent from
    it is rendered as unrecognised rather than dropped (see this module's docstring).
    """

    IN_FLIGHT_RESULT_MID_APPEND = "in_flight_result_mid_append"
    CONTROL_FRAME_NOT_DURABLE = "control_frame_not_durable"
    CONTROL_FRAME_NOT_PUBLISHED = "control_frame_not_published"
    PUBLISHED_FRAMES_RETAINED_IN_BROKER = "published_frames_retained_in_broker"
    AUTHORIZED_IDS_RESTAMP_UNBUILT = "authorized_ids_restamp_unbuilt"
    WARM_CACHE_PURGE_UNBUILT = "warm_cache_purge_unbuilt"
    DAEMON_SUBPROCESS_NOT_CANCELLABLE = "daemon_subprocess_not_cancellable"
    REVOKE_ACK_NOT_INTAKEN = "revoke_ack_not_intaken"
    POSTED_AGENT_RESULTS_RETAINED = "posted_agent_results_retained"


class RevocationReceiptView(BaseModel):
    """What a client needs off ``RevocationReceipt`` (``consent/ledger.py:273-328``).

    The tamper-evidence half (``proof``, ``ledger_seq_range``, ``chain_head_hash``, the full
    ``object_ref``) is deliberately NOT modelled: verifying a hash chain is the trust surface's job
    (``GET /v1/trust/receipts/{cascade_root_grant_id}``), and half-parsing a proof would invite a
    client to imply it had checked one. ``signature_present`` records only whether a signature was
    on the wire — ``ledger.py:324-328`` makes ``signature = None`` a SPECIFIED state ("verification
    pending"), not a missing field, so it must be renderable as such.
    """

    model_config = ConfigDict(frozen=True, extra="ignore")

    receipt_id: str = Field(min_length=1)
    cascade_root_grant_id: str = Field(min_length=1)
    revoked_principal_ids: tuple[str, ...] = ()
    revoked_by: str = Field(min_length=1)
    revoked_at: datetime
    settled_at: datetime | None = None
    grants_revoked: int = Field(ge=0)
    ack_pending: int = Field(ge=0)
    cache_entries_purged: int = Field(ge=0)
    state: RevocationReceiptState
    #: ``ledger.py:315`` marks this *"NEVER softened"*: absent a confirmation, the read-after-revoke
    #: window on a pulled LOCAL copy is bounded by a TTL, not by the revoke.
    local_copy_ttl_ceiling_s: int | None = Field(default=None, ge=0)
    #: RAW strings on purpose — see this module's docstring, divergence 2.
    unreachable: tuple[str, ...] = ()
    signature_present: bool = False

    @classmethod
    def of(cls, payload: dict[str, object]) -> RevocationReceiptView:
        """Parse a receipt body, deriving ``signature_present`` from the presence of the field.

        The server sends ``signature: {...} | null``; this client keeps only the boolean, so the
        one-line branch that turns a nullable object into a flag lives HERE rather than at every
        render site.
        """
        return cls.model_validate(
            {**payload, "signature_present": payload.get("signature") is not None}
        )
