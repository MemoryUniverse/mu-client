"""``AgentShareConsentService`` — **Decision D4's client half, as one object.**

Two verbs, and they are the two D4 §4.2-D step 4 names: the persistent *"your agent is shared
here"* affordance, and one-tap revoke.

--------------------------------------------------------------------------------------------
:meth:`~AgentShareConsentService.describe` — "what does this grant expose?"
--------------------------------------------------------------------------------------------
Reads the server's projection, computes the exposes-vs-keeps-private contract against THIS device's
real capability surface (:mod:`mu_client.consent.exposure`), and folds in whether this device has
locally withdrawn the grant. The local answer is not cosmetic: it **wins**. See below.

--------------------------------------------------------------------------------------------
:meth:`~AgentShareConsentService.revoke` — and the ordering that gives it teeth
--------------------------------------------------------------------------------------------
The order is: **read the grant (best effort) → write the durable local tombstone → call the
server → report residue.** The local write is FIRST, and that is copied deliberately from the
server lane's own cascade (``mu-server/src/mu_server/agents/bridge.py:519-526``):

    *"the cascade is ordered consent-first precisely so a crash mid-cascade leaves access CUT,
    never open."*

The consequence, which is the whole reason this verb is not a no-op on a client with almost no room
runtime: **a revoke whose network leg fails still cuts here.** This device will not present that
share as live again — :attr:`~mu_client.consent.exposure.AgentExposureContract.effectively_live` is
``server_active and not locally_revoked`` — and the outcome carries
:attr:`~mu_client.consent.residue.ClientCascadeResidue.SERVER_REVOKE_NOT_CONFIRMED` so the owner is
told, in the same breath, that the agent may still be able to act in the room. A client revoke that
silently no-ops is the exact failure D4 exists to prevent; a client revoke that silently *succeeds*
when the server never heard it is the same failure wearing a green tick.

--------------------------------------------------------------------------------------------
What this service deliberately does NOT have
--------------------------------------------------------------------------------------------
No ``assert_may_act(capability)`` gate. The server has one (``consent.assert_may_act`` before
``DispatchRegistry.open``, ``design-sessions-live-rooms.puml:691-699``) because the server has a
dispatch path to gate. This device has none: it drives no agent subprocess, posts no
``AGENT_RESULT``, and holds no room runtime. A gate here would be a call site that looks wired and
never fires — the specific failure this project keeps recording — so it is REPORTED as the seam to
build when the daemon's room leg lands, and not written.

**Content-free (rule 3).** Ids, capability names, named reasons, enum members, fixed English.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, Field, computed_field

from mu_client.config import ClientSettings
from mu_client.consent.client import AgentSharePort
from mu_client.consent.exposure import AgentExposureContract, compute_exposure
from mu_client.consent.residue import (
    ClientCascadeResidue,
    ResidueExplanation,
    explain_all,
)
from mu_client.consent.tombstone import GrantTombstone, SqliteGrantTombstones
from mu_client.consent.wire import (
    AgentShareGrantView,
    RevocationReceiptState,
    RevocationReceiptView,
    assert_named_reason,
)
from mu_client.errors import SharedPlaneUnreachableError

__all__ = [
    "AgentShareConsentService",
    "AgentShareStatus",
    "ClientRevocationOutcome",
]


class AgentShareStatus(BaseModel):
    """The answer to *"is my agent shared here, and what does that expose?"*"""

    model_config = ConfigDict(frozen=True, extra="forbid")

    room_id: str = Field(min_length=1)
    agent_principal_id: str = Field(min_length=1)
    #: ``None`` when the server has no live grant for this pair (its non-enumerating 404).
    grant: AgentShareGrantView | None = None
    #: ``None`` exactly when ``grant`` is ``None`` — there is nothing to compute a contract over.
    exposure: AgentExposureContract | None = None
    #: True when this device has withdrawn the share, whatever the server says. When ``grant`` is
    #: ``None`` this reflects a cut recorded while the server was unreachable or had nothing live.
    locally_revoked: bool = False
    #: ``True`` when the server could not be reached at all for this read. The durable local record
    #: is still answered from — a tombstone that can only be read while the network is up would
    #: defeat the reason it is written before the server is called.
    server_unreachable: bool = False
    #: What the durable record says about the server leg of the covering cut. ``None`` when there
    #: is no local cut.
    local_cut_server_confirmed: bool | None = None

    def render(self) -> tuple[str, ...]:
        """The consent screen. Delegates to the contract when there is one."""
        if self.exposure is not None:
            return self.exposure.render()
        if self.server_unreachable:
            return self._offline_lines()
        if self.locally_revoked:
            return (
                f"This device has WITHDRAWN the share of agent {self.agent_principal_id} in room "
                f"{self.room_id}.",
                # The 404 collapse is preserved rather than resolved: `client.get_grant`'s docstring
                # records that "absent" and "not yours to probe" are ONE answer, deliberately
                # (`routes/rooms.py:854-856`). Stating "the server reports no live share" as a fact
                # asserted a privacy answer the server had refused to give.
                "  The server did not report a live share — which also means it may simply not "
                "have told this device (a room's agent roster is a fact only a member may probe).",
            )
        return (
            f"Agent {self.agent_principal_id} is not shared into room {self.room_id} "
            "(or this room is not yours to probe).",
        )

    def _offline_lines(self) -> tuple[str, ...]:
        """The screen when the shared plane did not answer — answered from the DURABLE record.

        Raising here made the tombstone unreadable during exactly the failure it exists to survive:
        an owner who revoked offline, reopened the affordance and was still offline got a stderr
        line and exit 1 instead of their own withdrawal.
        """
        head = (
            f"The server could not be reached, so this screen answers from THIS DEVICE's durable "
            f"record only (agent {self.agent_principal_id}, room {self.room_id})."
        )
        if not self.locally_revoked:
            return (head, "  This device has NO record of withdrawing this share.")
        if self.local_cut_server_confirmed:
            return (
                head,
                "  This device has WITHDRAWN this share, and the server confirmed that revoke.",
            )
        return (
            head,
            "  This device has WITHDRAWN this share.",
            "  ⚠ THE SERVER NEVER CONFIRMED THAT REVOKE — the agent may still be able to act in "
            "this room. Revoke again once the server is reachable.",
        )


class ClientRevocationOutcome(BaseModel):
    """What a revoke initiated on this device actually achieved — and what it did not."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    room_id: str = Field(min_length=1)
    agent_principal_id: str = Field(min_length=1)
    #: The grant this device cut. Empty string when the grant id could not be learned, i.e. a
    #: BLANKET cut (see :mod:`mu_client.consent.tombstone`).
    grant_id: str = ""
    #: **Always ``True``.** The local tombstone is written before the server is called and its
    #: write is durable before this returns. Modelled as a field rather than omitted so a caller
    #: renders a fact rather than an assumption.
    locally_cut: bool
    cut_at: datetime
    #: ``True`` iff the server confirmed. ``False`` covers both "the server was unreachable" and
    #: "there was nothing live to withdraw" — which are distinguished by ``server_receipt`` and by
    #: the residue, never conflated into one boolean.
    server_confirmed: bool
    #: ``None`` on 204 (nothing live to withdraw) or on a failed server leg.
    server_receipt: RevocationReceiptView | None = None
    #: Everything the revoke did NOT reach: this device's own residue first, then the server's own
    #: ``unreachable`` vocabulary, each translated. Never empty in this build.
    residue: tuple[ResidueExplanation, ...] = ()

    @computed_field  # type: ignore[prop-decorator]
    @property
    def fully_settled(self) -> bool:
        """``False`` in this build, structurally — and never softened.

        ``mu-server/src/mu_server/routes/rooms.py:912-916``: *"``state`` is ``PARTIAL`` in this
        build by construction — there is no ``revoke_ack`` intake, so no confirmation can land. A
        client that renders 'revoked everywhere' from a ``PARTIAL`` receipt is misreporting it."*
        This property is the one place that judgement is made, so no render site can quietly make
        a different one — which is why it is a ``@computed_field`` and not a bare ``@property``:
        ``model_dump`` was dropping it, so the daemon's IPC consumers received every input to the
        judgement and not the judgement, and each had to re-derive it. That is the drift this
        sentence forbids.
        """
        if self.server_receipt is None:
            return False
        return self.server_receipt.state is RevocationReceiptState.SETTLED and not any(
            not explanation.by_design for explanation in self.residue
        )

    def render(self) -> tuple[str, ...]:
        lines = [
            f"Withdrew agent {self.agent_principal_id} from room {self.room_id}.",
            (
                "  This device: CUT (durable, effective immediately)."
                if self.locally_cut
                else "  This device: NOT cut."
            ),
        ]
        # ⚠ The discriminator is ``server_confirmed``, NOT ``grant_id``. Branching on the grant id
        # meant the MIXED path — status read OK (grant id known), revoke leg failed (401/403/500,
        # a dropped pool connection, a rolling restart) — fell through to the last branch and told
        # the owner the server had "nothing live to withdraw" about a grant that was still ACTIVE
        # and an agent that could still act. No test executed that line.
        if self.server_confirmed and self.server_receipt is not None:
            lines.append("  The server: consent REVOKED.")
        elif self.server_confirmed:
            lines.append("  The server: nothing live to withdraw (already revoked or expired).")
        else:
            lines.append("  The server: NOT confirmed — see below.")
        lines.append(
            "  This revoke is NOT fully settled — the items below were not reached:"
            if not self.fully_settled
            else "  This revoke is fully settled."
        )
        for explanation in self.residue:
            marker = "by design" if explanation.by_design else "NOT REACHED"
            lines.append(f"    [{marker}] {explanation.name}: {explanation.text}")
        return tuple(lines)


#: The residue that is TRUE OF EVERY REVOKE this build performs, because each names a mechanism
#: that does not exist here rather than an outcome that happened to occur. Stated as a constant so
#: the set cannot silently shrink: a member leaves this tuple only when the thing it names is
#: actually built.
def _utc_now() -> datetime:
    """The default clock. Named rather than a lambda so it is patchable and greppable."""
    return datetime.now(tz=UTC)


_STANDING_CLIENT_RESIDUE: tuple[ClientCascadeResidue, ...] = (
    ClientCascadeResidue.LOCAL_SUBPROCESS_NOT_DRIVEN_HERE,
    ClientCascadeResidue.REVOKE_ACK_NOT_EMITTED,
    ClientCascadeResidue.CONTROL_FRAME_NOT_CONSUMED,
    ClientCascadeResidue.WARM_CACHE_NOT_GRANT_SCOPED,
    ClientCascadeResidue.POSTED_AGENT_RESULTS_RETAINED,
)


class AgentShareConsentService:
    """D4's client-side consent object, over a real wire client and a real durable local store."""

    def __init__(
        self,
        *,
        wire: AgentSharePort,
        tombstones: SqliteGrantTombstones,
        settings: ClientSettings,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._wire = wire
        self._tombstones = tombstones
        self._settings = settings
        #: Injected so a test can pin the cut instant; DEV-STANDARDS forbids wall-clock in a
        #: deterministic test, and the tombstone's ``issued_at`` comparison is a real ordering rule.
        self._clock = clock

    async def describe(self, *, room_id: str, agent_principal_id: str) -> AgentShareStatus:
        """The *"your agent is shared here"* affordance, with the exposure contract computed.

        Answers from the DURABLE local record when the shared plane does not answer, rather than
        raising: the tombstone exists so a revoke survives a network failure, and a screen that
        cannot be opened during that failure gives the owner nothing to survive with.
        """
        try:
            grant = await self._wire.get_grant(
                room_id=room_id, agent_principal_id=agent_principal_id
            )
        except SharedPlaneUnreachableError:
            cut = await self._tombstones.latest_cut(
                room_id=room_id, agent_principal_id=agent_principal_id
            )
            return AgentShareStatus(
                room_id=room_id,
                agent_principal_id=agent_principal_id,
                locally_revoked=cut is not None,
                server_unreachable=True,
                local_cut_server_confirmed=None if cut is None else cut.server_confirmed,
            )
        if grant is None:
            cut = await self._tombstones.latest_cut(
                room_id=room_id, agent_principal_id=agent_principal_id
            )
            return AgentShareStatus(
                room_id=room_id,
                agent_principal_id=agent_principal_id,
                locally_revoked=cut is not None,
                local_cut_server_confirmed=None if cut is None else cut.server_confirmed,
            )
        cut = await self._tombstones.cut_of(
            room_id=room_id,
            agent_principal_id=agent_principal_id,
            grant_id=grant.grant_id,
            issued_at=grant.issued_at,
        )
        return AgentShareStatus(
            room_id=room_id,
            agent_principal_id=agent_principal_id,
            grant=grant,
            exposure=compute_exposure(
                grant,
                mcp=self._settings.mcp,
                locally_revoked=cut is not None,
                local_cut_server_confirmed=None if cut is None else cut.server_confirmed,
                uncovering_blanket_cut_at=await self._tombstones.uncovering_blanket_cut_at(
                    room_id=room_id,
                    agent_principal_id=agent_principal_id,
                    issued_at=grant.issued_at,
                ),
            ),
            locally_revoked=cut is not None,
            local_cut_server_confirmed=None if cut is None else cut.server_confirmed,
        )

    async def revoke(
        self, *, room_id: str, agent_principal_id: str, reason: str | None = None
    ) -> ClientRevocationOutcome:
        """Withdraw the share. **Cuts locally first, then calls the server, then reports residue.**

        See this module's docstring for why that order is load-bearing rather than incidental.
        """
        # 0) Validate the ONLY caller-supplied free value BEFORE anything is attempted. A
        #    `GrantTombstone` built at step 2 validates `reason` itself, and a ValidationError
        #    raised THERE lands between the server read and the durable write — defeating the
        #    consent-first ordering this whole verb is built on, and (over IPC) escaping `_dispatch`
        #    to close the socket with no reply at all. Refuse first, by name, having cut nothing.
        assert_named_reason(reason)

        # 1) Learn the grant id if we can. A failure here is not fatal: it downgrades the cut from
        #    grant-scoped to BLANKET, which is wider, never narrower.
        #
        #    ⚠ The guard is `Exception`, not `SharedPlaneUnreachableError`, and that breadth is the
        #    point: the narrow guard implemented the opposite of the sentence above it. A proxy or
        #    captive portal answering 200 text/html raised `JSONDecodeError`; a server one version
        #    ahead that renamed a required field raised `ValidationError`; a naive `issued_at` now
        #    raises `NaiveConsentTimestampError`. Every one of them escaped `revoke` BEFORE the
        #    durable write, so the revoke performed NO local cut — on the one path `wire.py` says
        #    "must keep working so an owner can still revoke". Nothing is swallowed: an unreadable
        #    grant is reported as a failed server leg and cut BLANKET.
        grant: AgentShareGrantView | None = None
        read_failed = False
        try:
            grant = await self._wire.get_grant(
                room_id=room_id, agent_principal_id=agent_principal_id
            )
        except Exception:  # deliberately total; see the comment above.
            read_failed = True

        # 2) THE LOCAL CUT — durable before anything else is attempted.
        cut_at = self._clock()
        grant_id = grant.grant_id if grant is not None else ""
        tombstone = GrantTombstone(
            room_id=room_id,
            agent_principal_id=agent_principal_id,
            # "" is the BLANKET sentinel — a cut wide enough to cover a grant this device was never
            # able to read, and no wider (it does not reach a LATER consent act).
            grant_id=grant_id,
            revoked_at=cut_at,
            reason=reason,
        )
        await self._tombstones.record(tombstone)

        # 3) The server leg.
        receipt: RevocationReceiptView | None = None
        server_confirmed = False
        server_failed = read_failed
        try:
            receipt = await self._wire.revoke(
                room_id=room_id, agent_principal_id=agent_principal_id, reason=reason
            )
            # 204 (receipt is None) means there was nothing live to withdraw — the server-side
            # consent is not ACTIVE, which is the state the owner asked for. That IS confirmation.
            server_confirmed = True
            server_failed = False
        except SharedPlaneUnreachableError:
            server_failed = True

        if server_confirmed:
            await self._tombstones.record(tombstone.model_copy(update={"server_confirmed": True}))

        # 4) The honest report.
        names: list[str] = []
        if server_failed:
            names.append(ClientCascadeResidue.SERVER_REVOKE_NOT_CONFIRMED.value)
        names.extend(member.value for member in _STANDING_CLIENT_RESIDUE)
        if receipt is not None:
            names.extend(receipt.unreachable)

        return ClientRevocationOutcome(
            room_id=room_id,
            agent_principal_id=agent_principal_id,
            grant_id=grant_id,
            locally_cut=True,
            cut_at=cut_at,
            server_confirmed=server_confirmed,
            server_receipt=receipt,
            residue=explain_all(tuple(names)),
        )
