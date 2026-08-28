"""**The consent-first ordering, and the honest report** — D4's client half end to end.

The wire is a recording double here and ONLY here: these are pure-logic assertions about ordering
and residue assembly (DEV-STANDARDS: *"mocks are allowed ONLY in pure unit tests of isolated
logic"*). The same service is exercised against a REAL running ``mu-server`` in
``tests/integration/test_agent_share_consent_int.py``; neither run substitutes for the other. The
tombstone store is REAL sqlite in every test below, because the local cut is the thing being
proven.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from mu_client.config import ClientSettings
from mu_client.consent.capabilities import local_capability_name
from mu_client.consent.residue import ClientCascadeResidue
from mu_client.consent.service import AgentShareConsentService
from mu_client.consent.tombstone import SqliteGrantTombstones
from mu_client.consent.wire import (
    AgentShareGrantView,
    RevocationReceiptState,
    RevocationReceiptView,
)
from mu_client.errors import InvalidRevokeReasonError, SharedPlaneUnreachableError

pytestmark = pytest.mark.unit

_T0 = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)
ROOM, AGENT, OWNER = "room-42", "agt-claude", "prn-owner"


def _grant(*, active: bool = True, capabilities: tuple[str, ...] = ("room.participate",)):
    return AgentShareGrantView(
        grant_id="agentshare_deadbeef",
        agent_principal_id=AGENT,
        room_id=ROOM,
        granted_by=OWNER,
        capabilities=capabilities,
        issued_at=_T0,
        active=active,
    )


def _receipt(*unreachable: str) -> RevocationReceiptView:
    return RevocationReceiptView(
        receipt_id="rcpt-1",
        cascade_root_grant_id="agentshare_deadbeef",
        revoked_principal_ids=(AGENT,),
        revoked_by=OWNER,
        revoked_at=_T0,
        grants_revoked=1,
        ack_pending=1,
        cache_entries_purged=0,
        state=RevocationReceiptState.PARTIAL,
        unreachable=unreachable,
        signature_present=True,
    )


class _RecordingWire:
    """A recording double for :class:`~mu_client.consent.client.AgentSharePort`.

    Records the ORDER of its calls, which is what makes the consent-first assertion possible: the
    tombstone write happens between them and can be checked from the real store.
    """

    def __init__(
        self,
        *,
        grant: AgentShareGrantView | None,
        receipt: RevocationReceiptView | None = None,
        get_raises: bool = False,
        revoke_raises: bool = False,
        observer=None,
    ) -> None:
        self._grant = grant
        self._receipt = receipt
        self._get_raises = get_raises
        self._revoke_raises = revoke_raises
        self._observer = observer
        self.calls: list[str] = []

    async def get_grant(self, *, room_id: str, agent_principal_id: str):
        self.calls.append("get")
        if self._get_raises:
            raise SharedPlaneUnreachableError(operation="agent-share status")
        return self._grant

    async def revoke(self, *, room_id: str, agent_principal_id: str, reason: str | None):
        self.calls.append("revoke")
        if self._observer is not None:
            await self._observer()
        if self._revoke_raises:
            raise SharedPlaneUnreachableError(operation="agent-share revoke", status_code=503)
        return self._receipt


async def _service(tmp_path: Path, wire) -> tuple[AgentShareConsentService, SqliteGrantTombstones]:
    store = SqliteGrantTombstones(tmp_path / "consent.sqlite")
    await store.open()
    service = AgentShareConsentService(
        wire=wire, tombstones=store, settings=ClientSettings(), clock=lambda: _T0
    )
    return service, store


# ==================================================================================================
# describe — the "your agent is shared here" affordance
# ==================================================================================================
async def test_describe_computes_the_contract_for_the_grant_the_server_holds(
    tmp_path: Path,
) -> None:
    """**MUTATION:** in ``describe``, pass ``exposure=None`` -> RED (the affordance renders a bare
    identity line and the owner is never shown what the share exposes)."""
    service, store = await _service(tmp_path, _RecordingWire(grant=_grant()))
    try:
        status = await service.describe(room_id=ROOM, agent_principal_id=AGENT)
        assert status.exposure is not None
        assert [c.name for c in status.exposure.exposed_shared] == ["room.participate"]
        assert status.exposure.withheld_local  # the "keeps Y private" half is populated
        assert status.locally_revoked is False
    finally:
        await store.aclose()


async def test_describe_reports_a_local_cut_even_while_the_server_says_active(
    tmp_path: Path,
) -> None:
    """**Fail-closed, from the service down.**

    The server still reports ``active=True``; this device holds a tombstone. The affordance must
    read WITHDRAWN, because the owner performed a withdrawal here and the network leg is not what
    makes their decision real.

    **MUTATION:** in ``describe``, hardcode ``locally_revoked=False`` -> RED. VERIFIED RED.
    """
    grant = _grant(active=True)
    service, store = await _service(tmp_path, _RecordingWire(grant=grant))
    try:
        await store.record(
            SqliteGrantTombstones.blanket(
                room_id=ROOM, agent_principal_id=AGENT, revoked_at=_T0
            ).model_copy(update={"grant_id": grant.grant_id})
        )
        status = await service.describe(room_id=ROOM, agent_principal_id=AGENT)
        assert status.locally_revoked is True
        assert status.exposure is not None
        assert status.exposure.server_active is True
        assert status.exposure.effectively_live is False
        assert any("WITHDRAWN" in line for line in status.render())
    finally:
        await store.aclose()


async def test_describe_of_an_unshared_agent_answers_not_shared(tmp_path: Path) -> None:
    """The server's non-enumerating 404 becomes ``grant=None``, not an exception.

    **MUTATION:** raise instead of returning a status when the grant is ``None`` -> RED.
    """
    service, store = await _service(tmp_path, _RecordingWire(grant=None))
    try:
        status = await service.describe(room_id=ROOM, agent_principal_id=AGENT)
        assert status.grant is None and status.exposure is None
        assert any("is not shared" in line for line in status.render())
    finally:
        await store.aclose()


# ==================================================================================================
# revoke — the ordering that gives it teeth
# ==================================================================================================
async def test_the_local_cut_is_written_before_the_server_is_called(tmp_path: Path) -> None:
    """**The load-bearing ordering assertion.**

    Copied from the server lane's own rule (``mu-server/src/mu_server/agents/bridge.py:519-526``):
    *"the cascade is ordered consent-first precisely so a crash mid-cascade leaves access CUT,
    never open."* The observer runs INSIDE the wire's ``revoke`` call and reads the real sqlite
    store, so it can only see a row that was already durable at that instant.

    **MUTATION:** move ``await self._tombstones.record(tombstone)`` to after the server leg -> RED
    (the observer sees no row). VERIFIED RED.
    """
    seen: list[bool] = []
    store = SqliteGrantTombstones(tmp_path / "consent.sqlite")
    await store.open()

    async def observe() -> None:
        seen.append(
            await store.is_cut(
                room_id=ROOM,
                agent_principal_id=AGENT,
                grant_id="agentshare_deadbeef",
                issued_at=_T0,
            )
        )

    wire = _RecordingWire(grant=_grant(), receipt=_receipt(), observer=observe)
    service = AgentShareConsentService(
        wire=wire, tombstones=store, settings=ClientSettings(), clock=lambda: _T0
    )
    try:
        await service.revoke(room_id=ROOM, agent_principal_id=AGENT, reason="user_revoked")
        assert seen == [True], "the local cut was not durable before the server was called"
    finally:
        await store.aclose()


async def test_a_revoke_whose_server_leg_fails_still_cuts_locally_and_says_so(
    tmp_path: Path,
) -> None:
    """**The case that makes this verb more than a proxy call.**

    The server is unreachable. The withdrawal is still durable here, the outcome reports
    ``server_confirmed=False``, and ``SERVER_REVOKE_NOT_CONFIRMED`` is on the residue so the owner
    is told the agent may still be able to act in the room.

    **MUTATION:** drop the ``SERVER_REVOKE_NOT_CONFIRMED`` append -> RED (a failed revoke reads as
    a clean one). **MUTATION:** set ``server_confirmed=True`` unconditionally -> RED.
    VERIFIED RED for both.
    """
    wire = _RecordingWire(grant=_grant(), revoke_raises=True)
    service, store = await _service(tmp_path, wire)
    try:
        outcome = await service.revoke(room_id=ROOM, agent_principal_id=AGENT)
        assert outcome.locally_cut is True
        assert outcome.server_confirmed is False
        assert ClientCascadeResidue.SERVER_REVOKE_NOT_CONFIRMED.value in {
            e.name for e in outcome.residue
        }
        assert (
            await store.is_cut(
                room_id=ROOM,
                agent_principal_id=AGENT,
                grant_id="agentshare_deadbeef",
                issued_at=_T0,
            )
            is True
        )
        assert any("may still be able to act" in e.text for e in outcome.residue)
    finally:
        await store.aclose()


async def test_a_revoke_with_no_reachable_grant_still_cuts_with_a_blanket_tombstone(
    tmp_path: Path,
) -> None:
    """The grant id could never be learned; the cut widens rather than disappearing.

    **MUTATION:** in ``revoke``, ``return`` early when the grant read fails -> RED.
    """
    wire = _RecordingWire(grant=None, get_raises=True, revoke_raises=True)
    service, store = await _service(tmp_path, wire)
    try:
        outcome = await service.revoke(room_id=ROOM, agent_principal_id=AGENT)
        assert outcome.grant_id == ""
        assert outcome.locally_cut is True
        assert await store.blanket_cut_at(room_id=ROOM, agent_principal_id=AGENT) == _T0
    finally:
        await store.aclose()


async def test_the_outcome_merges_the_servers_residue_with_this_devices_own(
    tmp_path: Path,
) -> None:
    """One honest picture, not two half-pictures.

    **MUTATION:** drop ``names.extend(receipt.unreachable)`` -> RED (the server's own admissions
    never reach the owner). VERIFIED RED.
    """
    wire = _RecordingWire(
        grant=_grant(),
        receipt=_receipt("revoke_ack_not_intaken", "warm_cache_purge_unbuilt"),
    )
    service, store = await _service(tmp_path, wire)
    try:
        outcome = await service.revoke(room_id=ROOM, agent_principal_id=AGENT)
        names = {e.name for e in outcome.residue}
        # the server's
        assert {"revoke_ack_not_intaken", "warm_cache_purge_unbuilt"} <= names
        # and this device's four daemon-leg admissions
        assert {
            "local_subprocess_not_driven_here",
            "revoke_ack_not_emitted",
            "control_frame_not_consumed",
            "warm_cache_not_grant_scoped",
        } <= names
    finally:
        await store.aclose()


async def test_a_partial_receipt_is_never_rendered_as_fully_settled(tmp_path: Path) -> None:
    """``routes/rooms.py:912-916``'s explicit client obligation.

    **MUTATION:** make ``fully_settled`` ``return True`` -> RED (the render prints "fully settled"
    over a PARTIAL receipt with open gaps). VERIFIED RED.
    """
    wire = _RecordingWire(grant=_grant(), receipt=_receipt("revoke_ack_not_intaken"))
    service, store = await _service(tmp_path, wire)
    try:
        outcome = await service.revoke(room_id=ROOM, agent_principal_id=AGENT)
        assert outcome.server_receipt is not None
        assert outcome.server_receipt.state is RevocationReceiptState.PARTIAL
        assert outcome.fully_settled is False
        rendered = "\n".join(outcome.render())
        assert "NOT fully settled" in rendered
        assert "revoked everywhere" not in rendered
    finally:
        await store.aclose()


async def test_a_local_only_grant_makes_describe_warn_before_the_owner_consents_again(
    tmp_path: Path,
) -> None:
    """End-to-end through the service: a grant that reaches private memory is reported as such.

    This is the owner-facing form of ``test_a_grant_that_names_a_local_capability_BREAKS...`` — the
    same computation, reached through the verb an owner actually calls.

    **MUTATION:** have ``describe`` build the contract from a fixed empty grant -> RED.
    """
    wire = _RecordingWire(
        grant=_grant(capabilities=("room.participate", local_capability_name("recall")))
    )
    service, store = await _service(tmp_path, wire)
    try:
        status = await service.describe(room_id=ROOM, agent_principal_id=AGENT)
        assert any("EXPOSES YOUR PRIVATE MEMORY" in line for line in status.render())
    finally:
        await store.aclose()


# ==================================================================================================
# The consent-first ordering must survive the failures that are NOT SharedPlaneUnreachableError
# ==================================================================================================
class _ExplodingWire:
    """A wire whose STATUS read fails in a way the narrow guard never modelled.

    Real shapes, all reachable: a proxy or captive portal answering ``200 text/html`` makes
    ``response.json()`` raise ``JSONDecodeError``; a server one version ahead that renames a
    required field makes ``model_validate`` raise ``ValidationError``. Neither is a
    ``SharedPlaneUnreachableError``.
    """

    def __init__(self, exc: Exception, *, receipt: RevocationReceiptView | None = None) -> None:
        self._exc = exc
        self._receipt = receipt
        self.calls: list[str] = []

    async def get_grant(self, *, room_id: str, agent_principal_id: str):
        self.calls.append("get")
        raise self._exc

    async def revoke(self, *, room_id: str, agent_principal_id: str, reason: str | None):
        self.calls.append("revoke")
        return self._receipt


@pytest.mark.parametrize(
    "exc",
    [
        json.JSONDecodeError("Expecting value", "", 0),
        pytest.param(
            ValidationError.from_exception_data("AgentShareGrantView", []),
            id="server-renamed-a-required-field",
        ),
    ],
)
async def test_an_unreadable_grant_still_cuts_locally(tmp_path: Path, exc: Exception) -> None:
    """**The consent-first ordering was not exception-safe.**

    ``revoke`` guarded step 1 with ``except SharedPlaneUnreachableError`` only, and the durable
    write is step 2 — so any other failure of the status read escaped the whole verb BEFORE the cut,
    and the revoke performed NO local cut at all. ``wire.py``'s ``extra="ignore"`` decision was
    taken for exactly this: *"the consent path — the one path that must keep working so an owner
    can still revoke"*. ``extra="ignore"`` covers an ADDED field and does nothing for a renamed or
    dropped required one.

    The failure is not swallowed: an unreadable grant is a failed server leg and a BLANKET cut,
    which is wider, never narrower.

    **MUTATION:** narrow step 1's guard back to ``except SharedPlaneUnreachableError`` -> RED
    (the exception escapes and no tombstone exists).
    """
    wire = _ExplodingWire(exc)
    service, store = await _service(tmp_path, wire)
    try:
        outcome = await service.revoke(room_id=ROOM, agent_principal_id=AGENT)
        assert outcome.locally_cut is True
        assert outcome.grant_id == "", "an unreadable grant downgrades the cut to BLANKET"
        assert await store.blanket_cut_at(room_id=ROOM, agent_principal_id=AGENT) == _T0
    finally:
        await store.aclose()


async def test_a_reason_that_is_not_a_name_is_refused_before_anything_is_touched(
    tmp_path: Path,
) -> None:
    """A validation error raised BETWEEN the server read and the durable write defeats the ordering.

    ``GrantTombstone`` enforces the reason rule itself, and it is constructed at step 2 — so an
    over-long or prose reason aborted the revoke with a raw ``ValidationError`` traceback (echoing
    the rejected text) having cut nothing. Over IPC it was worse: the error escaped ``_dispatch``
    and the socket closed with no reply. The refusal now happens at step 0, by name, with the wire
    untouched.

    **MUTATION:** delete the ``assert_named_reason(reason)`` call at the top of ``revoke`` -> RED.
    """
    wire = _RecordingWire(grant=_grant())
    service, store = await _service(tmp_path, wire)
    try:
        with pytest.raises(InvalidRevokeReasonError):
            await service.revoke(
                room_id=ROOM,
                agent_principal_id=AGENT,
                reason="the user asked me to summarise their notes from tuesday morning",
            )
        assert wire.calls == [], "the server must not be touched by a request that is refused"
        assert await store.latest_cut(room_id=ROOM, agent_principal_id=AGENT) is None
    finally:
        await store.aclose()


# ==================================================================================================
# The revoke screen must not report a failure as "nothing live to withdraw"
# ==================================================================================================
async def test_a_failed_revoke_leg_never_reports_nothing_live_to_withdraw(
    tmp_path: Path,
) -> None:
    """**The MIXED path**: status read OK (grant id known), revoke leg failed.

    The branch was keyed on ``grant_id == ""``, which only holds when the server was down for BOTH
    legs. Every other failure — an expired token, 403, 500, a connection dropped between the two
    calls — fell through and told the owner *"the server: nothing live to withdraw (already revoked
    or expired)"* about a grant that was still ACTIVE, with an agent that could still act. The
    residue four lines below said the opposite; the headline is the line an owner reads.

    **MUTATION:** restore ``elif self.server_receipt is None and self.grant_id == "":`` -> RED.
    """
    service, store = await _service(tmp_path, _RecordingWire(grant=_grant(), revoke_raises=True))
    try:
        outcome = await service.revoke(room_id=ROOM, agent_principal_id=AGENT)
        assert outcome.grant_id == "agentshare_deadbeef" and outcome.server_confirmed is False
        rendered = "\n".join(outcome.render())
        assert "nothing live to withdraw" not in rendered
        assert "The server: NOT confirmed" in rendered
    finally:
        await store.aclose()


async def test_a_204_still_reports_nothing_live_to_withdraw(tmp_path: Path) -> None:
    """The branch the fix must NOT collapse: a confirmed revoke with no receipt is the server's 204.

    ``routes/rooms.py:895-905`` — *"a revoke of an already-revoked grant is idempotent and must not
    be an error, but it must also not hand back a receipt implying a second cascade ran."*

    **MUTATION:** in ``render``, merge the two ``server_confirmed`` branches into one -> RED.
    """
    service, store = await _service(tmp_path, _RecordingWire(grant=_grant(), receipt=None))
    try:
        outcome = await service.revoke(room_id=ROOM, agent_principal_id=AGENT)
        assert outcome.server_confirmed is True and outcome.server_receipt is None
        rendered = "\n".join(outcome.render())
        assert "nothing live to withdraw" in rendered
        assert "consent REVOKED" not in rendered
    finally:
        await store.aclose()


async def test_the_settlement_judgement_survives_model_dump(tmp_path: Path) -> None:
    """``fully_settled``'s docstring: *"the one place that judgement is made, so no render site can
    quietly make a different one."* As a bare ``@property`` ``model_dump`` dropped it and the
    daemon's IPC payload shipped every input to the judgement without the judgement.

    **MUTATION:** remove ``@computed_field`` from ``fully_settled`` -> RED.
    """
    service, store = await _service(tmp_path, _RecordingWire(grant=_grant(), receipt=_receipt()))
    try:
        outcome = await service.revoke(room_id=ROOM, agent_principal_id=AGENT)
        assert outcome.model_dump(mode="json")["fully_settled"] is False
    finally:
        await store.aclose()


# ==================================================================================================
# describe — the durable record must be readable while the server is NOT
# ==================================================================================================
async def test_describe_answers_from_the_durable_record_when_the_server_is_unreachable(
    tmp_path: Path,
) -> None:
    """The tombstone exists so a revoke survives a network failure. ``describe`` raised during
    exactly that failure, so the affordance could not be opened to see it — CLI exit 1 and one
    stderr line; over IPC, a 502.

    **MUTATION:** delete the ``except SharedPlaneUnreachableError`` arm in ``describe`` -> RED.
    """
    service, store = await _service(tmp_path, _RecordingWire(grant=_grant(), get_raises=True))
    try:
        await store.record(
            SqliteGrantTombstones.blanket(room_id=ROOM, agent_principal_id=AGENT, revoked_at=_T0)
        )
        status = await service.describe(room_id=ROOM, agent_principal_id=AGENT)
        assert status.server_unreachable is True and status.locally_revoked is True
        rendered = "\n".join(status.render())
        assert "could not be reached" in rendered
        assert "This device has WITHDRAWN this share." in rendered
        assert "THE SERVER NEVER CONFIRMED THAT REVOKE" in rendered
    finally:
        await store.aclose()


async def test_describe_names_the_still_active_server_grant_after_an_unconfirmed_cut(
    tmp_path: Path,
) -> None:
    """**The survivor of a failed revoke, on the PERSISTENT screen.**

    ``SERVER_REVOKE_NOT_CONFIRMED`` was printed once, on the revoke's own output, and then the
    process exited and the warning was gone forever — while the server still held the grant ACTIVE
    and the agent could still read and write in the room. The next ``mu agent-share status`` said
    "WITHDRAWN" and nothing else.

    **MUTATION:** stop passing ``local_cut_server_confirmed`` from ``describe`` (leave it ``None``)
    -> RED.
    """
    wire = _RecordingWire(grant=_grant(), revoke_raises=True)
    service, store = await _service(tmp_path, wire)
    try:
        await service.revoke(room_id=ROOM, agent_principal_id=AGENT)
        status = await service.describe(room_id=ROOM, agent_principal_id=AGENT)
        assert status.local_cut_server_confirmed is False
        rendered = "\n".join(status.render())
        assert "THE SERVER NEVER CONFIRMED YOUR REVOKE" in rendered
        assert "the agent can still act in this room" in rendered
    finally:
        await store.aclose()


async def test_the_404_branch_keeps_the_servers_own_hedge(tmp_path: Path) -> None:
    """``client.get_grant``'s docstring records that "absent" and "not yours to probe" are ONE
    answer, deliberately (``routes/rooms.py:854-856``). The revoked branch asserted *"The server
    reports no live share"* as a fact — a privacy answer the server had explicitly refused to give,
    while the unrevoked branch two lines below hedged it correctly.

    **MUTATION:** drop the parenthetical from the revoked branch -> RED.
    """
    service, store = await _service(tmp_path, _RecordingWire(grant=None))
    try:
        await store.record(
            SqliteGrantTombstones.blanket(room_id=ROOM, agent_principal_id=AGENT, revoked_at=_T0)
        )
        rendered = "\n".join(
            (await service.describe(room_id=ROOM, agent_principal_id=AGENT)).render()
        )
        assert "only a member may probe" in rendered
    finally:
        await store.aclose()
