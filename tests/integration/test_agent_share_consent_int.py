"""**Decision D4's client half against a REAL ``mu-server``. Zero mocks, real HTTP, real stores.**

``SERVER-AND-COLLAB-DESIGN-REVIEW.md:95`` / §4.2-A at ``:118-124``.

--------------------------------------------------------------------------------------------
What runs, and why it is shaped this way
--------------------------------------------------------------------------------------------
``mu-client`` may not import ``mu-server`` (project ``CLAUDE.md``'s boundary rule;
``.importlinter``'s ``client-has-no-server``), so the server runs as a **separate OS process in
mu-server's own virtualenv** — ``scripts/run_real_mu_server_for_consent_it.py``, which composes
``mu_server.app.build_app`` over a scratch Postgres database, the real Valkey-backed consent store,
the real Postgres trust ledger and the real Ed25519 signer. Nothing crosses the boundary but bytes
on a socket. Every call below is real HTTP against that process.

The mu-sdk conformance-server pattern was deliberately NOT used. A conformance server written by
the same lane as the client proves only that the two agree with each other, and the D4 receipt's
``state``, its ``unreachable`` residue list and the 200-vs-204 split are **server judgements** — the
client's entire job is to render them honestly, so they have to come from the real server.

--------------------------------------------------------------------------------------------
What this run does NOT prove — stated so nobody reads more into a green than is there
--------------------------------------------------------------------------------------------
* **Authentication.** The harness overrides ``get_auth`` and sets ``app.state.client_gateway =
  None``, the same posture mu-server's own ``tests/integration/test_consent_revoke_int.py`` takes.
  The gateway is ENABLED (the settings refuse to construct otherwise) but bypassed, so no
  credential is ever presented or judged. This run is about the consent routes, not the edge.
* **Live push.** Centrifugo is off — and that omission is load-bearing, not a compromise: it makes
  the real server report ``control_frame_not_published``, which is exactly the sort of
  server-authored residue the client must translate rather than invent. Asserted on below.
* **The daemon's leg of the cascade.** It does not exist and cannot: see
  :class:`~mu_client.consent.residue.ClientCascadeResidue` for the four obligations this device
  cannot discharge, each with its citation.
"""

from __future__ import annotations

import os
import subprocess
import time
import uuid
from collections.abc import AsyncIterator
from datetime import datetime
from pathlib import Path

import httpx
import pytest

from mu_client.config import ClientSettings, ConsentSettings
from mu_client.consent.capabilities import local_capability_name
from mu_client.consent.client import HttpAgentShareClient
from mu_client.consent.composition import open_consent_service
from mu_client.consent.exposure import ExposureInvariant
from mu_client.consent.residue import ClientCascadeResidue
from mu_client.consent.service import AgentShareConsentService
from mu_client.consent.tombstone import SqliteGrantTombstones
from mu_client.consent.wire import RevocationReceiptState
from mu_client.mcp.surface import offered_tool_names

pytestmark = pytest.mark.integration

_REPO = Path(__file__).resolve().parents[2]
_HARNESS = _REPO / "scripts" / "run_real_mu_server_for_consent_it.py"
#: mu-server's own venv interpreter. Absent = the run is BLOCKED and skipped with a reason, never
#: faked (DEV-STANDARDS: *"if it can't be stood up, the test is BLOCKED (reported), never faked"*).
_SERVER_PY = Path(
    os.environ.get(
        "MU_D4_IT_SERVER_PY", str(_REPO.parent / "mu-server" / ".venv" / "bin" / "python")
    )
)
#: ⚠ A skip is the right answer for a developer who has not checked out the sibling repo, and the
#: WRONG answer for CI: these are the only tests that prove the D4 client half against a real
#: server, and a silent skip makes their absence indistinguishable from their success. Set
#: ``MU_REQUIRE_D4_IT=1`` wherever the run is supposed to be possible and the skip becomes a
#: FAILURE naming what is missing.
_REQUIRED = os.environ.get("MU_REQUIRE_D4_IT", "").strip().lower() in {"1", "true", "yes"}


def _blocked(reason: str) -> None:
    """Skip, or FAIL when this run was declared possible. One place the decision is made."""
    if _REQUIRED:
        pytest.fail(f"MU_REQUIRE_D4_IT is set but the D4 integration run is {reason}")
    pytest.skip(f"BLOCKED: {reason}")


#: 0 = let the kernel choose the port. A FIXED port cost a whole mutation round: a leaked
#: server from an earlier run held 18099, every later run then SKIPPED with "did not report
#: READY", and three mutations came back "green" while proving nothing. The harness prints
#: the port it was actually given on its READY line, so nothing has to guess it.
_PORT = int(os.environ.get("MU_D4_IT_PORT", "0"))
_OWNER = "prn-mu-client-d4-owner"
_STARTUP_TIMEOUT_S = 90.0


@pytest.fixture(scope="session")
def real_server() -> AsyncIterator[str]:
    """Launch the REAL mu-server and yield its base URL. One process for the whole session."""
    if not _SERVER_PY.exists():
        _blocked(f"impossible: no mu-server venv at {_SERVER_PY} — cannot stand up a real server")
    proc = subprocess.Popen(  # noqa: S603 — fixed argv, resolved interpreter, no shell
        [str(_SERVER_PY), str(_HARNESS), "--port", str(_PORT)],
        cwd=str(_REPO.parent / "mu-server"),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    base_url: str | None = None
    try:
        assert proc.stdout is not None
        started = time.monotonic()
        while time.monotonic() - started < _STARTUP_TIMEOUT_S:
            line = proc.stdout.readline()
            if not line:
                break
            if line.startswith("READY "):
                base_url = line.split()[1]
                break
        if base_url is None:
            proc.kill()
            _blocked("not READY (the real mu-server did not start — stores unreachable?)")
        yield base_url
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:  # pragma: no cover - defensive
            proc.kill()


async def _seed_shared_agent(base_url: str, room_id: str, agent_id: str, *tools: str) -> None:
    """``open`` -> ``join`` -> ``bind`` over the real wire.

    ``bind`` is what MINTS the grant: ``routes/rooms.py:829-834`` — *"There is no route that ISSUES
    a grant… sharing an agent IS the consent act."* So this is the real consent act, performed the
    only way the server allows.
    """
    async with httpx.AsyncClient(timeout=20.0) as http:
        opened = await http.post(f"{base_url}/v1/rooms/{room_id}/open", json={})
        assert opened.status_code == 201, opened.text
        joined = await http.post(
            f"{base_url}/v1/rooms/{room_id}/join",
            json={"principal_id": _OWNER, "kind": "human"},
        )
        assert joined.status_code == 200, joined.text
        bound = await http.post(
            f"{base_url}/v1/rooms/{room_id}/bind",
            json={
                "agent_principal_id": agent_id,
                "binding_id": f"bind-{uuid.uuid4().hex[:8]}",
                "allowed_tools": list(tools),
            },
        )
        assert bound.status_code == 200, bound.text


def _settings(base_url: str, tmp_path: Path) -> ClientSettings:
    return ClientSettings(
        consent=ConsentSettings(
            server_base_url=base_url,
            request_timeout_s=20.0,
            tombstone_db_path=tmp_path / "consent.sqlite",
        )
    )


def _ids() -> tuple[str, str]:
    tag = uuid.uuid4().hex[:12]
    return f"d4it{tag}", f"agt-d4it-{tag}"


# ==================================================================================================
# describe — the contract computed from a grant the REAL server minted
# ==================================================================================================
async def test_the_exposure_contract_is_computed_from_the_real_servers_grant(
    real_server: str, tmp_path: Path
) -> None:
    """A room-only share: the privacy invariant HOLDS and every local verb is named as withheld.

    Nothing here is stubbed — the capabilities come off a grant the real server minted from a real
    ``bind``, and the withheld set comes off this device's real MCP surface policy.

    **MUTATION:** in ``compute_exposure``, drop the ``CapabilityPlane.LOCAL`` filter from
    ``exposed_local`` -> RED. **MUTATION:** return ``withheld_local=()`` -> RED.
    """
    room_id, agent_id = _ids()
    await _seed_shared_agent(real_server, room_id, agent_id, "room.participate")

    async with open_consent_service(_settings(real_server, tmp_path)) as consent:
        status = await consent.describe(room_id=room_id, agent_principal_id=agent_id)

    assert status.grant is not None
    assert status.grant.capabilities == ("room.participate",)
    assert status.grant.active is True
    assert status.exposure is not None
    assert [c.name for c in status.exposure.exposed_shared] == ["room.participate"]
    assert status.exposure.exposed_local == ()
    assert ExposureInvariant.NO_LOCAL_CAPABILITY_EXPOSED in status.exposure.invariants_held
    assert status.exposure.unrecognised == ()
    assert local_capability_name("recall") in {c.name for c in status.exposure.withheld_local}

    rendered = "\n".join(status.render())
    assert "CANNOT see your private memory" in rendered
    assert "KEEPS PRIVATE: memory.local.recall" in rendered


async def test_a_real_grant_naming_a_local_capability_is_reported_as_exposing_private_memory(
    real_server: str, tmp_path: Path
) -> None:
    """**The case D4 exists for.** An owner binds an agent with a capability that reaches this
    device's private memory; the consent screen must say so, over a real grant.

    ``allowed_tools`` is free-form on the wire (a capability is a TOOL NAME —
    ``mu-server/src/mu_server/consent/model.py:72``), so the server accepts and stores it verbatim.
    Which is exactly why the CLIENT has to be the one that recognises what it means: nothing
    server-side knows that ``memory.local.recall`` names a private-plane verb on this laptop.

    **MUTATION:** in ``compute_exposure``, classify granted names as SHARED unconditionally -> RED.
    """
    room_id, agent_id = _ids()
    await _seed_shared_agent(
        real_server, room_id, agent_id, "room.participate", local_capability_name("recall")
    )

    async with open_consent_service(_settings(real_server, tmp_path)) as consent:
        status = await consent.describe(room_id=room_id, agent_principal_id=agent_id)

    assert status.exposure is not None
    assert [c.name for c in status.exposure.exposed_local] == [local_capability_name("recall")]
    assert ExposureInvariant.NO_LOCAL_CAPABILITY_EXPOSED in status.exposure.invariants_broken
    rendered = "\n".join(status.render())
    assert "EXPOSES YOUR PRIVATE MEMORY" in rendered
    assert "CANNOT see your private memory" not in rendered


async def test_an_unshared_agent_reads_as_not_shared_over_the_real_404(
    real_server: str, tmp_path: Path
) -> None:
    """The server's non-enumerating 404 becomes ``grant=None`` and a plain answer, not an error.

    **MUTATION:** in ``HttpAgentShareClient.get_grant``, raise on 404 instead of returning
    ``None`` -> RED.
    """
    room_id, agent_id = _ids()
    await _seed_shared_agent(real_server, room_id, agent_id, "room.participate")
    async with open_consent_service(_settings(real_server, tmp_path)) as consent:
        status = await consent.describe(room_id=room_id, agent_principal_id="agt-never-bound")
    assert status.grant is None
    assert any("is not shared" in line for line in status.render())


# ==================================================================================================
# revoke — against the REAL cascade and the REAL receipt
# ==================================================================================================
async def test_a_real_revoke_returns_a_partial_receipt_and_every_residue_is_translated(
    real_server: str, tmp_path: Path
) -> None:
    """**The obligation ``routes/rooms.py:912-916`` names, discharged against the real receipt.**

    The real server's cascade reports ``state=partial``, ``ack_pending=1`` and eight residue names.
    Every one must arrive at the owner translated and marked NOT-reached, and the outcome must
    refuse to call itself settled.

    ``control_frame_not_published`` in particular is a fact about the RUNNING server (no push tier
    configured), not something this client could have known — which is the point of reading it off
    the receipt instead of asserting it.

    **MUTATION:** drop ``names.extend(receipt.unreachable)`` from ``revoke`` -> RED.
    **MUTATION:** make ``fully_settled`` return ``True`` -> RED.
    """
    room_id, agent_id = _ids()
    await _seed_shared_agent(real_server, room_id, agent_id, "room.participate")

    async with open_consent_service(_settings(real_server, tmp_path)) as consent:
        outcome = await consent.revoke(
            room_id=room_id, agent_principal_id=agent_id, reason="user_revoked"
        )

    assert outcome.locally_cut is True
    assert outcome.server_confirmed is True
    assert outcome.server_receipt is not None
    receipt = outcome.server_receipt
    assert receipt.state is RevocationReceiptState.PARTIAL
    assert receipt.grants_revoked == 1
    assert receipt.ack_pending == 1
    assert receipt.signature_present is True
    # ``local_copy_ttl_ceiling_s`` is the "NEVER softened" honest bound (``ledger.py:315``).
    assert receipt.local_copy_ttl_ceiling_s is not None

    names = {e.name for e in outcome.residue}
    # what the REAL server admits it did not reach
    assert {
        "revoke_ack_not_intaken",
        "daemon_subprocess_not_cancellable",
        "control_frame_not_published",
        "warm_cache_purge_unbuilt",
        "authorized_ids_restamp_unbuilt",
    } <= names
    # what THIS DEVICE admits it did not reach
    assert {
        ClientCascadeResidue.LOCAL_SUBPROCESS_NOT_DRIVEN_HERE.value,
        ClientCascadeResidue.REVOKE_ACK_NOT_EMITTED.value,
        ClientCascadeResidue.CONTROL_FRAME_NOT_CONSUMED.value,
    } <= names
    # nothing arrives untranslated
    assert all(e.recognised for e in outcome.residue), [
        e.name for e in outcome.residue if not e.recognised
    ]
    assert outcome.fully_settled is False
    assert "NOT fully settled" in "\n".join(outcome.render())


async def test_after_a_real_revoke_the_server_reports_inactive_and_this_device_reports_withdrawn(
    real_server: str, tmp_path: Path
) -> None:
    """Both halves of the cut, read back over the real wire.

    **MUTATION:** in ``describe``, hardcode ``locally_revoked=False`` -> RED on the local half
    while the server half stays green — which is exactly why both are asserted.
    """
    room_id, agent_id = _ids()
    await _seed_shared_agent(real_server, room_id, agent_id, "room.participate")
    settings = _settings(real_server, tmp_path)

    async with open_consent_service(settings) as consent:
        await consent.revoke(room_id=room_id, agent_principal_id=agent_id)
        status = await consent.describe(room_id=room_id, agent_principal_id=agent_id)

    assert status.grant is not None
    assert status.grant.active is False, "the real server did not deactivate the grant"
    assert status.grant.revoked_at is not None
    assert status.locally_revoked is True
    assert status.exposure is not None
    assert status.exposure.effectively_live is False


async def test_a_second_revoke_answers_204_and_is_not_dressed_up_as_a_second_cascade(
    real_server: str, tmp_path: Path
) -> None:
    """``routes/rooms.py:895-901``: idempotent, but *"must also not hand back a receipt implying a
    second cascade ran."*

    **MUTATION:** in ``HttpAgentShareClient.revoke``, synthesise a receipt on 204 -> RED.
    """
    room_id, agent_id = _ids()
    await _seed_shared_agent(real_server, room_id, agent_id, "room.participate")
    settings = _settings(real_server, tmp_path)

    async with open_consent_service(settings) as consent:
        first = await consent.revoke(room_id=room_id, agent_principal_id=agent_id)
        second = await consent.revoke(room_id=room_id, agent_principal_id=agent_id)

    assert first.server_receipt is not None
    assert second.server_receipt is None
    assert second.server_confirmed is True  # 204 IS the state the owner asked for
    assert second.locally_cut is True


async def test_the_local_cut_is_durable_across_a_fresh_service_over_the_same_file(
    real_server: str, tmp_path: Path
) -> None:
    """A withdrawal survives the process that made it — the whole reason it is on disk.

    **MUTATION:** open the tombstone store on ``":memory:"`` -> RED.
    """
    room_id, agent_id = _ids()
    await _seed_shared_agent(real_server, room_id, agent_id, "room.participate")
    settings = _settings(real_server, tmp_path)

    async with open_consent_service(settings) as consent:
        outcome = await consent.revoke(room_id=room_id, agent_principal_id=agent_id)

    store = SqliteGrantTombstones(settings.consent.tombstone_db_path)
    await store.open()
    try:
        async with httpx.AsyncClient(timeout=20.0) as http:
            raw = await http.get(f"{real_server}/v1/rooms/{room_id}/agent-share/{agent_id}")
        issued_at = raw.json()["issued_at"]
        assert (
            await store.is_cut(
                room_id=room_id,
                agent_principal_id=agent_id,
                grant_id=outcome.grant_id,
                issued_at=datetime.fromisoformat(issued_at),
            )
            is True
        )
    finally:
        await store.aclose()


# ==================================================================================================
# The failure path — a REAL transport failure, no mock anywhere
# ==================================================================================================
async def test_a_revoke_against_a_dead_server_still_cuts_locally_and_says_it_did_not_confirm(
    real_server: str, tmp_path: Path
) -> None:
    """**The consent-first ordering, proven against a real unreachable socket.**

    A client bound to a port nothing is listening on: a genuine ``httpx.ConnectError``, not a
    patched exception. The local cut must still be durable, and the outcome must carry
    ``SERVER_REVOKE_NOT_CONFIRMED`` so the owner knows the agent may still be able to act.

    **MUTATION:** suppress the ``SERVER_REVOKE_NOT_CONFIRMED`` append -> RED. VERIFIED.

    ⚠ **What this test does NOT prove, corrected after measuring it.** Moving the tombstone write
    to AFTER the server leg leaves this test GREEN (MEASURED — 9 passed), because the transport
    failure is *caught*, so the write still happens, just later. The ordering only shows up if the
    process dies in between, which no black-box test can stage. The ORDERING is pinned instead by
    ``test_the_local_cut_is_written_before_the_server_is_called`` in
    ``tests/unit/test_consent_service_unit.py``, whose observer runs INSIDE the wire call and reads
    the real sqlite file at that instant; that one goes RED on the same mutation. What THIS test
    proves is the OUTCOME against a real unreachable socket: the cut is durable and the residue is
    honest.
    """
    room_id, agent_id = _ids()
    dead = ConsentSettings(
        # A port in the ephemeral range with nothing bound. Not the real server's.
        server_base_url="http://127.0.0.1:1",
        request_timeout_s=3.0,
        tombstone_db_path=tmp_path / "consent-dead.sqlite",
    )
    store = SqliteGrantTombstones(dead.tombstone_db_path)
    await store.open()
    wire = HttpAgentShareClient(dead)
    try:
        service = AgentShareConsentService(
            wire=wire, tombstones=store, settings=ClientSettings(consent=dead)
        )
        outcome = await service.revoke(room_id=room_id, agent_principal_id=agent_id)

        assert outcome.locally_cut is True
        assert outcome.server_confirmed is False
        assert outcome.grant_id == ""  # the grant id could not be learned -> a BLANKET cut
        assert ClientCascadeResidue.SERVER_REVOKE_NOT_CONFIRMED.value in {
            e.name for e in outcome.residue
        }
        assert await store.blanket_cut_at(room_id=room_id, agent_principal_id=agent_id) is not None
    finally:
        await wire.aclose()
        await store.aclose()


async def test_a_local_cut_wins_over_a_real_server_that_still_reports_the_grant_active(
    real_server: str, tmp_path: Path
) -> None:
    """**Fail-closed, end to end.** The share is live on the real server; this device withdrew it
    while the server was unreachable. The affordance must read WITHDRAWN.

    This is the state a failed revoke actually leaves behind, and it is the reason the local
    tombstone is written first rather than as a cache of the server's answer.

    **MUTATION:** change ``effectively_live`` to ``return self.server_active`` -> RED.
    """
    room_id, agent_id = _ids()
    await _seed_shared_agent(real_server, room_id, agent_id, "room.participate")
    settings = _settings(real_server, tmp_path)

    # Cut locally through a client pointed at a dead socket — the grant on the REAL server is
    # untouched by this.
    dead = settings.consent.model_copy(update={"server_base_url": "http://127.0.0.1:1"})
    store = SqliteGrantTombstones(settings.consent.tombstone_db_path)
    await store.open()
    dead_wire = HttpAgentShareClient(dead)
    try:
        await AgentShareConsentService(
            wire=dead_wire, tombstones=store, settings=ClientSettings(consent=dead)
        ).revoke(room_id=room_id, agent_principal_id=agent_id)
    finally:
        await dead_wire.aclose()
        await store.aclose()

    # Now read the affordance against the REAL server, over the same tombstone file.
    async with open_consent_service(settings) as consent:
        status = await consent.describe(room_id=room_id, agent_principal_id=agent_id)

    assert status.grant is not None
    assert status.grant.active is True, "the real server should still hold this grant as live"
    assert status.locally_revoked is True
    assert status.exposure is not None
    assert status.exposure.effectively_live is False
    assert any("WITHDRAWN" in line for line in status.render())


# ==================================================================================================
# The two HIGH defects, proven against the REAL server rather than against a double
# ==================================================================================================
async def test_a_real_grant_naming_a_withdrawn_tool_still_says_it_exposes_memory(
    real_server: str, tmp_path: Path
) -> None:
    """**The shipped lie, end to end, under the DEFAULT configuration.**

    ``add`` is withdrawn by default (``MU_MCP__EXPOSE_AUTOMATIC_TOOLS`` is ``False``), and while the
    capability PLANE was decided by membership in the offered tool set, a real grant naming
    ``memory.local.add`` — *"write a new memory into your private store"* — fell out of the
    vocabulary entirely and the consent screen printed *"It CANNOT see your private memory"* over
    it, directly beside its own admission that the list was INCOMPLETE.

    The server makes this reachable, not theoretical: ``assert_consentable_capabilities``
    (``mu-server/src/mu_server/consent/model.py:159-182``) imposes LENGTH only, so the bind below
    really is accepted and really is stored verbatim — asserted here off the grant the server hands
    back, not assumed.

    **MUTATION:** in ``all_local_capabilities``, iterate ``offered_tool_names(mcp)`` -> RED.
    **MUTATION:** in ``_private_memory_lines``, drop the recognition conjunct -> RED.
    """
    room_id, agent_id = _ids()
    withdrawn = local_capability_name("add")
    await _seed_shared_agent(real_server, room_id, agent_id, "room.participate", withdrawn)

    settings = _settings(real_server, tmp_path)
    assert "add" not in offered_tool_names(settings.mcp), "precondition: `add` is off by default"

    async with open_consent_service(settings) as consent:
        status = await consent.describe(room_id=room_id, agent_principal_id=agent_id)

    assert status.grant is not None
    assert withdrawn in status.grant.capabilities, "the real server stored the name verbatim"
    assert status.exposure is not None
    assert [c.name for c in status.exposure.exposed_local] == [withdrawn]
    assert ExposureInvariant.NO_LOCAL_CAPABILITY_EXPOSED in status.exposure.invariants_broken
    rendered = "\n".join(status.render())
    assert "CANNOT see your private memory" not in rendered
    assert f"EXPOSES YOUR PRIVATE MEMORY: {withdrawn}" in rendered
    # Honest about WHY it is inert here without pretending that makes it harmless.
    assert "does not offer that tool today" in rendered


async def test_the_persistent_screen_names_the_still_active_grant_a_failed_revoke_left_behind(
    real_server: str, tmp_path: Path
) -> None:
    """**The survivor of a failed revoke is the SERVER-SIDE GRANT, and the screen must name it.**

    ``SERVER_REVOKE_NOT_CONFIRMED`` appeared once, on the revoke's transient receipt; the process
    then exited and the fact was unrecoverable through any client surface, while the durable row
    one column away recorded it. An owner returning to D4 §4.2-D's *persistent* affordance was told
    "WITHDRAWN" over a grant this very test proves the REAL server still reports ACTIVE — i.e. an
    agent that can still read and write in the room.

    Every half here is real: the grant and its ``active: true`` come from the running server over
    HTTP, and the local cut is a real durable sqlite row written by a real ``revoke`` whose server
    leg genuinely failed against a dead socket.

    ⚠ This harness kills BOTH legs (a dead socket), so ``grant_id`` is empty and the revoke's own
    render takes the branch that was always correct. The MIXED path — status read OK, revoke leg
    failed, which is where "the server: nothing live to withdraw" was printed over a live grant —
    cannot be produced here and is pinned by
    ``tests/unit/test_consent_service_unit.py::test_a_failed_revoke_leg_never_reports_nothing_live_to_withdraw``.
    An assertion about it here MEASURED GREEN under the mutation that restores the bug, so it was
    removed rather than kept as decoration.

    **MUTATION:** in ``_survivor_lines``, return ``()`` unconditionally -> RED.
    **MUTATION:** in ``cut_of``, hardcode ``server_confirmed=True`` -> RED.
    """
    room_id, agent_id = _ids()
    await _seed_shared_agent(real_server, room_id, agent_id, "room.participate")
    settings = _settings(real_server, tmp_path)

    dead = settings.consent.model_copy(update={"server_base_url": "http://127.0.0.1:1"})
    store = SqliteGrantTombstones(settings.consent.tombstone_db_path)
    await store.open()
    dead_wire = HttpAgentShareClient(dead)
    try:
        outcome = await AgentShareConsentService(
            wire=dead_wire, tombstones=store, settings=ClientSettings(consent=dead)
        ).revoke(room_id=room_id, agent_principal_id=agent_id, reason="user_revoked")
        assert outcome.server_confirmed is False
    finally:
        await dead_wire.aclose()
        await store.aclose()

    async with open_consent_service(settings) as consent:
        status = await consent.describe(room_id=room_id, agent_principal_id=agent_id)

    assert status.grant is not None and status.grant.active is True
    assert status.local_cut_server_confirmed is False
    rendered = "\n".join(status.render())
    assert "THE SERVER NEVER CONFIRMED YOUR REVOKE" in rendered
    assert "the agent can still act in this room" in rendered


async def test_the_durable_record_is_readable_while_the_real_server_is_unreachable(
    real_server: str, tmp_path: Path
) -> None:
    """The affordance must open during exactly the failure the tombstone exists to survive.

    ``describe`` raised ``SharedPlaneUnreachableError`` here, so an owner who revoked offline and
    reopened the screen — still offline — got a stderr line and exit 1 instead of their own
    withdrawal. The cut below is written against the REAL server's grant id (a real successful GET),
    and then read back with the wire pointed at a dead socket.

    **MUTATION:** delete the ``except SharedPlaneUnreachableError`` arm in ``describe`` -> RED.
    """
    room_id, agent_id = _ids()
    await _seed_shared_agent(real_server, room_id, agent_id, "room.participate")
    settings = _settings(real_server, tmp_path)

    dead = settings.consent.model_copy(update={"server_base_url": "http://127.0.0.1:1"})
    store = SqliteGrantTombstones(settings.consent.tombstone_db_path)
    await store.open()
    dead_wire = HttpAgentShareClient(dead)
    try:
        service = AgentShareConsentService(
            wire=dead_wire, tombstones=store, settings=ClientSettings(consent=dead)
        )
        await service.revoke(room_id=room_id, agent_principal_id=agent_id)
        offline = await service.describe(room_id=room_id, agent_principal_id=agent_id)
    finally:
        await dead_wire.aclose()
        await store.aclose()

    assert offline.server_unreachable is True and offline.locally_revoked is True
    rendered = "\n".join(offline.render())
    assert "could not be reached" in rendered
    assert "This device has WITHDRAWN this share." in rendered
    assert "THE SERVER NEVER CONFIRMED THAT REVOKE" in rendered
