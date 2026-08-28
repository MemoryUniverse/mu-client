"""The daemon's two consent routes — D4 §4.2-D step 4's persistent affordance.

Real ``IpcServer`` dispatch, real ``AgentShareConsentService``, real sqlite tombstone store; only
the wire (the network) is a double, per DEV-STANDARDS' unit-test carve-out.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from mu_client.config import ClientSettings, DaemonIpcSettings
from mu_client.consent.ipc_surface import (
    AGENT_SHARE_REVOKE_ROUTE,
    AGENT_SHARE_ROUTE,
    CONSENT_INVALID_REASON,
    CONSENT_MALFORMED_REQUEST,
    SHARED_PLANE_UNCONFIGURED,
)
from mu_client.consent.service import AgentShareConsentService
from mu_client.consent.tombstone import SqliteGrantTombstones
from mu_client.consent.wire import AgentShareGrantView
from mu_client.daemon.ipc import IpcServer

pytestmark = pytest.mark.unit

_T0 = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)
ROOM, AGENT = "room-42", "agt-claude"


class _Wire:
    def __init__(self, grant: AgentShareGrantView | None) -> None:
        self._grant = grant

    async def get_grant(self, *, room_id: str, agent_principal_id: str):
        return self._grant

    async def revoke(self, *, room_id: str, agent_principal_id: str, reason: str | None):
        return None  # 204 — nothing live to withdraw


def _grant() -> AgentShareGrantView:
    return AgentShareGrantView(
        grant_id="agentshare_deadbeef",
        agent_principal_id=AGENT,
        room_id=ROOM,
        granted_by="prn-owner",
        capabilities=("room.participate",),
        issued_at=_T0,
        active=True,
    )


async def _server(tmp_path: Path, *, consent: AgentShareConsentService | None) -> IpcServer:
    return IpcServer(
        DaemonIpcSettings(socket_path=tmp_path / "d.sock"),
        registry=None,  # type: ignore[arg-type]  # unreached: these routes touch no capture path
        outbox=None,  # type: ignore[arg-type]
        bridge=None,  # type: ignore[arg-type]
        consent=consent,
    )


async def _dispatch(server: IpcServer, request: dict[str, Any]) -> dict[str, Any]:
    return await server._dispatch(request)  # exercising the real route table


async def test_the_status_route_returns_the_computed_exposure_contract(tmp_path: Path) -> None:
    """The daemon answers what a grant exposes, not merely that one exists.

    **MUTATION:** in ``_route_agent_share``, return ``{"status": 200}`` without the payload -> RED.
    """
    store = SqliteGrantTombstones(tmp_path / "consent.sqlite")
    await store.open()
    service = AgentShareConsentService(
        wire=_Wire(_grant()), tombstones=store, settings=ClientSettings(), clock=lambda: _T0
    )
    try:
        reply = await _dispatch(
            await _server(tmp_path, consent=service),
            {"route": AGENT_SHARE_ROUTE, "room_id": ROOM, "agent_principal_id": AGENT},
        )
        assert reply["status"] == 200
        exposure = reply["agent_share"]["exposure"]
        assert [c["name"] for c in exposure["exposed_shared"]] == ["room.participate"]
        assert exposure["withheld_local"]
        assert "no_local_capability_exposed" in exposure["invariants_held"]
    finally:
        await store.aclose()


async def test_the_revoke_route_returns_the_outcome_with_its_residue(tmp_path: Path) -> None:
    """**MUTATION:** in ``_route_agent_share_revoke``, drop ``"revocation"`` from the payload ->
    RED."""
    store = SqliteGrantTombstones(tmp_path / "consent.sqlite")
    await store.open()
    service = AgentShareConsentService(
        wire=_Wire(_grant()), tombstones=store, settings=ClientSettings(), clock=lambda: _T0
    )
    try:
        reply = await _dispatch(
            await _server(tmp_path, consent=service),
            {
                "route": AGENT_SHARE_REVOKE_ROUTE,
                "room_id": ROOM,
                "agent_principal_id": AGENT,
                "reason": "user_revoked",
            },
        )
        assert reply["status"] == 200
        revocation = reply["revocation"]
        assert revocation["locally_cut"] is True
        assert revocation["residue"], "a revoke that reports NO residue is over-claiming"
        assert (
            await store.is_cut(
                room_id=ROOM,
                agent_principal_id=AGENT,
                grant_id="agentshare_deadbeef",
                issued_at=_T0,
            )
            is True
        )
    finally:
        await store.aclose()


@pytest.mark.parametrize("route", [AGENT_SHARE_ROUTE, AGENT_SHARE_REVOKE_ROUTE])
async def test_both_routes_refuse_by_name_when_no_server_is_configured(
    tmp_path: Path, route: str
) -> None:
    """503 + a stable name — never a fabricated "not shared", which an owner would read as a
    privacy fact.

    **MUTATION:** return ``{"status": 200, "agent_share": None}`` when ``self._consent is None`` ->
    RED.
    """
    reply = await _dispatch(
        await _server(tmp_path, consent=None),
        {"route": route, "room_id": ROOM, "agent_principal_id": AGENT},
    )
    assert reply == {"status": 503, "error": SHARED_PLANE_UNCONFIGURED}


@pytest.mark.parametrize(
    "request_body",
    [
        {"room_id": ROOM},  # missing agent
        {"room_id": ROOM, "agent_principal_id": 7},  # wrong type
        {"room_id": "", "agent_principal_id": AGENT},  # empty
        {"room_id": ROOM, "agent_principal_id": AGENT, "reason": 3},  # wrong reason type
    ],
)
async def test_a_malformed_request_is_refused_without_echoing_it(
    tmp_path: Path, request_body: dict[str, Any]
) -> None:
    """400 + a stable name, echoing NO part of the request (content-free refusal).

    **MUTATION:** include ``request`` in ``consent_malformed_response``'s payload -> RED.
    """
    store = SqliteGrantTombstones(tmp_path / "consent.sqlite")
    await store.open()
    service = AgentShareConsentService(
        wire=_Wire(_grant()), tombstones=store, settings=ClientSettings(), clock=lambda: _T0
    )
    try:
        reply = await _dispatch(
            await _server(tmp_path, consent=service),
            {"route": AGENT_SHARE_REVOKE_ROUTE, **request_body},
        )
        assert reply == {"status": 400, "error": CONSENT_MALFORMED_REQUEST}
    finally:
        await store.aclose()


async def test_a_reason_that_is_not_a_name_is_a_named_400_not_an_escape(tmp_path: Path) -> None:
    """**The failure mode ``_handle``'s own 413 comment names by hand.**

    A ``ValidationError`` raised inside the revoke route escaped ``_dispatch``; ``_handle`` catches
    only ``(TimeoutError, ConnectionError)``, so the request fell to ``finally`` and the socket
    closed with NO response — and *"a silent close is indistinguishable from a success at the
    client, and that is exactly how captures were being dropped."*

    The refusal is now a modelled 400 with a stable name, and the RULE is echoed rather than the
    rejected value — echoing it would put the very prose this refusal keeps off a ledger row onto an
    IPC payload instead.

    **MUTATION:** remove ``assert_named_reason(reason)`` from ``consent_request_of`` -> RED.
    **MUTATION:** drop the ``except CONSENT_SURFACE_ERRORS`` arm around the parse -> RED (the error
    escapes ``_dispatch`` rather than becoming a response).
    """
    prose = "the user asked me to summarise their therapy notes"
    store = SqliteGrantTombstones(tmp_path / "consent.sqlite")
    await store.open()
    service = AgentShareConsentService(
        wire=_Wire(_grant()), tombstones=store, settings=ClientSettings(), clock=lambda: _T0
    )
    try:
        reply = await _dispatch(
            await _server(tmp_path, consent=service),
            {
                "route": AGENT_SHARE_REVOKE_ROUTE,
                "room_id": ROOM,
                "agent_principal_id": AGENT,
                "reason": prose,
            },
        )
        assert reply["status"] == 400
        assert reply["error"] == CONSENT_INVALID_REASON
        assert prose not in str(reply)
        # Nothing was cut: the refusal happens at the edge, before the service is entered.
        assert await store.latest_cut(room_id=ROOM, agent_principal_id=AGENT) is None
    finally:
        await store.aclose()


async def test_an_unmodelled_route_failure_still_answers_instead_of_closing_the_socket(
    tmp_path: Path,
) -> None:
    """Every route, not just this one: an exception no route modelled used to close the socket with
    no reply at all. A named 500 keeps DEV-STANDARDS rule 8 (never a silent wrong answer) while
    still surfacing the bug.

    Exercised through the REAL ``_handle`` over a REAL unix socket, because the defect was in the
    handler's exception arms and a ``_dispatch``-level test cannot see them.

    **MUTATION:** delete the ``except Exception`` arm in ``IpcServer._handle`` -> RED (the client's
    ``readline()`` returns b"" — a silent close).
    """
    socket_path = tmp_path / "d.sock"
    server = IpcServer(
        DaemonIpcSettings(socket_path=socket_path),
        registry=None,  # type: ignore[arg-type]
        outbox=None,  # type: ignore[arg-type]
        bridge=None,  # type: ignore[arg-type]
        consent=None,
    )

    async def _explode(request: dict[str, Any]) -> dict[str, Any]:
        raise RuntimeError("a route that nobody modelled")

    server._dispatch = _explode  # type: ignore[method-assign]
    await server.bind()
    try:
        reader, writer = await asyncio.open_unix_connection(str(socket_path))
        writer.write(b'{"route": "state"}\n')
        await writer.drain()
        raw = await asyncio.wait_for(reader.readline(), timeout=5)
        writer.close()
        await writer.wait_closed()
    finally:
        await server.stop_accepting()

    assert raw, "the socket closed with no reply — the failure the 413 branch refuses by name"
    assert json.loads(raw) == {"status": 500, "error": "internal_error"}
