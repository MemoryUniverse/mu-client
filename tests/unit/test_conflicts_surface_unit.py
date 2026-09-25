"""The CLIENT half of conflict resolution — ``conflict-resolution-async-design.md`` §5 (AD-300).

Same discipline as the sibling ``test_memory_health_surface_unit.py``: three surfaces (daemon IPC,
CLI, MCP tool) onto TWO real ``mu-engine`` services, driven over a REAL unix socket with REAL
``ConflictInboxProjector``/``ConflictResolutionService`` instances over a REAL (in-process)
``InMemoryConflictRecordRepository`` — the sanctioned LOCAL-plane default
(``mu_engine.lifecycle.conflict``), not a mock. Nothing here re-tests the DETECTION pipeline or the
FalkorDB apply path — those are proven end to end against a real graph by
``mu-core/packages/mu-local/tests/test_ad269_conflict_resolution_wiring_int.py`` and this lane's
own ``test_ad300_conflict_inbox_wiring_int.py``. This file's job is narrower and different: prove
the THREE FACES (§5) that did not exist before AD-300 — the daemon IPC routes, the ``mu conflicts``
CLI, and the ``conflicts``/``conflicts_resolve`` MCP tools — each genuinely reach the real engine
services, through the verb a user/model/CLI would actually call, not through the service class.

MUTATION CHECK (run, red, restored, one representative per surface — the same technique
``test_ad300_conflict_inbox_wiring_int.py`` already documents for the composition-root wiring):
  - IPC: replace ``self._conflict_inbox``/``self._conflict_resolution`` with ``None``
    unconditionally in ``IpcServer.__init__`` -> every IPC test below goes red on the 503
    ``*_UNWIRED`` degrade instead of 200.
  - CLI: comment out the ``if args.command == "conflicts":`` dispatch branch in ``cli._run`` ->
    every ``mu conflicts …`` test below goes red with ``SystemExit``/``None`` instead of 0.
  - MCP: drop ``"conflicts_resolve"`` from ``CONFLICTS_TOOL_NAMES`` (``mcp/surface.py``) ->
    ``test_conflicts_tools_are_independently_gated`` goes red (the resolve tool stays hidden with
    the flag on).
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from mu_contracts.domain.model.conflict import (
    ConflictRecord,
    ConflictState,
)
from mu_contracts.domain.model.conflict_inbox import ConflictInboxView
from mu_contracts.domain.model.memory import Namespace, Visibility
from mu_engine.lifecycle.conflict import InMemoryConflictRecordRepository
from mu_engine.platform.adapters.bus_inproc import InprocBus
from mu_engine.services.conflict.inbox import ConflictInboxProjector
from mu_engine.services.conflict.ports import RecordBackedResolutionQueue
from mu_engine.services.conflict.resolution import (
    ConflictResolutionService,
    ManualDecision,
    ManualDecisionKind,
)

from mu_client import cli
from mu_client.capture.parsers import ParserRegistry
from mu_client.config import ClientSettings, DaemonIpcSettings
from mu_client.conflicts import (
    CONFLICT_INBOX_UNWIRED,
    CONFLICT_RESOLUTION_UNWIRED,
    CONFLICTS_LIST_ROUTE,
    CONFLICTS_RESOLVE_ROUTE,
    MALFORMED_REQUEST,
    SHARED_PLANE_REFUSED,
)
from mu_client.conflicts import (
    namespace_for as conflict_namespace_for,
)
from mu_client.daemon.ipc import IpcServer
from mu_client.daemon.ipc_client import IpcClient
from mu_client.errors import ServiceNotWiredError
from mu_client.host import LocalMemoryHost
from mu_client.inject.recall_bridge import RecallInjectBridge
from mu_client.mcp import tools
from mu_client.mcp.server import build_server
from mu_client.outbox.sqlite_outbox import SqliteOutbox

pytestmark = pytest.mark.unit

_NOW = datetime(2026, 9, 25, 12, 0, tzinfo=UTC)


def _ns(user: str = "default", *, visibility: Visibility = Visibility.PRIVATE) -> Namespace:
    return Namespace(
        org="default", workspace="local", user=user, session="default", visibility=visibility
    )


def _record(
    ns: Namespace,
    *,
    conflict_id: str = "c1",
    state: ConflictState = ConflictState.MANUAL_PENDING,
    member_ids: tuple[str, ...] = ("resident", "incoming"),
) -> ConflictRecord:
    return ConflictRecord(
        conflict_id=conflict_id,
        namespace=ns,
        member_ids=member_ids,
        predicate_key="lives_in",
        method="polarity_cardinality",
        detected_confidence=0.4,
        proposed_winner_id=member_ids[-1],
        state=state,
        detected_at=_NOW,
    )


def _services(
    *records: ConflictRecord,
) -> tuple[ConflictInboxProjector, ConflictResolutionService, InMemoryConflictRecordRepository]:
    """A REAL projector + REAL resolution service over a REAL in-process record store —
    ``LocalContainer``'s own AD-300 wiring, minus FalkorDB (this file tests the FACES, not
    detection/apply — see module docstring)."""
    repo = InMemoryConflictRecordRepository()
    bus = InprocBus()
    inbox = ConflictInboxProjector(records=repo, bus=bus)
    resolution = ConflictResolutionService(
        records=repo, queue=RecordBackedResolutionQueue(repo), bus=bus
    )
    return inbox, resolution, repo


async def _seed(repo: InMemoryConflictRecordRepository, *records: ConflictRecord) -> None:
    for record in records:
        await repo.add(record)


# ------------------------------------------------------------------------------ the real socket
def _settings(tmp_path: Path, socket_path: Path) -> ClientSettings:
    return ClientSettings(
        daemon_socket_path=socket_path,
        outbox_db_path=tmp_path / "client-outbox.sqlite",
        ipc=DaemonIpcSettings(socket_path=socket_path),
        model=None,
    )


class _Daemon:
    def __init__(self, settings: ClientSettings, server: IpcServer) -> None:
        self.settings = settings
        self.server = server
        self.client = IpcClient(settings.ipc)


@pytest_asyncio.fixture
async def daemon_factory(tmp_path: Path) -> AsyncIterator[Any]:
    """A REAL :class:`IpcServer` on a REAL unix socket, mirroring
    ``test_memory_health_surface_unit.py::daemon_factory`` exactly, with the conflict services
    threaded in instead of health/pin."""
    started: list[tuple[IpcServer, SqliteOutbox]] = []

    async def _start(
        *,
        conflict_inbox: ConflictInboxProjector | None = None,
        conflict_resolution: ConflictResolutionService | None = None,
    ) -> _Daemon:
        socket_path = tmp_path / f"d{len(started)}.sock"
        settings = _settings(tmp_path, socket_path)
        outbox = SqliteOutbox(tmp_path / f"daemon-outbox-{len(started)}.sqlite")
        await outbox.open()
        server = IpcServer(
            settings.ipc,
            registry=ParserRegistry(),
            outbox=outbox,
            bridge=RecallInjectBridge(LocalMemoryHost(settings), settings=settings.inject),
            conflict_inbox=conflict_inbox,
            conflict_resolution=conflict_resolution,
        )
        await server.bind()
        started.append((server, outbox))
        return _Daemon(settings, server)

    try:
        yield _start
    finally:
        for server, outbox in started:
            with contextlib.suppress(Exception):
                await server.stop_accepting()
            with contextlib.suppress(Exception):
                await outbox.aclose()


# ==================================================================== 1. IPC — the happy paths
async def test_conflicts_list_route_returns_both_sides_of_a_pending_conflict(
    daemon_factory: Any,
) -> None:
    ns = _ns()
    inbox, resolution, repo = _services()
    await _seed(repo, _record(ns))
    daemon = await daemon_factory(conflict_inbox=inbox, conflict_resolution=resolution)

    reply = await daemon.client.request(CONFLICTS_LIST_ROUTE, {"namespace": list(ns.parts())})

    assert reply["status"] == 200
    assert reply["pending_count"] == 1
    item = reply["pending"][0]
    assert item["conflict_id"] == "c1"
    assert {m["memory_id"] for m in item["members"]} == {"resident", "incoming"}


async def test_conflicts_resolve_route_records_the_decision_and_the_inbox_reflects_it(
    daemon_factory: Any,
) -> None:
    ns = _ns()
    inbox, resolution, repo = _services()
    await _seed(repo, _record(ns))
    daemon = await daemon_factory(conflict_inbox=inbox, conflict_resolution=resolution)

    reply = await daemon.client.request(
        CONFLICTS_RESOLVE_ROUTE,
        {
            "namespace": list(ns.parts()),
            "conflict_id": "c1",
            "kind": "supersede",
            "winner_id": "incoming",
        },
    )
    assert reply["status"] == 200
    assert reply["state"] == ConflictState.RESOLVED.value
    assert reply["resolved_winner_id"] == "incoming"
    # resolved_by is derived from η.user, never taken from the wire (never authz, audit only).
    assert reply["resolved_by"] == ns.user

    after = await daemon.client.request(CONFLICTS_LIST_ROUTE, {"namespace": list(ns.parts())})
    assert after["pending_count"] == 0, "a RESOLVED conflict must not still show as pending"


async def test_conflicts_resolve_does_not_apply_inline(daemon_factory: Any) -> None:
    """§5 line 218: the HTTP/IPC call returns immediately with the record's new state; nothing
    about the resolve route itself supersedes anything (that is the background DISTILL worker's
    job, proven separately against real FalkorDB)."""
    ns = _ns()
    inbox, resolution, repo = _services()
    await _seed(repo, _record(ns))
    daemon = await daemon_factory(conflict_inbox=inbox, conflict_resolution=resolution)

    reply = await daemon.client.request(
        CONFLICTS_RESOLVE_ROUTE,
        {
            "namespace": list(ns.parts()),
            "conflict_id": "c1",
            "kind": "supersede",
            "winner_id": "incoming",
        },
    )
    assert reply["resolution_applied_at"] is None


# ======================================================================== 2. IPC — degrades
async def test_unwired_conflict_services_answer_a_named_503(daemon_factory: Any) -> None:
    ns = _ns()
    daemon = await daemon_factory()
    list_reply = await daemon.client.request(CONFLICTS_LIST_ROUTE, {"namespace": list(ns.parts())})
    assert list_reply == {"status": 503, "error": CONFLICT_INBOX_UNWIRED}
    resolve_reply = await daemon.client.request(
        CONFLICTS_RESOLVE_ROUTE,
        {"namespace": list(ns.parts()), "conflict_id": "c1", "kind": "dismiss"},
    )
    assert resolve_reply == {"status": 503, "error": CONFLICT_RESOLUTION_UNWIRED}


async def test_an_unknown_conflict_id_is_refused_without_echoing_it(daemon_factory: Any) -> None:
    ns = _ns()
    inbox, resolution, _repo = _services()
    daemon = await daemon_factory(conflict_inbox=inbox, conflict_resolution=resolution)
    reply = await daemon.client.request(
        CONFLICTS_RESOLVE_ROUTE,
        {"namespace": list(ns.parts()), "conflict_id": "ghost", "kind": "dismiss"},
    )
    assert reply["status"] == 404
    assert "ghost" not in str(reply)


async def test_resolving_an_already_resolved_conflict_is_a_named_409(daemon_factory: Any) -> None:
    ns = _ns()
    inbox, resolution, repo = _services()
    await _seed(repo, _record(ns, state=ConflictState.RESOLVED))
    daemon = await daemon_factory(conflict_inbox=inbox, conflict_resolution=resolution)
    reply = await daemon.client.request(
        CONFLICTS_RESOLVE_ROUTE,
        {"namespace": list(ns.parts()), "conflict_id": "c1", "kind": "dismiss"},
    )
    # RESOLVED is a terminal state -> _load_actionable refuses it as "not found" (non-enumerating,
    # same shape as absent — see mu_client.conflicts module docstring).
    assert reply["status"] == 404


async def test_a_malformed_resolve_body_is_answered_not_dropped(daemon_factory: Any) -> None:
    ns = _ns()
    inbox, resolution, repo = _services()
    await _seed(repo, _record(ns))
    daemon = await daemon_factory(conflict_inbox=inbox, conflict_resolution=resolution)
    reply = await daemon.client.request(
        CONFLICTS_RESOLVE_ROUTE,
        {"namespace": list(ns.parts()), "conflict_id": "c1", "kind": "not-a-real-kind"},
    )
    assert reply == {"status": 400, "error": MALFORMED_REQUEST}


async def test_a_shared_namespace_is_refused_at_the_surface(daemon_factory: Any) -> None:
    ns = _ns(user="*", visibility=Visibility.SHARED)
    inbox, resolution, _repo = _services()
    daemon = await daemon_factory(conflict_inbox=inbox, conflict_resolution=resolution)
    reply = await daemon.client.request(CONFLICTS_LIST_ROUTE, {"namespace": list(ns.parts())})
    assert reply == {"status": 403, "error": SHARED_PLANE_REFUSED}


# ============================================================================ 3. the CLI surface
@pytest.fixture
def cli_daemon(monkeypatch: pytest.MonkeyPatch) -> Any:
    def _use(settings: ClientSettings) -> None:
        monkeypatch.setattr(cli, "get_client_settings", lambda: settings)

    return _use


async def test_mu_conflicts_list_renders_both_sides(
    daemon_factory: Any, cli_daemon: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    ns = _ns()
    inbox, resolution, repo = _services()
    await _seed(repo, _record(ns))
    daemon = await daemon_factory(conflict_inbox=inbox, conflict_resolution=resolution)
    cli_daemon(daemon.settings)

    assert await cli._run(["conflicts", "list"]) == 0
    out = capsys.readouterr().out
    assert "pending=1" in out
    assert "c1" in out
    assert "resident" in out and "incoming" in out


async def test_mu_conflicts_resolve_drives_the_daemon(
    daemon_factory: Any, cli_daemon: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    ns = _ns()
    inbox, resolution, repo = _services()
    await _seed(repo, _record(ns))
    daemon = await daemon_factory(conflict_inbox=inbox, conflict_resolution=resolution)
    cli_daemon(daemon.settings)

    assert (
        await cli._run(["conflicts", "resolve", "c1", "supersede", "--winner-id", "incoming"])
        == 0
    )
    out = capsys.readouterr().out
    assert "state=resolved" in out
    assert "resolved_winner_id=incoming" in out

    assert await cli._run(["conflicts", "list"]) == 0
    assert "pending=0" in capsys.readouterr().out


async def test_mu_conflicts_reports_the_unwired_service_by_name(
    daemon_factory: Any, cli_daemon: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    daemon = await daemon_factory()
    cli_daemon(daemon.settings)
    assert await cli._run(["conflicts", "list"]) == 1
    assert CONFLICT_INBOX_UNWIRED in capsys.readouterr().err


# ============================================================================ 4. the MCP surface
async def _tool_names(settings: ClientSettings | None = None) -> set[str]:
    server = build_server(settings=settings)
    return {tool.name for tool in await server.list_tools()}


async def test_conflicts_tools_are_off_the_default_agent_surface() -> None:
    assert {"conflicts", "conflicts_resolve"} & await _tool_names() == set()


async def test_conflicts_tools_are_independently_gated() -> None:
    on = await _tool_names(ClientSettings(mcp={"expose_conflicts_tools": True}, model=None))
    assert {"conflicts", "conflicts_resolve"} <= on
    # Turning the OTHER flags on must not also expose these — two different product rules.
    off = await _tool_names(
        ClientSettings(
            mcp={"expose_health_tool": True, "expose_pin_tools": True}, model=None
        )
    )
    assert {"conflicts", "conflicts_resolve"} & off == set()


async def test_the_mcp_tools_delegate_to_the_real_services() -> None:
    settings = ClientSettings(default_user="alice", default_workspace="local", model=None)
    ns = conflict_namespace_for(settings, user="alice", session=None)
    inbox, resolution, repo = _services()
    await _seed(repo, _record(ns))

    view = await tools.tool_conflicts(inbox, settings=settings, user="alice", session=None)
    assert view["pending_count"] == 1

    record = await tools.tool_conflicts_resolve(
        resolution,
        settings=settings,
        conflict_id="c1",
        kind="supersede",
        user="alice",
        session=None,
        winner_id="incoming",
    )
    assert record["state"] == ConflictState.RESOLVED.value
    assert record["resolved_by"] == "alice"


async def test_the_mcp_tools_refuse_loudly_when_the_service_is_not_wired() -> None:
    settings = ClientSettings(default_user="alice", model=None)
    with pytest.raises(ServiceNotWiredError, match="ConflictInboxProjector"):
        await tools.tool_conflicts(None, settings=settings, user="alice", session=None)
    with pytest.raises(ServiceNotWiredError, match="ConflictResolutionService"):
        await tools.tool_conflicts_resolve(
            None, settings=settings, conflict_id="c1", kind="dismiss", user="alice", session=None
        )


# ================================================================= 5. reply-shape round trip
async def test_the_ipc_reply_round_trips_through_the_real_frozen_contracts(
    daemon_factory: Any,
) -> None:
    """Same discipline ``cli._reply_body`` enforces for health/pin — a shape surprise must be a
    named refusal, never a raw ``KeyError``. Proven directly here against the real wire shape."""
    ns = _ns()
    inbox, resolution, repo = _services()
    await _seed(repo, _record(ns))
    daemon = await daemon_factory(conflict_inbox=inbox, conflict_resolution=resolution)

    listed = await daemon.client.request(CONFLICTS_LIST_ROUTE, {"namespace": list(ns.parts())})
    view = cli._reply_body(ConflictInboxView, listed)
    assert view.pending_count == 1

    resolved = await daemon.client.request(
        CONFLICTS_RESOLVE_ROUTE,
        {"namespace": list(ns.parts()), "conflict_id": "c1", "kind": "keep_both"},
    )
    record = cli._reply_body(ConflictRecord, resolved)
    assert record.state is ConflictState.RESOLVED
    assert record.resolution_kind is not None


# Exercise the module's own decision-decoding helper directly too, for the branch the IPC tests
# above cover only through one kind (`ManualDecisionKind` round trip via the wire).
async def test_manual_decision_of_is_never_told_the_resolver() -> None:
    from mu_client.conflicts import manual_decision_of

    ns = _ns(user="bob")
    decision = manual_decision_of(
        {"kind": "merge", "winner_id": "incoming", "merged_text_ref": "ref-1"}, ns=ns
    )
    assert isinstance(decision, ManualDecision)
    assert decision.kind is ManualDecisionKind.MERGE
    assert decision.resolved_by == "bob", "resolved_by must come from η.user, never the wire"
