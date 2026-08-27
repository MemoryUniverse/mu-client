"""The CLIENT half of memory-health + pinning — ``memory-health-pinning-spec.md`` §7.

Three surfaces (daemon IPC, MCP tool, CLI) onto the TWO real ``mu-engine`` services, driven here
over a REAL unix socket with REAL ``MemoryHealthService``/``PinService`` instances. The only
substitute anywhere in this file is :class:`_FakeMemoryRepository`, and it is a substitute for the
one thing mu-core has not built: there is no ``MemoryRepository`` implementation in the repo (the
only ``set_pinned``/``enumerate`` definitions are the two Protocol declarations at
``mu-core/packages/mu-contracts/src/mu_contracts/ports/memory.py:47,70,157,171``, and the
``TierRouter`` they fan through is listed as unbuilt scaffold at
``mu-core/packages/mu-engine/src/mu_engine/services/__init__.py:1-12``). Everything ABOVE that port
— the flag rules, the pin bound, the authz refusal, the event, the route table, the tool
registration, the CLI rendering — is the shipped code.

That is also why the surfaces accept their services by injection and answer a NAMED degrade when
none was handed in: the "not wired" path below is not a placeholder, it is the behaviour a caller
gets on a real host today, and it is asserted as such.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
import structlog
from mu_contracts.domain.errors import (
    PinAuthorizationError,
    PinLimitExceededError,
    PinnedTransitionBlocked,
    PinTargetNotFoundError,
    PinTargetNotPinnableError,
)
from mu_contracts.domain.model.health import MemoryHealthFlag, MemoryHealthView
from mu_contracts.domain.model.memory import (
    MemoryItem,
    MemoryKind,
    Namespace,
    SalienceComponents,
    State,
    Tier,
    Validity,
    Visibility,
)
from mu_contracts.domain.model.pin import PinResult
from mu_engine.lifecycle.conflict import InMemoryConflictRecordRepository
from mu_engine.platform.adapters.bus_inproc import InprocBus
from mu_engine.services.health import HealthSettings, HeuristicV1Assessor, MemoryHealthService
from mu_engine.services.health.conflict_edges import PendingConflictEdgeReader
from mu_engine.services.pin import PinService, PinSettings

from mu_client import cli
from mu_client.capture.parsers import ParserRegistry
from mu_client.config import ClientSettings, DaemonIpcSettings
from mu_client.daemon.ipc import IpcServer
from mu_client.daemon.ipc_client import IpcClient
from mu_client.errors import (
    DaemonReplyInvalidError,
    DaemonUnreachableError,
    ServiceNotWiredError,
)
from mu_client.host import LocalMemoryHost
from mu_client.inject.recall_bridge import RecallInjectBridge
from mu_client.mcp import tools
from mu_client.mcp.server import build_server
from mu_client.memory_health import (
    HEALTH_ROUTE,
    HEALTH_UNWIRED,
    MALFORMED_REQUEST,
    PIN_ROUTE,
    PIN_UNWIRED,
    SHARED_PLANE_REFUSED,
    UNKNOWN_HEALTH_FLAG,
    UNPIN_ROUTE,
    local_scope,
    namespace_for,
    parse_health_flags,
    pin_failure_response,
    private_plane_refusal,
)
from mu_client.outbox.sqlite_outbox import SqliteOutbox

pytestmark = pytest.mark.unit

_NOW = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)
#: A marker that exists ONLY as memory CONTENT, so any appearance of it in a log line, a wire
#: payload or a rendered CLI line is a content leak and nothing else.
_CONTENT_MARKER = "zx-plaintext-marker-9f31"


class _FixedClock:
    def now(self) -> datetime:
        return _NOW


def _ns(user: str = "alice", session: str = "default") -> Namespace:
    return Namespace(
        org="default",
        workspace="local",
        user=user,
        session=session,
        visibility=Visibility.PRIVATE,
    )


def _item(
    memory_id: str,
    ns: Namespace,
    *,
    content: str = _CONTENT_MARKER,
    tier: Tier = Tier.MTM,
    state: State = State.ACTIVE,
    pinned: bool = False,
    age_days: float = 30.0,
    recency: float = 0.05,
    strength: float = 1.0,
) -> MemoryItem:
    """A real ``MemoryItem`` whose flags the SHIPPED ``HeuristicV1Assessor`` decides.

    The defaults land on STALE only: 30 days is past ``stale_after_h=168`` and ``recency=0.05`` is
    under ``stale_recency=0.2`` (both halves of the conjunction), while ``R = exp(-30/1) ~= 0``
    sits BELOW ``demote_retention=0.3`` and so is out of the DECAYING band. Passing
    ``age_days=10, recency=0.9, strength=15`` instead lands on DECAYING only
    (``R = exp(-10/15) = 0.51``, inside ``[0.3, 0.6)``, and the recency half of STALE fails) —
    which is what lets a flag FILTER be tested with two genuinely different at-risk items.
    """
    seen = _NOW - timedelta(days=age_days)
    return MemoryItem(
        id=memory_id,
        namespace=ns,
        kind=MemoryKind.PROPOSITION,
        content=content,
        tier=tier,
        state=state,
        validity=Validity(valid_at=seen, recorded_at=seen),
        salience=SalienceComponents(
            relevance=0.5,
            recency=recency,
            usage=0.0,
            importance=0.5,
            score=0.3,
            strength=strength,
            scored_at=seen,
        ),
        last_seen=seen,
        pinned=pinned,
        provenance_id="prov-1",
    )


class _FakeMemoryRepository:
    """An in-memory stand-in for the ``MemoryRepository`` façade mu-core has not implemented.

    PARTITION-SCOPED ON PURPOSE. Every method filters on ``ns.to_prefix()`` — the tenancy
    GUARANTEE (CANONICAL §1 rule 5), not a filter — so "the surface is namespace-scoped" is a
    property the tests can actually observe rather than assume: an item written under one η is
    invisible to a call carrying another, exactly as a real store's key prefix makes it.
    """

    def __init__(self, items: list[MemoryItem] | None = None) -> None:
        self.items: dict[str, MemoryItem] = {item.id: item for item in items or []}
        self.set_pinned_calls: list[tuple[str, str, bool]] = []

    def _in(self, ns: Namespace) -> list[MemoryItem]:
        prefix = ns.to_prefix()
        return [item for item in self.items.values() if item.namespace.to_prefix() == prefix]

    async def get(self, ns: Namespace, id: str) -> MemoryItem | None:
        item = self.items.get(id)
        if item is None or item.namespace.to_prefix() != ns.to_prefix():
            return None
        return item

    async def enumerate(
        self,
        ns: Namespace,
        *,
        states: frozenset[State],
        tiers: frozenset[Tier] | None,
        pinned: bool | None,
        cursor: str | None,
        limit: int,
    ) -> tuple[list[MemoryItem], str | None]:
        rows = sorted(
            (
                item
                for item in self._in(ns)
                if item.state in states
                and (tiers is None or item.tier in tiers)
                and (pinned is None or item.pinned is pinned)
            ),
            key=lambda item: item.id,
        )
        start = int(cursor) if cursor else 0
        page = rows[start : start + limit]
        next_cursor = str(start + limit) if start + limit < len(rows) else None
        return page, next_cursor

    async def set_pinned(
        self,
        ns: Namespace,
        id: str,
        pinned: bool,
        *,
        at: datetime,
        by: str,
        reason: str | None = None,
    ) -> int:
        item = await self.get(ns, id)
        if item is None:
            raise PinTargetNotFoundError("not found")
        self.set_pinned_calls.append((ns.to_prefix(), id, pinned))
        self.items[id] = item.model_copy(
            update={
                "pinned": pinned,
                "pinned_at": at if pinned else None,
                "pinned_by": by if pinned else None,
                "pin_reason": reason if pinned else None,
            }
        )
        return len(self.set_pinned_calls)


def _health_service(repo: _FakeMemoryRepository, **kwargs: Any) -> MemoryHealthService:
    settings = HealthSettings(**kwargs)
    return MemoryHealthService(
        repo=repo,  # type: ignore[arg-type]
        assessor=HeuristicV1Assessor(settings),
        conflicts=PendingConflictEdgeReader(InMemoryConflictRecordRepository()),
        settings=settings,
        clock=_FixedClock(),
    )


def _pin_service(repo: _FakeMemoryRepository, **kwargs: Any) -> PinService:
    return PinService(
        repo=repo,  # type: ignore[arg-type]
        bus=InprocBus(),
        settings=PinSettings(**kwargs),
        clock=_FixedClock(),
    )


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
    """A REAL :class:`IpcServer` on a REAL unix socket — the same fixture shape
    ``test_capture_ipc_framing.py`` uses, with the health/pin services threaded in."""
    started: list[tuple[IpcServer, SqliteOutbox]] = []

    async def _start(
        *, health: MemoryHealthService | None = None, pin: PinService | None = None
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
            health=health,
            pin=pin,
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


@contextlib.contextmanager
def _captured_logs() -> Iterator[list[dict[str, Any]]]:
    with structlog.testing.capture_logs() as entries:
        yield entries


# ================================================================= 1. IPC — the three happy paths
async def test_health_route_returns_the_engines_own_view(daemon_factory: Any) -> None:
    ns = _ns()
    repo = _FakeMemoryRepository([_item("m1", ns), _item("m2", ns, pinned=True)])
    daemon = await daemon_factory(health=_health_service(repo))

    reply = await daemon.client.request(HEALTH_ROUTE, {"namespace": list(ns.parts())})

    assert reply["status"] == 200
    assert reply["summary"]["total"] == 2
    assert reply["summary"]["pinned_count"] == 1
    # The heuristic assessor's own verdict, not one this surface computed: a 30-day-old MTM item
    # at strength 1.0 is both STALE and past the decay band.
    surfaced = {entry["memory_id"]: set(entry["flags"]) for entry in reply["entries"]}
    assert MemoryHealthFlag.STALE.value in surfaced["m1"]
    assert MemoryHealthFlag.PINNED.value in surfaced["m2"]


async def test_pin_route_sets_the_override_and_unpin_route_releases_it(
    daemon_factory: Any,
) -> None:
    ns = _ns()
    repo = _FakeMemoryRepository([_item("m1", ns)])
    daemon = await daemon_factory(pin=_pin_service(repo))
    payload = {"namespace": list(ns.parts()), "memory_id": "m1"}

    pinned = await daemon.client.request(PIN_ROUTE, {**payload, "reason": "policy"})
    assert pinned["status"] == 200
    assert pinned["pinned"] is True
    assert pinned["memory_id"] == "m1"
    assert repo.items["m1"].pinned is True
    assert repo.items["m1"].pin_reason == "policy"

    released = await daemon.client.request(UNPIN_ROUTE, payload)
    assert released["status"] == 200
    assert released["pinned"] is False
    assert repo.items["m1"].pinned is False


# ================================================================ 2. IPC — every Pin* error maps
@pytest.mark.parametrize(
    ("exc", "status", "name"),
    [
        (PinAuthorizationError("x"), 403, "pin_not_authorized"),
        (PinTargetNotFoundError("x"), 404, "pin_target_not_found"),
        (PinTargetNotPinnableError("x"), 409, "pin_target_not_pinnable"),
        (PinLimitExceededError("x"), 429, "pin_limit_exceeded"),
        (PinnedTransitionBlocked("x"), 409, "pinned_transition_blocked"),
    ],
)
def test_every_pin_error_has_its_own_named_status(exc: Exception, status: int, name: str) -> None:
    """One row per §9 error (plus mu-core's un-specced ``PinTargetNotPinnableError``). The
    response carries the STATUS and the stable NAME only — never ``str(exc)``, because mu-core
    writes these denials non-enumerating on purpose."""
    assert pin_failure_response(exc) == {"status": status, "error": name}


async def test_an_unknown_id_is_refused_without_echoing_it(daemon_factory: Any) -> None:
    ns = _ns()
    daemon = await daemon_factory(pin=_pin_service(_FakeMemoryRepository()))
    reply = await daemon.client.request(
        PIN_ROUTE, {"namespace": list(ns.parts()), "memory_id": "nope-1234"}
    )
    assert reply == {"status": 404, "error": "pin_target_not_found"}
    assert "nope-1234" not in json.dumps(reply)


async def test_the_pin_explosion_bound_is_enforced_end_to_end(daemon_factory: Any) -> None:
    ns = _ns()
    repo = _FakeMemoryRepository([_item("m1", ns, pinned=True), _item("m2", ns)])
    daemon = await daemon_factory(pin=_pin_service(repo, max_pins_per_namespace=1))
    reply = await daemon.client.request(
        PIN_ROUTE, {"namespace": list(ns.parts()), "memory_id": "m2"}
    )
    assert reply == {"status": 429, "error": "pin_limit_exceeded"}
    assert repo.items["m2"].pinned is False


async def test_a_settled_item_is_not_pinnable_but_is_still_unpinnable(
    daemon_factory: Any,
) -> None:
    """``pin`` refuses a superseded row (pinning it would strand it un-GC-able forever); ``unpin``
    must stay reachable in every state or a pin taken before the exit could never be released."""
    ns = _ns()
    repo = _FakeMemoryRepository([_item("m1", ns, state=State.SUPERSEDED, pinned=True)])
    daemon = await daemon_factory(pin=_pin_service(repo))
    payload = {"namespace": list(ns.parts()), "memory_id": "m1"}

    assert await daemon.client.request(PIN_ROUTE, payload) == {
        "status": 409,
        "error": "pin_target_not_pinnable",
    }
    released = await daemon.client.request(UNPIN_ROUTE, payload)
    assert released["status"] == 200
    assert repo.items["m1"].pinned is False


async def test_pinning_is_refused_when_the_deployment_disables_it(daemon_factory: Any) -> None:
    ns = _ns()
    repo = _FakeMemoryRepository([_item("m1", ns)])
    daemon = await daemon_factory(pin=_pin_service(repo, enabled=False))
    reply = await daemon.client.request(
        PIN_ROUTE, {"namespace": list(ns.parts()), "memory_id": "m1"}
    )
    assert reply == {"status": 403, "error": "pin_not_authorized"}


# ============================================================ 3. namespace scoping (CLAUDE.md #4)
async def test_every_route_is_scoped_to_the_namespace_on_the_wire(daemon_factory: Any) -> None:
    """η is the partition GUARANTEE, so a call carrying a different η must see nothing of the
    first one's memory and must not be able to mutate it."""
    mine, theirs = _ns(user="alice"), _ns(user="bob")
    repo = _FakeMemoryRepository([_item("m1", mine)])
    daemon = await daemon_factory(health=_health_service(repo), pin=_pin_service(repo))

    mine_view = await daemon.client.request(HEALTH_ROUTE, {"namespace": list(mine.parts())})
    theirs_view = await daemon.client.request(HEALTH_ROUTE, {"namespace": list(theirs.parts())})
    assert mine_view["summary"]["total"] == 1
    assert theirs_view["summary"]["total"] == 0
    assert theirs_view["entries"] == []

    refused = await daemon.client.request(
        PIN_ROUTE, {"namespace": list(theirs.parts()), "memory_id": "m1"}
    )
    assert refused == {"status": 404, "error": "pin_target_not_found"}
    assert repo.items["m1"].pinned is False
    assert repo.set_pinned_calls == []


async def test_a_pin_records_the_partition_it_was_applied_in(daemon_factory: Any) -> None:
    ns = _ns(user="alice", session="s-42")
    repo = _FakeMemoryRepository([_item("m1", ns)])
    daemon = await daemon_factory(pin=_pin_service(repo))
    await daemon.client.request(PIN_ROUTE, {"namespace": list(ns.parts()), "memory_id": "m1"})
    assert repo.set_pinned_calls == [(ns.to_prefix(), "m1", True)]


def test_the_local_scope_is_derived_from_the_authorized_namespace() -> None:
    """On this plane the acting principal IS η's user slot — the same shape ``LocalMemory._scope``
    builds — so ``TenancyGuard.assert_scope`` can never be passed a scope naming a different
    partition than the one the caller asked about."""
    ns = _ns(user="alice", session="s-42")
    scope = local_scope(ns)
    assert scope.principal_id == "alice"
    assert scope.agent_principal_id == "alice"
    assert scope.namespace(Visibility.PRIVATE) == ns


# =========================================================== 4. surface refusals (no silent pass)
async def test_unwired_services_answer_a_named_503_and_never_a_fabricated_view(
    daemon_factory: Any,
) -> None:
    """The state a real host is in today: mu-core ships no ``MemoryRepository``, so the daemon
    composition root cannot build either service. A named 503 is the honest answer — never a raise
    (which would close the connection with no reply) and never an empty view (which would read as
    'your memory is perfectly healthy')."""
    ns = _ns()
    daemon = await daemon_factory()
    assert await daemon.client.request(HEALTH_ROUTE, {"namespace": list(ns.parts())}) == {
        "status": 503,
        "error": HEALTH_UNWIRED,
    }
    for route in (PIN_ROUTE, UNPIN_ROUTE):
        assert await daemon.client.request(
            route, {"namespace": list(ns.parts()), "memory_id": "m1"}
        ) == {"status": 503, "error": PIN_UNWIRED}


async def test_a_shared_namespace_is_refused_at_the_surface(daemon_factory: Any) -> None:
    """mu-client is a PRIVATE-plane host (ADR-0003). ``PinService`` refuses SHARED itself, but
    health would happily assess it — so both are refused once, here."""
    shared = Namespace.shared(org="default", workspace="local", session="room-1")
    repo = _FakeMemoryRepository()
    daemon = await daemon_factory(health=_health_service(repo), pin=_pin_service(repo))
    for route, extra in (
        (HEALTH_ROUTE, {}),
        (PIN_ROUTE, {"memory_id": "m1"}),
        # UNPIN was missing here, and its plane guard was a mutation SURVIVOR because of it:
        # deleting the three ``private_plane_refusal`` lines from ``_route_unpin`` left the whole
        # file green. ``PinService`` would still refuse a SHARED η with ``PinAuthorizationError``,
        # so the deletion was not silently unsafe — but it changed 403 shared_plane_not_available
        # into 403 pin_not_authorized unnoticed, which is a different statement to the user.
        (UNPIN_ROUTE, {"memory_id": "m1"}),
    ):
        reply = await daemon.client.request(route, {"namespace": list(shared.parts()), **extra})
        assert reply == {"status": 403, "error": SHARED_PLANE_REFUSED}


@pytest.mark.parametrize(
    ("route", "request_body"),
    [
        # A ``reason`` past the contract's 200-char bound. USER-REACHABLE: `mu pin --reason` says
        # "max 200 chars" in its help and does not enforce it, so the daemon is where it lands.
        (PIN_ROUTE, {"memory_id": "m1", "reason": "x" * 201}),
        (PIN_ROUTE, {"memory_id": ""}),  # PinRequest bounds memory_id min_length=1
        (PIN_ROUTE, {}),  # no memory_id at all
        (UNPIN_ROUTE, {"memory_id": ""}),
        (UNPIN_ROUTE, {}),
    ],
)
async def test_a_malformed_body_is_answered_not_dropped(
    daemon_factory: Any, route: str, request_body: dict[str, Any]
) -> None:
    """The route handlers built their ``Namespace``/``PinRequest`` outside any ``except``, and
    ``IpcServer._handle`` catches only ``TimeoutError``/``ConnectionError`` — so a pydantic
    ``ValidationError`` (or a ``KeyError``) escaped ``client_connected_cb`` and the daemon closed
    the connection with NO reply. The client then raised ``DaemonUnreachableError``: the user was
    told the daemon was DOWN when it was healthy and their input was simply invalid.

    This is exactly what ``daemon/ipc.py``'s own docstring forbids — *"a close with no reply is
    indistinguishable from success on the wire ... so this server never answers with silence"*.
    """
    ns = _ns()
    repo = _FakeMemoryRepository([_item("m1", ns)])
    daemon = await daemon_factory(health=_health_service(repo), pin=_pin_service(repo))
    reply = await daemon.client.request(route, {"namespace": list(ns.parts()), **request_body})
    assert reply == {"status": 400, "error": MALFORMED_REQUEST}
    assert repo.set_pinned_calls == []


@pytest.mark.parametrize(
    "namespace",
    [
        ["default", "local"],  # not five parts — ValueError out of Namespace.from_parts
        None,  # absent -> KeyError
        7,  # not iterable -> TypeError
    ],
)
async def test_a_malformed_namespace_is_answered_on_every_new_route(
    daemon_factory: Any, namespace: Any
) -> None:
    repo = _FakeMemoryRepository()
    daemon = await daemon_factory(health=_health_service(repo), pin=_pin_service(repo))
    body: dict[str, Any] = {"memory_id": "m1"}
    if namespace is not None:
        body["namespace"] = namespace
    for route in (HEALTH_ROUTE, PIN_ROUTE, UNPIN_ROUTE):
        assert await daemon.client.request(route, body) == {
            "status": 400,
            "error": MALFORMED_REQUEST,
        }


async def test_a_malformed_body_never_echoes_what_the_caller_sent(daemon_factory: Any) -> None:
    """The refusal must not become an accidental content channel: a caller who put memory text in
    the wrong field must not get it back in the error (CLAUDE.md rule 3)."""
    ns = _ns()
    daemon = await daemon_factory(pin=_pin_service(_FakeMemoryRepository()))
    reply = await daemon.client.request(
        PIN_ROUTE,
        {"namespace": list(ns.parts()), "memory_id": "m1", "reason": _CONTENT_MARKER * 20},
    )
    assert reply == {"status": 400, "error": MALFORMED_REQUEST}
    assert _CONTENT_MARKER not in json.dumps(reply)


async def test_an_unknown_health_flag_is_refused_rather_than_ignored(daemon_factory: Any) -> None:
    ns = _ns()
    daemon = await daemon_factory(health=_health_service(_FakeMemoryRepository([_item("m1", ns)])))
    reply = await daemon.client.request(
        HEALTH_ROUTE, {"namespace": list(ns.parts()), "flags": ["stale", "not-a-flag"]}
    )
    assert reply == {"status": 422, "error": UNKNOWN_HEALTH_FLAG}


def test_flag_parsing_round_trips_and_refuses_junk() -> None:
    assert parse_health_flags(None) is None
    assert parse_health_flags([]) is None
    assert parse_health_flags(["stale", "decaying"]) == frozenset(
        {MemoryHealthFlag.STALE, MemoryHealthFlag.DECAYING}
    )
    with pytest.raises(ValueError, match="unknown health flag"):
        parse_health_flags(["nope"])


def test_only_a_private_namespace_passes_the_plane_check() -> None:
    assert private_plane_refusal(_ns()) is None
    assert private_plane_refusal(
        Namespace.shared(org="default", workspace="local", session="r")
    ) == {"status": 403, "error": SHARED_PLANE_REFUSED}


async def test_the_health_filter_reaches_the_service(daemon_factory: Any) -> None:
    """Both items are AT RISK, with DIFFERENT flags — so a filter that never reached the service
    would surface both and be caught, which a healthy-vs-unhealthy pair would not (the service
    drops healthy entries anyway when ``include_healthy`` is off)."""
    ns = _ns()
    repo = _FakeMemoryRepository(
        [_item("m1", ns), _item("m2", ns, age_days=10.0, recency=0.9, strength=15.0)]
    )
    daemon = await daemon_factory(health=_health_service(repo))
    unfiltered = await daemon.client.request(HEALTH_ROUTE, {"namespace": list(ns.parts())})
    assert [entry["memory_id"] for entry in unfiltered["entries"]] == ["m1", "m2"]

    stale = await daemon.client.request(
        HEALTH_ROUTE, {"namespace": list(ns.parts()), "flags": [MemoryHealthFlag.STALE.value]}
    )
    assert stale["status"] == 200
    assert [entry["memory_id"] for entry in stale["entries"]] == ["m1"]

    decaying = await daemon.client.request(
        HEALTH_ROUTE, {"namespace": list(ns.parts()), "flags": [MemoryHealthFlag.DECAYING.value]}
    )
    assert [entry["memory_id"] for entry in decaying["entries"]] == ["m2"]


async def test_the_health_page_is_bounded_and_hands_back_a_cursor(daemon_factory: Any) -> None:
    """§3.1: never an unbounded partition scan. The client surface must carry the cursor through
    or a user can only ever see the first page."""
    ns = _ns()
    repo = _FakeMemoryRepository([_item(f"m{i}", ns) for i in range(5)])
    daemon = await daemon_factory(health=_health_service(repo, page_size=2))
    first = await daemon.client.request(HEALTH_ROUTE, {"namespace": list(ns.parts())})
    assert first["summary"]["total"] == 2
    assert first["next_cursor"] == "2"
    second = await daemon.client.request(
        HEALTH_ROUTE, {"namespace": list(ns.parts()), "cursor": first["next_cursor"]}
    )
    assert [entry["memory_id"] for entry in second["entries"]] == ["m2", "m3"]


# ============================================== 5. the agent-callable / user-directed MCP decision
async def _tool_names(settings: ClientSettings | None = None) -> set[str]:
    server = build_server(settings=settings)
    return {tool.name for tool in await server.list_tools()}


async def test_health_pin_and_unpin_are_not_on_the_default_agent_surface() -> None:
    """THE DECISION (see ``mcp/server.py::_HEALTH_TOOL_NAMES`` for the full argument).

    Recorded honestly: this is an OVERRIDE of spec §7.1 line 332, not a reading of it. That line
    says ``memory.local.health`` is *"Reachable from inside the agent host where the user works"*,
    and in an MCP host the model is the only caller — so withholding it defeats that sentence. It
    is withheld anyway because ``test_mcp_surface_policy_unit.py:47`` (pre-existing, owner-
    ratified) asserts the default surface is EXACTLY the seven deep-dive names, and because
    §7.1:332's named companion ``memory.local.status`` is not registered on this client at all.
    Open question for the owner: ratify or reverse.
    """
    assert {"health", "pin", "unpin"} & await _tool_names() == set()


async def test_removal_leaves_tools_call_too_and_not_only_tools_list() -> None:
    """``remove_tool`` deletes the tool from FastMCP's ``_tool_manager``, so "registered but not
    offered" would be a false description of the default state — there is nothing left to call.
    Asserted so the comment above can never drift back into that softer, wrong claim."""
    server = build_server(settings=ClientSettings(model=None))
    for name in ("health", "pin", "unpin"):
        with pytest.raises(Exception, match=f"Unknown tool: {name}"):
            await server.call_tool(name, {})


async def test_health_and_pin_have_independent_flags() -> None:
    """One flag for both would force the read-only lens to inherit the lifecycle override's risk
    profile, and would make the most likely owner ruling — "health yes, pin no" — impossible to
    express. ``MemoryHealthService`` holds no write port at all; ``PinService`` does."""
    health_only = await _tool_names(ClientSettings(mcp={"expose_health_tool": True}, model=None))
    assert "health" in health_only
    assert {"pin", "unpin"} & health_only == set()

    pin_only = await _tool_names(ClientSettings(mcp={"expose_pin_tools": True}, model=None))
    assert {"pin", "unpin"} <= pin_only
    assert "health" not in pin_only


async def test_the_hook_owned_flag_exposes_neither_of_them() -> None:
    """Two DIFFERENT product rules: `add`/`promote` are withheld because a HOOK already does them,
    `pin`/`health` because the owner has not ruled on §7.1:332. Collapsing them into one flag
    would make 'expose the hook-owned verbs for a host with no hooks installed' silently also hand
    a model the lifecycle override."""
    names = await _tool_names(ClientSettings(mcp={"expose_automatic_tools": True}, model=None))
    assert {"add", "consolidate", "promote", "demote"} <= names
    assert {"health", "pin", "unpin"} & names == set()


async def test_all_three_flags_together_expose_everything() -> None:
    names = await _tool_names(
        ClientSettings(
            mcp={
                "expose_automatic_tools": True,
                "expose_health_tool": True,
                "expose_pin_tools": True,
            },
            model=None,
        )
    )
    assert {"add", "health", "pin", "unpin", "recall", "delete"} <= names


async def test_each_registered_tool_drives_its_own_wrapper_with_its_own_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The registration and the wrapper are two different code paths, and only the wrappers were
    ever called directly. Without this, ``unpin`` could be wired to ``tool_pin`` (a model asking to
    unpin would silently PIN), or the registration could drop ``flags``/``cursor`` on the floor,
    and every other test in this file would still pass. Driven through ``server.call_tool`` — the
    exact path an MCP client takes — with the wrappers recorded rather than run."""
    seen: list[tuple[str, dict[str, Any]]] = []

    def _record(name: str) -> Any:
        async def _wrapper(_service: Any, **kwargs: Any) -> dict[str, Any]:
            seen.append((name, kwargs))
            return {"called": name}

        return _wrapper

    for wrapper_name in ("tool_health", "tool_pin", "tool_unpin"):
        monkeypatch.setattr(tools, wrapper_name, _record(wrapper_name))

    server = build_server(
        settings=ClientSettings(
            mcp={"expose_health_tool": True, "expose_pin_tools": True}, model=None
        )
    )
    await server.call_tool("health", {"flags": ["stale"], "cursor": "7", "session": "s-1"})
    await server.call_tool("pin", {"memory_id": "m1", "reason": "policy", "session": "s-1"})
    await server.call_tool("unpin", {"memory_id": "m1", "session": "s-1"})

    assert [name for name, _ in seen] == ["tool_health", "tool_pin", "tool_unpin"]
    health_kwargs, pin_kwargs, unpin_kwargs = (kwargs for _, kwargs in seen)
    # The registration must carry the model's narrowing through, or a model asking for only
    # `conflicting` gets the UNFILTERED page and reads it as the answer to its question.
    assert health_kwargs["flags"] == ["stale"]
    assert health_kwargs["cursor"] == "7"
    assert pin_kwargs["memory_id"] == "m1"
    assert pin_kwargs["reason"] == "policy"
    assert unpin_kwargs["memory_id"] == "m1"
    for kwargs in (health_kwargs, pin_kwargs, unpin_kwargs):
        assert kwargs["session"] == "s-1"


# ===================================================================== 6. the MCP tool wrappers
def _mcp_settings() -> ClientSettings:
    return ClientSettings(default_user="alice", default_workspace="local", model=None)


async def test_the_mcp_tools_delegate_to_the_real_services() -> None:
    settings = _mcp_settings()
    ns = namespace_for(settings, user="alice", session=None)
    repo = _FakeMemoryRepository([_item("m1", ns)])

    pinned = await tools.tool_pin(
        _pin_service(repo),
        settings=settings,
        memory_id="m1",
        user="alice",
        session=None,
        reason="decision",
    )
    assert pinned["pinned"] is True
    assert repo.items["m1"].pinned is True
    assert repo.items["m1"].pin_reason == "decision"  # the NAMED classification, not a note

    view = await tools.tool_health(
        _health_service(repo), settings=settings, user="alice", session=None
    )
    assert view["summary"]["pinned_count"] == 1

    released = await tools.tool_unpin(
        _pin_service(repo), settings=settings, memory_id="m1", user="alice", session=None
    )
    assert released["pinned"] is False


async def test_the_mcp_health_tool_actually_applies_flags_and_cursor() -> None:
    """``flags``/``cursor`` were accepted by ``tool_health`` and never varied by any test, so the
    wrapper could have passed ``filter_flags=None, cursor=None`` and stayed green — a model
    narrowing to one category would get the whole page and read it as the answer.

    Both items are AT RISK with DIFFERENT flags, so an ignored filter surfaces both and is caught.
    """
    settings = _mcp_settings()
    ns = namespace_for(settings, user="alice", session=None)
    repo = _FakeMemoryRepository(
        [_item("m1", ns), _item("m2", ns, age_days=10.0, recency=0.9, strength=15.0)]
    )

    unfiltered = await tools.tool_health(
        _health_service(repo), settings=settings, user="alice", session=None
    )
    assert [entry["memory_id"] for entry in unfiltered["entries"]] == ["m1", "m2"]

    stale = await tools.tool_health(
        _health_service(repo),
        settings=settings,
        user="alice",
        session=None,
        flags=[MemoryHealthFlag.STALE.value],
    )
    assert [entry["memory_id"] for entry in stale["entries"]] == ["m1"]

    decaying = await tools.tool_health(
        _health_service(repo),
        settings=settings,
        user="alice",
        session=None,
        flags=[MemoryHealthFlag.DECAYING.value],
    )
    assert [entry["memory_id"] for entry in decaying["entries"]] == ["m2"]

    page = await tools.tool_health(
        _health_service(repo, page_size=1), settings=settings, user="alice", session=None
    )
    assert page["next_cursor"] == "1"
    second = await tools.tool_health(
        _health_service(repo, page_size=1),
        settings=settings,
        user="alice",
        session=None,
        cursor=page["next_cursor"],
    )
    assert [entry["memory_id"] for entry in second["entries"]] == ["m2"]


async def test_the_mcp_health_tool_refuses_an_unknown_flag_rather_than_ignoring_it() -> None:
    settings = _mcp_settings()
    with pytest.raises(ValueError, match="unknown health flag"):
        await tools.tool_health(
            _health_service(_FakeMemoryRepository()),
            settings=settings,
            user="alice",
            session=None,
            flags=["not-a-flag"],
        )


async def test_the_mcp_tools_address_the_same_partition_the_other_verbs_do() -> None:
    """``ClientSettings.default_namespace`` fills η.org and ``default_workspace`` fills
    η.workspace — the mapping ``LocalMemory.__init__`` uses. Reversing them would silently assess
    an empty partition while reporting success."""
    settings = ClientSettings(
        default_namespace="acme", default_workspace="team-a", default_user="alice", model=None
    )
    ns = namespace_for(settings)
    assert (ns.org, ns.workspace, ns.user, ns.session) == ("acme", "team-a", "alice", "default")
    assert ns.visibility is Visibility.PRIVATE


async def test_the_mcp_tools_refuse_loudly_when_the_service_is_not_wired() -> None:
    """The same absence the IPC 503 names, in the shape an MCP caller understands: a tool ERROR.
    Never an empty view, which a model would read as 'your memory is fine'."""
    settings = _mcp_settings()
    with pytest.raises(ServiceNotWiredError, match="MemoryHealthService"):
        await tools.tool_health(None, settings=settings, user="alice", session=None)
    with pytest.raises(ServiceNotWiredError, match="PinService"):
        await tools.tool_pin(None, settings=settings, memory_id="m1", user="alice", session=None)
    with pytest.raises(ServiceNotWiredError, match="PinService"):
        await tools.tool_unpin(None, settings=settings, memory_id="m1", user="alice", session=None)


async def test_a_pin_refusal_reaches_the_mcp_caller_as_a_typed_error() -> None:
    settings = _mcp_settings()
    with pytest.raises(PinTargetNotFoundError):
        await tools.tool_pin(
            _pin_service(_FakeMemoryRepository()),
            settings=settings,
            memory_id="ghost",
            user="alice",
            session=None,
        )


# ============================================================ 7. content-free discipline (rule 3)
async def test_no_memory_content_reaches_a_log_a_wire_payload_or_the_cli(
    daemon_factory: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    """CLAUDE.md rule 3. The item's content is a unique marker; pin it, assess it, render it, and
    assert the marker is nowhere in the structlog stream, the IPC replies, or stdout.

    This is structurally true rather than luckily true — ``MemoryHealthView`` has no content field
    at all (mu-core did not build the spec's ``preview``) and ``PinResult`` is ids/booleans/times —
    but a future field added to either would silently break it, which is what this guards.
    """
    ns = _ns()
    repo = _FakeMemoryRepository([_item("m1", ns, content=_CONTENT_MARKER)])
    daemon = await daemon_factory(health=_health_service(repo), pin=_pin_service(repo))

    with _captured_logs() as entries:
        pinned = await daemon.client.request(
            PIN_ROUTE, {"namespace": list(ns.parts()), "memory_id": "m1", "reason": "policy"}
        )
        view = await daemon.client.request(HEALTH_ROUTE, {"namespace": list(ns.parts())})

    # POSITIVE CONTROL. This assertion is structurally satisfied — no line of mu-client can
    # reach ``MemoryItem.content`` on these paths, because neither ``MemoryHealthView`` nor
    # ``PinResult`` has a content field to carry it — so a source mutation cannot make it fail
    # from THIS repo. That makes it worth proving the check has teeth: the marker really is on
    # the item under test, and a payload that DID carry it would be caught by the same call.
    assert _CONTENT_MARKER in repo.items["m1"].content
    assert _CONTENT_MARKER in json.dumps({"preview": repo.items["m1"].content})

    assert pinned["status"] == 200
    assert view["status"] == 200
    assert _CONTENT_MARKER not in json.dumps(entries)
    assert _CONTENT_MARKER not in json.dumps(pinned)
    assert _CONTENT_MARKER not in json.dumps(view)

    # Rendered through mu-core's own frozen, ``extra="forbid"`` contracts — the path the CLI
    # actually takes now, which is also what makes a shape surprise a named refusal instead of a
    # ``KeyError`` traceback escaping ``cli_error_boundary``.
    cli._render_health(cli._reply_body(MemoryHealthView, view))
    cli._render_pin(cli._reply_body(PinResult, pinned))
    assert _CONTENT_MARKER not in capsys.readouterr().out


# ============================================================================ 8. the CLI surface
@pytest.fixture
def cli_daemon(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Point the CLI's env boundary at a test daemon (``cli._run_health``/``_run_pin`` read it
    through ``get_client_settings``, which is ``@lru_cache``'d in production)."""

    def _use(settings: ClientSettings) -> None:
        monkeypatch.setattr(cli, "get_client_settings", lambda: settings)

    return _use


async def test_mu_health_renders_a_content_free_summary(
    daemon_factory: Any, cli_daemon: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    ns = _ns(user="default")
    repo = _FakeMemoryRepository([_item("m1", ns), _item("m2", ns, pinned=True)])
    daemon = await daemon_factory(health=_health_service(repo))
    cli_daemon(daemon.settings)

    assert await cli._run(["health"]) == 0
    out = capsys.readouterr().out
    assert "total=2" in out
    assert "pinned=1" in out
    assert "m1" in out
    assert _CONTENT_MARKER not in out


async def test_mu_health_options_reach_the_daemon(
    daemon_factory: Any, cli_daemon: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    """``--flag``/``--cursor`` were never typed by any test, so ``cli._run_health`` could have sent
    ``flags=None, cursor=None`` and stayed green — ``mu health --flag stale`` would print the
    UNFILTERED page under a heading the user asked to be narrowed. Two at-risk items with
    DIFFERENT flags, so an ignored filter shows both and is caught."""
    ns = _ns(user="default")
    repo = _FakeMemoryRepository(
        [_item("m1", ns), _item("m2", ns, age_days=10.0, recency=0.9, strength=15.0)]
    )
    daemon = await daemon_factory(health=_health_service(repo))
    cli_daemon(daemon.settings)

    assert await cli._run(["health"]) == 0
    both = capsys.readouterr().out
    assert "m1" in both
    assert "m2" in both

    assert await cli._run(["health", "--flag", MemoryHealthFlag.DECAYING.value]) == 0
    decaying = capsys.readouterr().out
    assert "m2" in decaying
    assert "m1" not in decaying

    assert await cli._run(["health", "--cursor", "1"]) == 0
    second_page = capsys.readouterr().out
    assert "m2" in second_page
    assert "m1" not in second_page


async def test_mu_health_refuses_an_unknown_flag_by_name(
    daemon_factory: Any, cli_daemon: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    daemon = await daemon_factory(health=_health_service(_FakeMemoryRepository()))
    cli_daemon(daemon.settings)
    assert await cli._run(["health", "--flag", "not-a-flag"]) == 1
    assert UNKNOWN_HEALTH_FLAG in capsys.readouterr().err


async def test_mu_pin_and_mu_unpin_drive_the_daemon(
    daemon_factory: Any, cli_daemon: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    ns = _ns(user="default")
    repo = _FakeMemoryRepository([_item("m1", ns)])
    daemon = await daemon_factory(pin=_pin_service(repo))
    cli_daemon(daemon.settings)

    assert await cli._run(["pin", "m1", "--reason", "policy"]) == 0
    assert repo.items["m1"].pinned is True
    assert "pinned=True" in capsys.readouterr().out

    assert await cli._run(["unpin", "m1"]) == 0
    assert repo.items["m1"].pinned is False
    assert "pinned=False" in capsys.readouterr().out


async def test_mu_pin_reports_a_refusal_by_name_and_exits_1(
    daemon_factory: Any, cli_daemon: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    daemon = await daemon_factory(pin=_pin_service(_FakeMemoryRepository()))
    cli_daemon(daemon.settings)

    assert await cli._run(["pin", "ghost"]) == 1
    err = capsys.readouterr().err
    assert "pin_target_not_found" in err
    assert "ghost" not in err  # non-enumerating: the CLI must not echo the id back either


async def test_mu_health_says_so_when_the_daemon_is_not_running(
    tmp_path: Path, cli_daemon: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    """These verbs have no second front door (unlike ``capture_once``, which spools). Saying "the
    daemon is not running" is the honest answer; a silent empty page is not."""
    missing = tmp_path / "absent.sock"
    cli_daemon(_settings(tmp_path, missing))

    assert await cli._run(["health"]) == 1
    assert "DaemonUnreachableError" in capsys.readouterr().err


async def test_mu_health_surfaces_the_unwired_service_by_name(
    daemon_factory: Any, cli_daemon: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    daemon = await daemon_factory()
    cli_daemon(daemon.settings)
    assert await cli._run(["health"]) == 1
    assert HEALTH_UNWIRED in capsys.readouterr().err


async def test_a_wrong_shaped_200_reply_is_a_named_refusal_not_a_traceback(
    tmp_path: Path, cli_daemon: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    """The renderers used to index the reply dict directly (``payload['memory_id']``,
    ``entry['tier']``), and ``cli_error_boundary`` re-raises anything outside the
    ``MemoryUniverseError`` hierarchy — so a 200 of the wrong shape became a raw ``KeyError``
    traceback instead of the single content-free line the CLI promises. Rendering through mu-core's
    frozen contracts closes the whole class.

    A version-skewed daemon is the realistic way to get here: this CLI and the running daemon are
    not required to be the same build.
    """
    socket_path = tmp_path / "skewed.sock"

    async def _answer_wrong_shape(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        await r.readline()
        w.write((json.dumps({"status": 200, "unexpected": "shape"}) + "\n").encode("utf-8"))
        await w.drain()
        w.close()

    server = await asyncio.start_unix_server(_answer_wrong_shape, path=str(socket_path))
    try:
        cli_daemon(_settings(tmp_path, socket_path))
        assert await cli._run(["health"]) == 1
        err = capsys.readouterr().err
        assert DaemonReplyInvalidError.__name__ in err
        assert "MemoryHealthView" in err
        assert "Traceback" not in err
    finally:
        server.close()
        await server.wait_closed()


async def test_the_ipc_client_refuses_an_absent_socket(tmp_path: Path) -> None:
    client = IpcClient(DaemonIpcSettings(socket_path=tmp_path / "nothing.sock"))
    with pytest.raises(DaemonUnreachableError, match="mu daemon run"):
        await client.request(HEALTH_ROUTE, {})


async def test_the_ipc_client_refuses_a_socket_that_answers_nothing(tmp_path: Path) -> None:
    """A close with no reply is indistinguishable from success on the wire — the exact failure
    mode that once dropped captures silently. Here it must be a loud refusal."""
    socket_path = tmp_path / "mute.sock"

    async def _hangup(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        # READ the request first, so the client's write+drain both SUCCEED and the only thing
        # missing is the reply itself — otherwise the client fails on a reset mid-drain and this
        # test would pass without ever exercising the empty-reply branch.
        await r.readline()
        w.close()

    server = await asyncio.start_unix_server(_hangup, path=str(socket_path))
    try:
        client = IpcClient(DaemonIpcSettings(socket_path=socket_path))
        with pytest.raises(DaemonUnreachableError):
            await client.request(HEALTH_ROUTE, {"namespace": list(_ns().parts())})
    finally:
        server.close()
        await server.wait_closed()
