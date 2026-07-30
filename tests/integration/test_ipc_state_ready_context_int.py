"""S3-03 acceptance — ``IpcServer``'s new ``/state``/``/ready-context`` routes over the REAL unix
socket, against a REAL ``MemoryLifecycleManager`` (S1-03) + REAL backing stores, ZERO mocks
(DEV-STANDARDS non-negotiable).

Authority: ``docs/superpowers/design/memory-lifecycle-manager-spec.md`` §5 (lines 247-252 —
"the existing loopback IPC socket serves ``/state``, ``/recall``, ``/ready-context`` as instant
warm-cache reads that never touch the runner"); ADR 0033 leg 2 ("always-accessible").

**Why the sweep is forced to be genuinely slow (not a timing-luck race).** Directly-seeded STM
items (bypassing ``LocalMemoryHost.add()``'s own auto-promote-on-ingest — the SAME technique
``tests/integration/test_maintenance_int.py`` already uses against the real ``mu-dev-cache`` Redis)
carry ``importance_score=1.0``, pushing every item PAST ``PromotionService``'s
``promote_stm_mtm=0.7`` gate (``w_recency=0.5 * rec(~1.0, fresh) + w_importance=0.3 * imp(1.0)
= 0.8``). That makes the triggered sweep do REAL work: STM->MTM ``MtmTierRepository.upsert``
(real Qdrant) for every item, then ``DistillPipeline.distill`` (real FalkorDB LTM write + a real
call to the SLM sidecar, Ollama ``qwen2.5:0.5b``, for extraction/adjudication) — multi-item real
network + LLM latency, not a single sub-millisecond Redis round trip. ``MemoryLifecycleManager``'s
default runner (``_InlineLifecycleRunner``) executes the job's body INSIDE ``submit()``, so the
whole ``sweep_user(...)`` coroutine — run as a background ``asyncio.Task`` here — stays not-``done``
for the ENTIRE real sweep duration; the concurrent ``/state``/``/ready-context`` IPC calls below
race against something that provably takes much longer than a socket round trip, not something
that might already have finished.

If a required container (mu-dev-cache/mu-dev-qdrant/mu-dev-falkordb) or the SLM sidecar is down,
this test RAISES via the real client's own connection error — never faked.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio
from falkordb.asyncio import FalkorDB
from mu_contracts.domain.events import MemoryCaptured
from mu_contracts.domain.model.lifecycle import UserPrefix
from mu_contracts.domain.model.memory import Namespace, Visibility
from mu_engine.storage.adapters.redis_stm import RedisStmAdapter
from mu_engine.storage.domain.memory import MemoryItem, MemoryTier
from mu_engine.storage.mappers.redis_mapper import RedisMapper
from qdrant_client import AsyncQdrantClient
from redis.asyncio import Redis

from mu_client.capture.parsers import ParserRegistry
from mu_client.config import ClientSettings, DaemonIpcSettings, InjectSettings, OutboxSettings
from mu_client.daemon.ipc import IpcServer
from mu_client.host import LocalMemoryHost
from mu_client.inject.recall_bridge import RecallInjectBridge
from mu_client.outbox.sqlite_outbox import SqliteOutbox

pytestmark = pytest.mark.integration

_SESSION = "state-sweep-s1"
_NUM_ITEMS = 8  # enough real MTM/LTM/SLM round trips to make the sweep observably slower than
#                 a single unix-socket JSON exchange (see module docstring)


def _ns(settings: ClientSettings, session: str) -> Namespace:
    return Namespace(
        org=settings.default_namespace,
        workspace=settings.default_workspace,
        user=settings.default_user,
        session=session,
        visibility=Visibility.PRIVATE,
    )


async def _teardown(settings: ClientSettings, uid: str) -> None:
    """Drop every qdrant collection / falkordb graph / redis key this test's isolated η partition
    created — duplicated from ``test_capture_daemon_int.py`` (no shared test-util package exists
    yet across integration modules; both stay independently runnable)."""
    qdrant = AsyncQdrantClient(url=settings.storage.vector.url)
    try:
        for coll in (await qdrant.get_collections()).collections:
            if uid in coll.name:
                with contextlib.suppress(Exception):
                    await qdrant.delete_collection(coll.name)
    finally:
        await qdrant.close()

    db = FalkorDB(host=settings.storage.graph.host, port=settings.storage.graph.port)
    try:
        for g in await db.list_graphs():
            name = g.decode() if isinstance(g, bytes) else g
            if uid in name:
                with contextlib.suppress(Exception):
                    await db.select_graph(name).delete()
    finally:
        with contextlib.suppress(Exception):
            await db.connection.aclose()

    redis: Redis = Redis.from_url(settings.storage.cache.url, decode_responses=False)
    try:
        keys = [k async for k in redis.scan_iter(match=f"*{uid}*".encode())]
        if keys:
            await redis.delete(*keys)
    finally:
        await redis.aclose()


@pytest_asyncio.fixture
async def isolated_settings(
    client_settings: ClientSettings, uid: str, tmp_path: Path
) -> AsyncIterator[ClientSettings]:
    """Isolated η partition (uid-tagged workspace/namespace, cleaned up in the real stores) AND
    isolated on-disk paths — a SHORT flat ``/tmp`` socket path (``AF_UNIX``'s ``sun_path`` caps at
    ~108 bytes; pytest's own nested ``tmp_path`` routinely exceeds that). NEVER the user's real
    ``~/.memory-universe``; never collides with a real resident daemon."""
    socket_path = Path(f"/tmp/mu-test-state-{uid}.sock")  # noqa: S108 — deliberate, see docstring
    settings = client_settings.model_copy(
        update={
            "default_workspace": f"ws{uid}",
            "default_namespace": f"org{uid}",
            "outbox": OutboxSettings(outbox_path=tmp_path / "unused-outbox.sqlite"),
            "ipc": DaemonIpcSettings(socket_path=socket_path),
            "inject": InjectSettings(recall_dir=tmp_path / "recall"),
        }
    )
    try:
        yield settings
    finally:
        socket_path.unlink(missing_ok=True)
        await _teardown(settings, uid)


async def _send(socket_path: Path, payload: dict[str, object]) -> tuple[dict[str, object], float]:
    """One newline-delimited-JSON request/response round trip over the real IPC unix socket —
    the SAME wire shape ``IpcServer._handle``/``_dispatch`` already serve every other route with."""
    started = time.monotonic()
    reader, writer = await asyncio.open_unix_connection(str(socket_path))
    try:
        writer.write((json.dumps(payload) + "\n").encode("utf-8"))
        await writer.drain()
        line = await reader.readline()
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
    elapsed = time.monotonic() - started
    response: dict[str, object] = json.loads(line)
    return response, elapsed


async def test_state_and_ready_context_unblocked_during_real_sweep(
    isolated_settings: ClientSettings, uid: str
) -> None:
    ns = _ns(isolated_settings, _SESSION)

    # (1) SEED real STM directly against the real mu-dev-cache Redis — see module docstring for
    # why importance_score=1.0 (forces real STM->MTM->distill work, not a same-order-of-magnitude
    # single Redis read).
    redis: Redis = Redis.from_url(isolated_settings.storage.cache.url, decode_responses=True)
    stm = RedisStmAdapter(redis, mapper=RedisMapper(default_ttl_s=3600))
    items = [
        MemoryItem(
            content=f"Fact {i}: Ada worked on project P{i} while based in city C{i}",
            namespace=ns,
            owner_id=ns.user,
            workspace_id=ns.workspace,
            session_id=ns.session,
            tier=MemoryTier.STM,
            importance_score=1.0,
        )
        for i in range(_NUM_ITEMS)
    ]
    for item in items:
        await stm.put(item)

    # (2) REAL engine host + REAL MemoryLifecycleManager (S1-03) over it — same construction path
    # daemon/app.py's own integrate-phase wiring will use (host.build_lifecycle_manager()).
    host = LocalMemoryHost(isolated_settings)
    await host.start()
    mlm = host.build_lifecycle_manager()

    # Populate the active-namespace registry via the manager's own public event hook (real API,
    # spec §7 hybrid discovery) — a single event (< LifecycleSettings.batch_size) makes `ns` a
    # member of `sweep_user`'s per-prefix namespace set WITHOUT itself firing a sweep, exactly as
    # a real MemoryCaptured publish would.
    await mlm.on_bus_event(MemoryCaptured(namespace=ns, ids=[i.id for i in items]))

    # (3) THIS task's own IpcServer wiring: same registry/outbox/bridge shape every other route
    # already needs, PLUS the new `lifecycle_manager=` param.
    outbox = SqliteOutbox(isolated_settings.outbox.outbox_path)
    await outbox.open()
    bridge = RecallInjectBridge(host, settings=isolated_settings.inject)
    ipc = IpcServer(
        isolated_settings.ipc,
        registry=ParserRegistry(),
        outbox=outbox,
        bridge=bridge,
        lifecycle_manager=mlm,
    )
    await ipc.bind()

    try:
        prefix = UserPrefix(ns)
        sweep_started = time.monotonic()
        sweep_task = asyncio.create_task(mlm.sweep_user(prefix))
        await asyncio.sleep(0)  # let the sweep task start running before we race it

        # (4) /state — concurrent with the in-flight sweep.
        state_resp, state_elapsed = await _send(
            isolated_settings.ipc.socket_path,
            {"route": "state", "namespace": list(ns.parts())},
        )
        print(  # noqa: T201
            f"/state resp={state_resp!r} elapsed={state_elapsed:.4f}s "
            f"sweep_task.done()={sweep_task.done()} "
            f"t+{time.monotonic() - sweep_started:.4f}s since sweep started"
        )
        assert state_resp["status"] == 200
        assert state_resp["user_prefix"] == str(prefix)
        assert state_resp["stm_count"] == 0  # documented 0 stub this slice (manager.py docstring)
        assert (
            state_elapsed < 1.0
        ), "/state must be an instant warm read — it must not wait on the runner's sweep"
        assert not sweep_task.done(), (
            f"the real sweep over {_NUM_ITEMS} items (STM->MTM + real distill/SLM calls) finished "
            "before the concurrent /state call even returned — this run's timing margin was too "
            "tight to prove non-blocking (widen _NUM_ITEMS if this ever flakes on faster infra)"
        )

        # (5) /ready-context — same concurrent-with-sweep proof, over the same live socket.
        ready_resp, ready_elapsed = await _send(
            isolated_settings.ipc.socket_path,
            {"route": "ready-context", "session_id": _SESSION},
        )
        print(  # noqa: T201
            f"/ready-context resp={ready_resp!r} elapsed={ready_elapsed:.4f}s "
            f"sweep_task.done()={sweep_task.done()}"
        )
        assert ready_resp["status"] == 200
        assert ready_resp["session_id"] == _SESSION
        assert ready_resp["wired"] is False  # documented not-yet-wired stub (S3-02 not landed)
        assert ready_elapsed < 1.0

        # (6) Let the real sweep run to completion — never leave a background task orphaned, and
        # confirm it actually did the promised real work (never silently a no-op).
        handle = await sweep_task
        total_sweep_s = time.monotonic() - sweep_started
        print(f"sweep JobHandle={handle!r} total_sweep_s={total_sweep_s:.4f}")  # noqa: T201
        assert total_sweep_s > state_elapsed, "sanity: the sweep really did outlast the /state call"

        # (7) A THIRD /state call, now that the sweep is done, is still fast and no longer shows a
        # pending job for this prefix (pending_jobs cleared in _execute_sweep's finally).
        final_state_resp, _ = await _send(
            isolated_settings.ipc.socket_path,
            {"route": "state", "namespace": list(ns.parts())},
        )
        print(f"/state (post-sweep) resp={final_state_resp!r}")  # noqa: T201
        assert final_state_resp["status"] == 200
        assert final_state_resp["pending_job"] is None
        assert final_state_resp["last_swept_at"] is not None
    finally:
        await ipc.stop_accepting()
        await outbox.aclose()
        await host.aclose()
        await redis.aclose()


async def test_state_503_when_lifecycle_manager_not_wired(
    isolated_settings: ClientSettings,
) -> None:
    """The documented degrade (module docstring): a caller that has not threaded a
    ``MemoryLifecycleManager`` through yet (``lifecycle_manager=None``, the default — today's
    ``daemon/app.py`` ahead of its own integrate-phase wiring) gets a named 503, never a raise/hang,
    for BOTH new routes. No real store touched by this test — pure IPC-layer behaviour."""
    outbox = SqliteOutbox(isolated_settings.outbox.outbox_path)
    await outbox.open()
    host = LocalMemoryHost(isolated_settings)
    await host.start()
    bridge = RecallInjectBridge(host, settings=isolated_settings.inject)
    ipc = IpcServer(
        isolated_settings.ipc, registry=ParserRegistry(), outbox=outbox, bridge=bridge
    )  # lifecycle_manager NOT passed
    await ipc.bind()
    try:
        state_resp, _ = await _send(
            isolated_settings.ipc.socket_path,
            {"route": "state", "namespace": ["o", "w", "u", "s", "private"]},
        )
        print(f"/state (unwired) resp={state_resp!r}")  # noqa: T201
        assert state_resp == {"status": 503, "error": "lifecycle_manager_not_wired"}

        ready_resp, _ = await _send(
            isolated_settings.ipc.socket_path,
            {"route": "ready-context", "session_id": "s"},
        )
        print(f"/ready-context (unwired) resp={ready_resp!r}")  # noqa: T201
        assert ready_resp == {"status": 503, "error": "lifecycle_manager_not_wired"}
    finally:
        await ipc.stop_accepting()
        await outbox.aclose()
        await host.aclose()
