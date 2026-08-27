"""``MaintenanceLoop`` acceptance-style integration tests — REAL running containers, ZERO mocks
(DEV-STANDARDS non-negotiable): real ``mu-dev-cache`` (Valkey) STM reads/writes with real TTLs,
real wall-clock timing (``asyncio`` real sleeps, never a ``FrozenClock``), and a real daemon
(``LocalDaemon``) over its real unix socket for the backpressure measurement.

``MemoryLifecycleManager`` (S1-03) — the real sweep body — is a sibling Stage-1 task landing in
parallel with this one and does not exist in this repo yet (see ``daemon/maintenance.py``'s own
module docstring). Where an acceptance criterion needs a "sweep" to exercise, this file supplies a
narrow, honestly-labeled reference stand-in for exactly the mechanism under test — backed by the
REAL Redis STM store, never an in-memory fake of it — and states plainly, in each test's docstring,
which slice of the criterion it can prove today versus what the integrate phase must re-verify once
the real ``MemoryLifecycleManager``/``PromotionService``/``SqliteWalRunner`` land.

If a required container is unreachable, these tests RAISE (BLOCKED, never faked) via the real
client's own connection error — no try/except swallows a real dependency being down.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
import pytest_asyncio
from mu_contracts.domain.model.lifecycle import JobHandle, UserPrefix
from mu_engine.lifecycle.settings import LifecycleSettings
from mu_engine.platform.adapters.bus_inproc import InprocBus
from mu_engine.storage.adapters.redis_stm import RedisStmAdapter
from mu_engine.storage.domain.memory import MemoryItem, MemoryTier
from mu_engine.storage.domain.namespace import Namespace, Visibility
from mu_engine.storage.mappers.redis_mapper import RedisMapper
from redis.asyncio import Redis

from mu_client.capture.hook import capture_once
from mu_client.capture.model import HostKind
from mu_client.config import ClientSettings, DaemonIpcSettings, OutboxSettings
from mu_client.daemon.app import LocalDaemon
from mu_client.daemon.maintenance import MaintenanceLoop

pytestmark = pytest.mark.integration


def _ns(*, uid: str, user: str = "u1") -> Namespace:
    return Namespace(
        org=f"org{uid}",
        workspace=f"ws{uid}",
        user=user,
        session="s1",
        visibility=Visibility.PRIVATE,
    )


@pytest_asyncio.fixture
async def redis_client(client_settings: ClientSettings) -> AsyncIterator[Redis]:
    client: Redis = Redis.from_url(client_settings.storage.cache.url, decode_responses=True)
    try:
        yield client
    finally:
        await client.aclose()


async def _cleanup_ns(client: Redis, ns: Namespace) -> None:
    keys = [k async for k in client.scan_iter(match=f"*{ns.to_prefix()}*")]
    if keys:
        await client.delete(*keys)


# =====================================================================================
# BQ2 — durable consumed-offset cursor (spec §13.1). local_memory.py:138's
# ``stm.recent(ns, limit=limit)`` blind re-read is the EXACT bug: calling it twice always
# returns the SAME full window, so a naive re-sweep re-sees (and, downstream in DistillPipeline,
# re-reconciles) an item it already consolidated. The fix is a durable consumed-offset cursor
# (persisted in S1-06's SqliteWalRunner job log, `after_offset` on `LifecycleJob`, advanced on
# `ConsolidationCompleted`) that filters `stm.recent`'s result to items newer than the cursor.
#
# S1-03/S1-05/S1-06 (the real MemoryLifecycleManager / LocalMemory.consolidate() cursor wiring /
# durable job-log offset persistence) are sibling tasks not yet landed. This test proves the
# MECHANISM against the REAL Redis STM store this bug actually lives in: (a) reproduces the
# blind-re-read bug verbatim (`stm.recent(ns, limit=N)`, unchanged from `local_memory.py:138`)
# always re-returns an already-consumed item; (b) shows an offset-filtered read of the SAME real
# STM window does not. The integrate phase must re-run this class of assertion once the real
# cursor is durably persisted end-to-end (this test's cursor is in-process, not yet WAL-durable).
# =====================================================================================
class _CursorAwareConsolidateStub:
    """Reference stand-in for the *mechanism* spec §13.1 describes: filter ``stm.recent``'s
    result to items whose ``created_at`` is after the last consumed offset, then advance the
    offset. Implements :class:`~mu_client.daemon.maintenance.LifecycleManagerPort` so a real
    ``MaintenanceLoop`` can drive it exactly as it will drive the real MLM."""

    def __init__(self, stm: RedisStmAdapter, *, ns: Namespace) -> None:
        self._stm = stm
        self._ns = ns
        self._cursor: float = 0.0  # after_offset — a unix timestamp, spec §13.1
        self.seen_ids: list[str] = []  # every id this stub has EVER processed (never re-added)

    async def sweep_user(self, user_prefix: UserPrefix) -> JobHandle:
        del user_prefix  # single-namespace stub; the real MLM keys per (org,workspace,user)
        window = await self._stm.recent(self._ns, limit=50)  # SAME call as local_memory.py:138
        newly_consumed = [
            scored.item for scored in window if scored.item.created_at.timestamp() > self._cursor
        ]
        for item in newly_consumed:
            self.seen_ids.append(item.id)
        if newly_consumed:
            self._cursor = max(i.created_at.timestamp() for i in newly_consumed)
        return JobHandle(job_id=f"stub-{len(self.seen_ids)}", submitted_at=datetime.now(UTC))


def _item(*, ns: Namespace, content: str) -> MemoryItem:
    return MemoryItem(
        content=content,
        namespace=ns,
        owner_id=ns.user,
        workspace_id=ns.workspace,
        session_id=ns.session,
        tier=MemoryTier.STM,
    )


@pytest.mark.asyncio
async def test_naive_blind_reread_reproduces_bq2_against_real_stm(
    redis_client: Redis, uid: str
) -> None:
    """Reproduces the BQ2 regression VERBATIM: ``stm.recent(ns, limit=N)`` called twice in a row
    over an unchanged real STM window always returns the SAME item both times — the literal
    mechanism that flips an already-SUPERSEDED fact back to ADD in ``LocalMemory.consolidate()``
    (``local_memory.py:138``) once the downstream DistillPipeline re-reconciles it."""
    ns = _ns(uid=uid)
    stm = RedisStmAdapter(redis_client, mapper=RedisMapper(default_ttl_s=120))
    try:
        item = _item(ns=ns, content="Ada lives in Paris")
        await stm.put(item)

        first_read = await stm.recent(ns, limit=50)
        second_read = await stm.recent(ns, limit=50)  # the "sweep runs again" moment

        first_ids = {s.item.id for s in first_read}
        second_ids = {s.item.id for s in second_read}
        assert item.id in first_ids
        assert item.id in second_ids, (
            "this IS the bug: a blind stm.recent() re-read re-surfaces an already-consumed item "
            "on the very next call over an unchanged window"
        )
    finally:
        await _cleanup_ns(redis_client, ns)


@pytest.mark.asyncio
async def test_cursor_aware_sweep_does_not_reprocess_an_already_consumed_item(
    redis_client: Redis, uid: str
) -> None:
    """The fix's mechanism (spec §13.1): call ``sweep_user`` twice in a row over the same real STM
    window (first with only item1 present, second with item2 additionally present) and confirm
    item1 is consumed exactly once — never re-added to ``seen_ids`` on the second call, which is
    the observable proxy for "does not re-flip an already-SUPERSEDED fact back to ADD"."""
    ns = _ns(uid=uid)
    stm = RedisStmAdapter(redis_client, mapper=RedisMapper(default_ttl_s=120))
    stub = _CursorAwareConsolidateStub(stm, ns=ns)
    bus = InprocBus()
    settings = LifecycleSettings(batch_size=10_000)  # fast-fire never trips; MaintenanceLoop is
    #                                                   only used here to drive sweep_user exactly
    #                                                   the way the real daemon would.
    loop = MaintenanceLoop(bus=bus, lifecycle_manager=stub, settings=settings)
    try:
        item1 = _item(ns=ns, content="Ada lives in Paris")
        await stm.put(item1)
        await loop._fire_sweep(UserPrefix(ns))  # "sweep #1" — the same call site fast-fire uses
        assert stub.seen_ids == [item1.id]

        item2 = _item(ns=ns, content="Ada works at Acme")
        await stm.put(item2)
        await loop._fire_sweep(UserPrefix(ns))  # "sweep #2" over the same (now-larger) STM window

        assert stub.seen_ids.count(item1.id) == 1, (
            "item1 must be consumed exactly once across both sweeps — a second appearance means "
            "the cursor failed to filter it out (the BQ2 re-flip class of bug)"
        )
        assert stub.seen_ids.count(item2.id) == 1
    finally:
        await _cleanup_ns(redis_client, ns)


# =====================================================================================
# AC-1.3a — pre-TTL rescue cadence lands inside a REAL Redis TTL window.
#
# Production defaults (pre_ttl_scan_interval_s=120, pre_ttl_window_s=300) would need a 5+ minute
# real-time wait per test run — impractical at component-test speed. This test proves the SAME
# invariant class the spec derives (`pre_ttl_scan_interval_s <= pre_ttl_window_s / 2` guarantees
# `floor(window/interval)` scan ticks land inside the window) against a REAL Redis-backed item
# with a REAL TTL and REAL wall-clock `asyncio` timing (never a mocked/frozen clock) at a scaled
# cadence preserving the same mathematical shape. The scan LOOP is the identical code path
# (`MaintenanceLoop._pre_ttl_loop`, `asyncio.wait_for` against `LifecycleSettings.
# pre_ttl_scan_interval_s`) regardless of the configured numbers — what changes at production
# defaults is only the wall-clock duration, not the mechanism under test.
# =====================================================================================
@pytest.mark.asyncio
async def test_pre_ttl_loop_lands_at_least_floor_window_over_interval_ticks_before_real_ttl_delete(
    redis_client: Redis, uid: str
) -> None:
    ns = _ns(uid=uid)
    window_s = 6
    interval_s = 2  # 2 <= 6/2=3 -> invariant holds; guarantees floor(6/2)=3 ticks
    stm = RedisStmAdapter(redis_client, mapper=RedisMapper(default_ttl_s=window_s))

    tick_observations: list[tuple[float, bool]] = []  # (t_since_put, item_still_present)
    t0 = time.monotonic()

    class _PreTtlObserverStub:
        async def sweep_user(self, user_prefix: UserPrefix) -> JobHandle:
            del user_prefix
            present = (await stm.get(ns, item.id)) is not None
            tick_observations.append((time.monotonic() - t0, present))
            return JobHandle(job_id="obs", submitted_at=datetime.now(UTC))

    item = _item(ns=ns, content="Ada lives in Paris")
    try:
        await stm.put(item)

        bus = InprocBus()
        settings = LifecycleSettings(
            maintenance_interval_s=3600,  # never fires in this test's short window
            pre_ttl_scan_interval_s=interval_s,
            pre_ttl_window_s=window_s,
            batch_size=10_000,  # fast-fire never trips
        )
        loop = MaintenanceLoop(bus=bus, lifecycle_manager=_PreTtlObserverStub(), settings=settings)
        loop._active_users[UserPrefix(ns)] = time.monotonic()  # register without a bus event

        run_task = asyncio.create_task(loop.run())
        await asyncio.sleep(window_s + 1.5)  # run past real TTL expiry
        await loop.stop()
        await run_task

        print(f"PRE-TTL TICK OBSERVATIONS (t_s, item_present) = {tick_observations}")  # noqa: T201
        assert len(tick_observations) >= 3, (
            f"expected >= floor({window_s}/{interval_s})=3 pre-TTL scan ticks, "
            f"got {len(tick_observations)}: {tick_observations}"
        )
        present_ticks = [present for _, present in tick_observations if present]
        assert len(present_ticks) >= 3, (
            "the pre-TTL loop must observe the item PRESENT (inside its rescue window) at least "
            f"floor(window/interval)=3 times before Redis deletes it — got {tick_observations}"
        )

        # Real TTL eventually deletes it — Redis, not this loop, is the source of truth.
        assert (
            await stm.get(ns, item.id)
        ) is None, "real Redis TTL never actually expired the item"
    finally:
        await _cleanup_ns(redis_client, ns)


# =====================================================================================
# AC-1.2 — capture-ack p99 latency delta while MaintenanceLoop concurrently runs, real daemon,
# real timing.
# =====================================================================================
_SESSION = "maint-ac12"


async def _teardown_daemon_ns(settings: ClientSettings, uid: str) -> None:
    redis: Redis = Redis.from_url(settings.storage.cache.url, decode_responses=False)
    try:
        keys = [k async for k in redis.scan_iter(match=f"*{uid}*".encode())]
        if keys:
            await redis.delete(*keys)
    finally:
        await redis.aclose()


def _isolated_settings(client_settings: ClientSettings, tag: str, tmp_path: Path) -> ClientSettings:
    socket_path = Path(f"/tmp/mu-maint-test-{tag}.sock")  # noqa: S108 — mirrors
    # test_capture_daemon_int.py's isolated_settings: AF_UNIX sun_path is capped ~108 bytes.
    return client_settings.model_copy(
        update={
            "default_workspace": f"ws{tag}",
            "default_namespace": f"org{tag}",
            "outbox": OutboxSettings(
                outbox_path=tmp_path / f"outbox-{tag}.sqlite", poll_interval_s=0.05, batch_size=64
            ),
            "ipc": DaemonIpcSettings(socket_path=socket_path),
        }
    )


async def _one_capture_ack_latency(
    settings: ClientSettings, daemon: LocalDaemon, *, tag: str
) -> float:
    raw = _hook_json(session_id=_SESSION, prompt=f"fact {tag} about Ada in Paris")
    started = time.monotonic()
    await capture_once(settings, host=HostKind.CLAUDE_CODE, raw=raw)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline and await daemon.outbox.outbox_depth() > 0:  # noqa: ASYNC110
        await asyncio.sleep(0.01)
    assert await daemon.outbox.outbox_depth() == 0, f"capture {tag} never drained"
    return time.monotonic() - started


def _hook_json(*, session_id: str, prompt: str) -> bytes:
    return json.dumps(
        {
            "hook_event_name": "UserPromptSubmit",
            "session_id": session_id,
            "cwd": "/home/user/D/mu_project/mu-client",
            "transcript_path": f"/home/user/.claude/projects/x/{session_id}.jsonl",
            "prompt": prompt,
        }
    ).encode("utf-8")


def _p99(values: list[float]) -> float:
    ordered = sorted(values)
    idx = min(len(ordered) - 1, int(round(0.99 * (len(ordered) - 1))))
    return ordered[idx]


@pytest.mark.asyncio
async def test_capture_ack_p99_within_budget_while_maintenance_loop_sweeps_many_active_users(
    client_settings: ClientSettings, tmp_path: Path
) -> None:
    """AC-1.2: capture-ack p99 latency with the sweep concurrently running must stay within
    ``LifecycleSettings.capture_ack_p99_delta_budget_ms`` of capture-ack p99 with no sweep running
    — real daemons, real unix sockets, real outbox/WorkerPool drain, real wall-clock timing.

    ``MemoryLifecycleManager`` doesn't exist yet (S1-03, parallel sibling task) — this test
    exercises the ACTUAL contention surface AC-1.2 is about regardless: ``MaintenanceLoop``'s own
    ``_sweep_active_users`` cooperative-yield loop, populated with ``max_users_per_sweep``
    synthetic active users so the periodic tick genuinely does ``max_users_per_sweep`` real
    ``asyncio`` scheduling round-trips (+ a real content-free ``structlog`` emission per user via
    the honest ``_UnwiredLifecycleManager`` degrade) concurrently with the real capture-ack path.
    Swapping in the real MLM only adds MORE per-user work on the far side of the identical
    scheduling loop — it cannot make this loop's own overhead measurement smaller; a follow-up
    re-run once S1-03 lands should re-verify the budget against the real sweep body too.

    Methodology note: the baseline and loaded daemons run CONCURRENTLY (not sequentially) and
    measurements are interleaved round-robin between them — an earlier version of this test ran
    them sequentially and measured "loaded" reliably FASTER than "baseline" (an artifact of
    process-wide warm-up — imported libs/JITed code/warm OS caches favoring whichever daemon runs
    second, not a real MaintenanceLoop effect). Interleaving removes that ordering bias: any
    process-wide drift affects both daemons' measurements at roughly the same wall-clock moments.
    """
    n = 15
    baseline_settings = _isolated_settings(client_settings, "base", tmp_path)
    loaded_settings = _isolated_settings(client_settings, "load", tmp_path)

    baseline_daemon = LocalDaemon(baseline_settings)
    loaded_daemon = LocalDaemon(loaded_settings)
    try:
        async with baseline_daemon.lifespan(), loaded_daemon.lifespan():
            assert baseline_daemon._maintenance is not None
            assert loaded_daemon._maintenance is not None
            baseline_daemon._maintenance._settings = LifecycleSettings(
                maintenance_interval_s=3600, pre_ttl_scan_interval_s=3600
            )
            loop = loaded_daemon._maintenance
            sweep_settings = LifecycleSettings(
                maintenance_interval_s=3600, pre_ttl_scan_interval_s=5
            )
            loop._settings = sweep_settings
            for i in range(sweep_settings.max_users_per_sweep):
                loop._active_users[
                    UserPrefix(
                        Namespace(
                            org=loaded_settings.default_namespace,
                            workspace=loaded_settings.default_workspace,
                            user=f"synthetic-{i}-{uuid.uuid4().hex[:6]}",
                            session="s",
                            visibility=Visibility.PRIVATE,
                        )
                    )
                ] = time.monotonic()

            # Warm up both daemons once each (discarded) before interleaved timed rounds.
            await _one_capture_ack_latency(baseline_settings, baseline_daemon, tag="warmup")
            await _one_capture_ack_latency(loaded_settings, loaded_daemon, tag="warmup")

            baseline_latencies: list[float] = []
            loaded_latencies: list[float] = []
            for i in range(n):
                baseline_latencies.append(
                    await _one_capture_ack_latency(baseline_settings, baseline_daemon, tag=f"b{i}")
                )
                loaded_latencies.append(
                    await _one_capture_ack_latency(loaded_settings, loaded_daemon, tag=f"l{i}")
                )
    finally:
        baseline_settings.ipc.socket_path.unlink(missing_ok=True)
        loaded_settings.ipc.socket_path.unlink(missing_ok=True)
        await _teardown_daemon_ns(baseline_settings, "base")
        await _teardown_daemon_ns(loaded_settings, "load")

    base_p99_ms = _p99(baseline_latencies) * 1000
    loaded_p99_ms = _p99(loaded_latencies) * 1000
    delta_ms = loaded_p99_ms - base_p99_ms
    budget_ms = LifecycleSettings().capture_ack_p99_delta_budget_ms

    print(  # noqa: T201
        f"AC-1.2 capture-ack p99 (interleaved): baseline={base_p99_ms:.1f}ms "
        f"loaded={loaded_p99_ms:.1f}ms delta={delta_ms:.1f}ms budget={budget_ms}ms "
        f"(baseline_all={[f'{x * 1000:.1f}' for x in baseline_latencies]}, "
        f"loaded_all={[f'{x * 1000:.1f}' for x in loaded_latencies]})"
    )
    assert delta_ms <= budget_ms, (
        f"capture-ack p99 delta {delta_ms:.1f}ms exceeds "
        f"capture_ack_p99_delta_budget_ms={budget_ms}ms "
        f"(baseline p99={base_p99_ms:.1f}ms, loaded p99={loaded_p99_ms:.1f}ms)"
    )
