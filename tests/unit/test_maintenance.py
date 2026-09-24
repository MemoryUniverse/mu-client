"""``MaintenanceLoop`` isolated logic — mocks/stubs permitted (DEV-STANDARDS: mocks ONLY in pure
unit tests). Uses a REAL ``InprocBus`` throughout (trivial, real, no reason to fake it — "not the
bus" per DEV-STANDARDS applies most strongly to integration tests, but there is no upside to
faking a two-method in-process dispatcher here either). ``LifecycleManagerPort`` is satisfied by a
tiny recording stub: the real ``MemoryLifecycleManager`` (S1-03) is a sibling Stage-1 task landing
in parallel with this one and does not exist in this repo yet — see ``daemon/maintenance.py``'s own
module docstring for the Protocol-decoupling rationale. Real-timing/real-store acceptance-style
tests (AC-1.2, AC-1.3a, the BQ2 cursor regression) live in ``tests/integration/
test_maintenance_int.py``.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from mu_contracts.domain.events import DegradeReason, MemoryCaptured, MemoryPromoted
from mu_contracts.domain.model.lifecycle import JobHandle, UserPrefix
from mu_contracts.domain.model.memory import Namespace, Tier, Visibility
from mu_engine.lifecycle.settings import LifecycleSettings
from mu_engine.platform.adapters.bus_inproc import InprocBus

from mu_client.daemon.maintenance import (
    MaintenanceEnvSettings,
    MaintenanceLoop,
    _UnwiredLifecycleManager,
)

pytestmark = pytest.mark.unit


def _ns(*, user: str, session: str = "s1", workspace: str = "ws", org: str = "org") -> Namespace:
    return Namespace(
        org=org, workspace=workspace, user=user, session=session, visibility=Visibility.PRIVATE
    )


class _RecordingLifecycleManager:
    """A minimal :class:`~mu_client.daemon.maintenance.LifecycleManagerPort` stub — records every
    ``sweep_user`` call (user + monotonic order) AND every ``rescue_pre_ttl_user`` call
    SEPARATELY (FAULT-HUNT-0924 F5 fix, ADR 0054: the two are no longer the same call, and a test
    that cannot tell them apart cannot tell the defect the fix removed from a working system —
    see ``test_pre_ttl_loop_calls_rescue_pre_ttl_user_never_the_full_sweep`` below), optionally
    gated by an ``asyncio.Event`` so a test can hold a "call in flight" window open to exercise
    the coalescing floor."""

    def __init__(self, *, gate: asyncio.Event | None = None) -> None:
        self.calls: list[UserPrefix] = []
        self.rescue_calls: list[UserPrefix] = []
        self._gate = gate

    async def sweep_user(self, user_prefix: UserPrefix) -> JobHandle:
        if self._gate is not None:
            await self._gate.wait()
        self.calls.append(user_prefix)
        return JobHandle(job_id=f"job-{len(self.calls)}", submitted_at=datetime.now(UTC))

    async def rescue_pre_ttl_user(self, user_prefix: UserPrefix) -> JobHandle:
        if self._gate is not None:
            await self._gate.wait()
        self.rescue_calls.append(user_prefix)
        return JobHandle(
            job_id=f"rescue-job-{len(self.rescue_calls)}", submitted_at=datetime.now(UTC)
        )


@pytest.fixture
def bus() -> InprocBus:
    return InprocBus()


@pytest.fixture
def fast_settings() -> LifecycleSettings:
    """batch_size small + both cadences short so unit tests run in well under a second."""
    return LifecycleSettings(
        batch_size=3, maintenance_interval_s=3600, pre_ttl_scan_interval_s=3600
    )


# --------------------------------------------------------------------------- fast-fire (event)
async def test_fast_fire_triggers_sweep_user_at_batch_size(
    bus: InprocBus, fast_settings: LifecycleSettings
) -> None:
    mlm = _RecordingLifecycleManager()
    loop = MaintenanceLoop(bus=bus, lifecycle_manager=mlm, settings=fast_settings)
    loop._subscribe()  # exercise the bus wiring directly, no run()/stop() needed for this assertion

    ns = _ns(user="alice")
    for i in range(fast_settings.batch_size - 1):
        await bus.publish(MemoryCaptured(namespace=ns, ids=[f"m{i}"], tier=Tier.STM))
    assert mlm.calls == [], "must not fire before batch_size is reached"

    await bus.publish(MemoryCaptured(namespace=ns, ids=["m-last"], tier=Tier.STM))
    assert len(mlm.calls) == 1
    assert mlm.calls[0] == UserPrefix(ns)
    assert loop.fast_fire_count == 1

    await loop._unsubscribe()


async def test_fast_fire_counters_are_per_user_isolated(
    bus: InprocBus, fast_settings: LifecycleSettings
) -> None:
    mlm = _RecordingLifecycleManager()
    loop = MaintenanceLoop(bus=bus, lifecycle_manager=mlm, settings=fast_settings)
    loop._subscribe()

    alice, bob = _ns(user="alice"), _ns(user="bob")
    # alice gets batch_size-1 events (no fire); bob gets a full batch_size (fires) — alice's
    # count must be untouched by bob's events and vice versa.
    for i in range(fast_settings.batch_size - 1):
        await bus.publish(MemoryCaptured(namespace=alice, ids=[f"a{i}"], tier=Tier.STM))
    for i in range(fast_settings.batch_size):
        await bus.publish(MemoryCaptured(namespace=bob, ids=[f"b{i}"], tier=Tier.STM))

    assert mlm.calls == [UserPrefix(bob)]
    await loop._unsubscribe()


async def test_fast_fire_counter_resets_after_firing_so_a_second_batch_fires_again(
    bus: InprocBus, fast_settings: LifecycleSettings
) -> None:
    mlm = _RecordingLifecycleManager()
    loop = MaintenanceLoop(bus=bus, lifecycle_manager=mlm, settings=fast_settings)
    loop._subscribe()
    ns = _ns(user="alice")

    for _ in range(2 * fast_settings.batch_size):
        await bus.publish(MemoryCaptured(namespace=ns, ids=["m"], tier=Tier.STM))

    assert len(mlm.calls) == 2, "the counter must reset to 0 after firing, not go negative/skip"
    await loop._unsubscribe()


async def test_memory_promoted_events_also_feed_the_batch_counter(
    bus: InprocBus, fast_settings: LifecycleSettings
) -> None:
    mlm = _RecordingLifecycleManager()
    loop = MaintenanceLoop(bus=bus, lifecycle_manager=mlm, settings=fast_settings)
    loop._subscribe()
    ns = _ns(user="alice")

    for _ in range(fast_settings.batch_size):
        await bus.publish(
            MemoryPromoted(namespace=ns, id="m1", frm=Tier.STM, to=Tier.MTM, reason="test")
        )

    assert len(mlm.calls) == 1
    await loop._unsubscribe()


# ------------------------------------------------------------------------------- coalescing
async def test_coalescing_never_launches_two_concurrent_sweeps_for_the_same_user(
    bus: InprocBus, fast_settings: LifecycleSettings
) -> None:
    """Spec §7: "A running sweep coalesces new triggers rather than stacking." Hold the first
    sweep open (gate), cross the batch threshold again while it is in flight, and confirm the
    second crossing is coalesced (counted, not a second concurrent ``sweep_user`` call)."""
    gate = asyncio.Event()
    mlm = _RecordingLifecycleManager(gate=gate)
    loop = MaintenanceLoop(bus=bus, lifecycle_manager=mlm, settings=fast_settings)
    loop._subscribe()
    ns = _ns(user="alice")

    # Cross the threshold once — the batch_size-th publish blocks inside sweep_user on `gate`
    # (``bus.publish`` awaits its handler directly), so drive the whole batch as a background
    # task to keep the test able to publish more events concurrently.
    async def _publish_first_batch() -> None:
        for i in range(fast_settings.batch_size):
            await bus.publish(MemoryCaptured(namespace=ns, ids=[f"m{i}"], tier=Tier.STM))

    first_task: asyncio.Task[None] = asyncio.create_task(_publish_first_batch())
    await asyncio.sleep(0.05)  # let the fast-fire handler reach the gate
    assert loop._inflight == {UserPrefix(ns)}

    # Cross the threshold a second time WHILE the first sweep is still gated open.
    for i in range(fast_settings.batch_size):
        await bus.publish(MemoryCaptured(namespace=ns, ids=[f"n{i}"], tier=Tier.STM))
    assert loop.coalesced_count >= 1, "a same-user re-cross while in flight must be coalesced"
    assert len(mlm.calls) == 0, "sweep_user must not have completed yet (still gated)"

    gate.set()  # release the first sweep
    await first_task
    await asyncio.sleep(0.05)
    assert len(mlm.calls) == 1, "exactly one sweep_user call must have actually executed"
    await loop._unsubscribe()


# ------------------------------------------------------ AD-268 discovery (D5 fix, ADR 0075)
class _StubUserRegistry:
    """A minimal ``UserPrefixRegistryPort`` — records every ``list_user_prefixes`` call so a test
    can assert the discovery cadence, and can be told to raise (transient store fault) on demand."""

    def __init__(self, prefixes: list[UserPrefix] | None = None, *, raises: bool = False) -> None:
        self.prefixes = prefixes or []
        self.raises = raises
        self.call_count = 0

    async def list_user_prefixes(self, *, limit: int) -> list[UserPrefix]:
        self.call_count += 1
        if self.raises:
            raise ConnectionError("simulated transient registry fault")
        return list(self.prefixes)[:limit]


async def test_run_reseeds_active_users_from_the_durable_registry_before_any_periodic_tick(
    bus: InprocBus,
) -> None:
    """PROTOTYPE-DEBT-0924.md D5's own PROBE 2, proved as a fix: a ``MaintenanceLoop`` built over
    a registry that ALREADY durably knows about a user — no bus event for that user in THIS
    process — must show that user in :attr:`active_user_count` immediately (before either
    periodic loop's first tick even runs), and the very next maintenance tick must sweep them.

    **MUTATION:** remove the ``await self._discover_known_users()`` call from ``run()`` -> this
    test goes RED on both asserts (pre-fix behaviour: ``active_user_count == 0``, ``mlm.calls ==
    []``, exactly PROBE 2's measured `active_user_count 0, sweep_user calls 0`)."""
    ns = _ns(user="restarted-user")
    registry = _StubUserRegistry(prefixes=[UserPrefix(ns)])
    settings = LifecycleSettings(maintenance_interval_s=3600, pre_ttl_scan_interval_s=3600)
    mlm = _RecordingLifecycleManager()
    loop = MaintenanceLoop(
        bus=bus, lifecycle_manager=mlm, settings=settings, user_registry=registry
    )

    run_task = asyncio.create_task(loop.run())
    try:
        await asyncio.sleep(0.05)
        assert loop.active_user_count == 1, "a durably-registered user must be found on restart"
        assert UserPrefix(ns) in mlm.calls, (
            "the FIRST maintenance tick (body-first, immediate) must sweep a user discovered from "
            "durable storage, exactly as it would sweep one discovered from a bus event"
        )
    finally:
        await loop.stop()
        await run_task


async def test_discovery_with_no_registry_is_a_safe_no_op(bus: InprocBus) -> None:
    """The pre-fix behaviour (bus-events-only) must be exactly preserved when the bound STM
    backend does not support the registry (``user_registry=None``, the default) — discovery must
    never itself be a reason the loop cannot start."""
    settings = LifecycleSettings(maintenance_interval_s=3600, pre_ttl_scan_interval_s=3600)
    loop = MaintenanceLoop(
        bus=bus, lifecycle_manager=_RecordingLifecycleManager(), settings=settings
    )

    run_task = asyncio.create_task(loop.run())
    try:
        await asyncio.sleep(0.05)
        assert loop.active_user_count == 0
        assert loop.discovery_tick_count == 0
    finally:
        await loop.stop()
        await run_task


async def test_discovery_read_failure_never_crashes_the_loop(bus: InprocBus) -> None:
    """Best-effort by design (module docstring): a transient registry fault must not propagate
    out of the supervised ``run()`` task and take the whole daemon's TaskGroup down with it."""
    settings = LifecycleSettings(maintenance_interval_s=3600, pre_ttl_scan_interval_s=3600)
    registry = _StubUserRegistry(raises=True)
    loop = MaintenanceLoop(
        bus=bus,
        lifecycle_manager=_RecordingLifecycleManager(),
        settings=settings,
        user_registry=registry,
    )

    run_task = asyncio.create_task(loop.run())
    try:
        await asyncio.sleep(0.05)
        assert not run_task.done(), "a registry read fault must not crash the supervised task"
        assert registry.call_count >= 1
        assert loop.active_user_count == 0
    finally:
        await loop.stop()
        await run_task


async def test_discovery_never_overwrites_a_fresher_in_process_touched_at(bus: InprocBus) -> None:
    """A user this process has ALREADY observed via a real bus event keeps its own state —
    discovery only ever ADDS a prefix the process has not seen yet (``setdefault``, never a raw
    assignment that could stomp fresher in-process activity with a staler durable timestamp)."""
    ns = _ns(user="already-active")
    registry = _StubUserRegistry(prefixes=[UserPrefix(ns)])
    settings = LifecycleSettings(
        maintenance_interval_s=3600, pre_ttl_scan_interval_s=3600, batch_size=1000
    )
    mlm = _RecordingLifecycleManager()
    loop = MaintenanceLoop(
        bus=bus, lifecycle_manager=mlm, settings=settings, user_registry=registry
    )
    loop._subscribe()
    await bus.publish(MemoryCaptured(namespace=ns, ids=["m1"], tier=Tier.STM))
    touched_at_before = loop._active_users[UserPrefix(ns)]

    await loop._discover_known_users()

    assert loop._active_users[UserPrefix(ns)] == touched_at_before
    await loop._unsubscribe()


async def test_full_scan_loop_ticks_on_its_own_cadence(bus: InprocBus) -> None:
    """The D5 backstop is a genuinely independent 4th cadence, not folded into either lifecycle
    loop — same "ticks repeatedly on its own short interval while the others stay far outside
    their window" proof :meth:`test_both_periodic_loops_tick_independently_at_their_own_cadence`
    already uses for the pre-TTL loop."""
    settings = LifecycleSettings(
        maintenance_interval_s=3600, pre_ttl_scan_interval_s=3600, full_scan_interval_s=1
    )
    registry = _StubUserRegistry()
    loop = MaintenanceLoop(
        bus=bus,
        lifecycle_manager=_RecordingLifecycleManager(),
        settings=settings,
        user_registry=registry,
    )

    run_task = asyncio.create_task(loop.run())
    try:
        await asyncio.sleep(2.2)  # >= 2 ticks on a 1s cadence
    finally:
        await loop.stop()
        await run_task

    assert loop.discovery_tick_count >= 2
    assert loop.maintenance_tick_count == 1, "the OTHER two cadences must not have sped up"
    assert loop.pre_ttl_tick_count == 1


# ---------------------------------------------------------------------------- env settings
def test_maintenance_env_settings_reads_mu_lifecycle_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mirrors MMA's demo cadence-override pattern (``controller.py:405,412``,
    ``MU_LIFECYCLE__MAINTENANCE_INTERVAL_S=60``)."""
    monkeypatch.setenv("MU_LIFECYCLE__MAINTENANCE_INTERVAL_S", "60")
    monkeypatch.setenv("MU_LIFECYCLE__PRE_TTL_SCAN_INTERVAL_S", "7")
    monkeypatch.setenv("MU_LIFECYCLE__BATCH_SIZE", "5")

    settings = MaintenanceEnvSettings()

    assert settings.maintenance_interval_s == 60
    assert settings.pre_ttl_scan_interval_s == 7
    assert settings.batch_size == 5


def test_maintenance_env_settings_defaults_match_lifecycle_settings_byte_for_byte() -> None:
    """No env override -> reproduces ``LifecycleSettings()``'s own defaults exactly (no duplicated
    literal drifted from the canonical S0-07 field set)."""
    canonical = LifecycleSettings()
    env_backed = MaintenanceEnvSettings()

    assert env_backed.maintenance_interval_s == canonical.maintenance_interval_s
    assert env_backed.pre_ttl_scan_interval_s == canonical.pre_ttl_scan_interval_s
    assert env_backed.batch_size == canonical.batch_size
    assert env_backed.max_users_per_sweep == canonical.max_users_per_sweep


# --------------------------------------------------------------------------- periodic cadences
async def test_both_periodic_loops_tick_independently_at_their_own_cadence(bus: InprocBus) -> None:
    """§7b MAJOR-4 fix, in miniature: two DIFFERENT short intervals, run concurrently, each ticks
    at its OWN cadence — proves the decoupling (not one loop driving both)."""
    settings = LifecycleSettings(maintenance_interval_s=100, pre_ttl_scan_interval_s=1)
    mlm = _RecordingLifecycleManager()
    loop = MaintenanceLoop(bus=bus, lifecycle_manager=mlm, settings=settings)

    run_task = asyncio.create_task(loop.run())
    await asyncio.sleep(
        2.2
    )  # >= 2 pre-TTL ticks (interval=1s), 0 maintenance ticks (interval=100s)
    await loop.stop()
    await run_task

    assert loop.maintenance_tick_count == 1, "maintenance loop only fires its FIRST immediate tick"
    assert (
        loop.pre_ttl_tick_count >= 2
    ), "pre-TTL loop must have ticked repeatedly on its own 1s cadence"


async def test_periodic_loop_sweeps_active_users_registered_via_bus_events(
    bus: InprocBus,
) -> None:
    settings = LifecycleSettings(
        maintenance_interval_s=3600, pre_ttl_scan_interval_s=1, batch_size=1000
    )
    mlm = _RecordingLifecycleManager()
    loop = MaintenanceLoop(bus=bus, lifecycle_manager=mlm, settings=settings)

    run_task = asyncio.create_task(loop.run())
    await asyncio.sleep(0.05)
    ns = _ns(user="carol")
    await bus.publish(MemoryCaptured(namespace=ns, ids=["m1"], tier=Tier.STM))
    assert loop.active_user_count == 1
    assert mlm.calls == [], "batch_size=1000 must not fast-fire on a single event"

    await asyncio.sleep(1.3)  # >= 1 pre-TTL tick
    await loop.stop()
    await run_task

    # FAULT-HUNT-0924 F5 fix (ADR 0054): the pre-TTL loop calls the NARROW rescue verb, never the
    # full sweep — `mlm.calls` (sweep_user) must stay empty here (maintenance_interval_s=3600
    # never ticks in this window); only `mlm.rescue_calls` records the pre-TTL loop's own fire.
    assert UserPrefix(ns) in mlm.rescue_calls, "the pre-TTL loop must rescue the active user"
    assert mlm.calls == [], (
        "the pre-TTL loop must NEVER call the full sweep_user (F5: this was the defect — both "
        "periodic loops called the same full-sweep body)"
    )


async def test_pre_ttl_loop_never_calls_the_full_sweep(bus: InprocBus) -> None:
    """FAULT-HUNT-0924 F5 regression (ADR 0054): dedicated test for the defect itself — the
    24h-cadence ``maintenance_interval_s`` loop and the 120s-cadence ``pre_ttl_scan_interval_s``
    loop must call TWO DIFFERENT verbs on ``LifecycleManagerPort``. Before the fix, both loops
    called ``sweep_user`` (the full promotion+demotion+retention sweep) — this test fails on that
    prior behaviour because ``mlm.calls`` would be non-empty from the pre-TTL loop alone, with
    ``maintenance_interval_s`` set far outside this test's window."""
    settings = LifecycleSettings(
        maintenance_interval_s=3600, pre_ttl_scan_interval_s=1, batch_size=1000
    )
    mlm = _RecordingLifecycleManager()
    loop = MaintenanceLoop(bus=bus, lifecycle_manager=mlm, settings=settings)

    run_task = asyncio.create_task(loop.run())
    await asyncio.sleep(0.05)
    ns = _ns(user="erin")
    await bus.publish(MemoryCaptured(namespace=ns, ids=["m1"], tier=Tier.STM))

    await asyncio.sleep(1.3)  # >= 1 pre-TTL tick, 0 maintenance ticks
    await loop.stop()
    await run_task

    assert mlm.rescue_calls, "the pre-TTL loop must fire rescue_pre_ttl_user on its own cadence"
    assert mlm.calls == [], "the pre-TTL loop must never fire the full sweep_user"


async def test_run_unsubscribes_on_stop_so_further_events_are_not_observed(bus: InprocBus) -> None:
    settings = LifecycleSettings(maintenance_interval_s=3600, pre_ttl_scan_interval_s=3600)
    mlm = _RecordingLifecycleManager()
    loop = MaintenanceLoop(bus=bus, lifecycle_manager=mlm, settings=settings)

    run_task = asyncio.create_task(loop.run())
    await asyncio.sleep(0.05)
    await loop.stop()
    await run_task

    ns = _ns(user="dave")
    await bus.publish(MemoryCaptured(namespace=ns, ids=["m1"], tier=Tier.STM))
    assert (
        loop.active_user_count == 0
    ), "a stopped loop must be unsubscribed, not silently listening"


# --------------------------------------------------------------------------- degrade honesty
async def test_unwired_lifecycle_manager_degrades_honestly_never_silently_no_ops() -> None:
    mgr = _UnwiredLifecycleManager()
    events: list[object] = []

    async def _capture(*args: object, **kwargs: object) -> None:
        events.append(kwargs)

    import structlog

    with structlog.testing.capture_logs() as logs:
        handle = await mgr.sweep_user(UserPrefix(_ns(user="eve")))

    assert handle.job_id.startswith("unwired-")
    reasons = [entry for entry in logs if entry.get("reason") == DegradeReason.HOST_WIRING_ABSENT]
    assert reasons, f"expected a HOST_WIRING_ABSENT DegradedModeEntered log, got: {logs}"
    assert reasons[0]["component"] == "lifecycle"


async def test_default_lifecycle_manager_is_the_unwired_degrade(bus: InprocBus) -> None:
    """Constructing ``MaintenanceLoop`` with no ``lifecycle_manager=`` must default to the honest
    degrade, never raise and never silently do nothing without a trace."""
    loop = MaintenanceLoop(bus=bus)
    assert isinstance(loop._mlm, _UnwiredLifecycleManager)
