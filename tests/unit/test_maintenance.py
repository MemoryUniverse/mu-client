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
    ``sweep_user`` call (user + monotonic order), optionally gated by an ``asyncio.Event`` so a
    test can hold a "sweep in flight" window open to exercise the coalescing floor."""

    def __init__(self, *, gate: asyncio.Event | None = None) -> None:
        self.calls: list[UserPrefix] = []
        self._gate = gate

    async def sweep_user(self, user_prefix: UserPrefix) -> JobHandle:
        if self._gate is not None:
            await self._gate.wait()
        self.calls.append(user_prefix)
        return JobHandle(job_id=f"job-{len(self.calls)}", submitted_at=datetime.now(UTC))


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

    assert UserPrefix(ns) in mlm.calls, "the pre-TTL periodic loop must sweep the active user"


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
