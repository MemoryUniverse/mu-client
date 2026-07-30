"""``MaintenanceLoop`` — the daemon's 3rd supervised task (daemon-app-skeleton-spec.md pattern,
``memory-lifecycle-manager-spec.md`` §7/§7b/§13.1/§14 slice 1).

Three independent trigger paths run concurrently, mirroring the MMA dual-trigger
(``mma/mma/memory/controller.py:380,405``) plus the spec's §7b decoupling fix:

1. **Event-driven fast-fire** (§7) — subscribes the LOCAL ``EventBusPort`` for
   ``MemoryCaptured``/``MemoryPromoted``, keeps a per-user batch counter, and fires
   ``MLM.sweep_user`` for THAT user only once ``lifecycle.batch_size`` is reached.
2. **Periodic safety-net sweep** (§7) — the slow backstop at ``lifecycle.maintenance_interval_s``
   (24h prod default) that catches decay/retention/GC no event ever triggers, bounded per tick by
   ``max_users_per_sweep`` with a cooperative yield between users (§7 S3 backpressure / AC-1.2).
3. **Pre-TTL rescue scan** (§7b MAJOR-4 fix) — its OWN independent, short cadence
   (``lifecycle.pre_ttl_scan_interval_s``), deliberately DECOUPLED from (2): the 24h-prod-default
   periodic sweep cannot reliably land inside a demoted item's ``pre_ttl_window_s``-wide rescue
   window before Redis TTL-deletes it (spec §7b, the arithmetic-pass correction — the composed
   invariant is ``pre_ttl_scan_interval_s <= pre_ttl_window_s / 2``, not a comparison against
   ``stm_ttl_s - pre_ttl_window_s``). AC-1.3a (§14.1).

``MaintenanceLoop`` itself carries NO promotion/demotion/consolidate/cursor logic — that is
entirely ``MemoryLifecycleManager.sweep_user``'s business (S1-03, wrapping ``PromotionService``/
``DemotionService``/``DistillPipeline`` — S1-01/S1-02/S1-05, including the durable consumed-offset
consolidate cursor, spec §13.1, that fixes BQ2). This module's job is exactly WHEN to call
``sweep_user`` (event threshold / two independent cadences), never HOW the sweep behaves. It
depends on :class:`LifecycleManagerPort` — a narrow, ``runtime_checkable`` PEP 544 Protocol this
module defines and owns (the SAME decoupling discipline every port in ``mu_contracts.ports``
already uses, e.g. ``EventBusPort``/``LifecycleLeasePort``/``LifecycleWorkflowRunnerPort``) so this
task's build does not import ``mu_engine.lifecycle.manager`` — a sibling Stage-1 task (S1-03)
landing IN PARALLEL with this one. The real ``MemoryLifecycleManager`` satisfies this Protocol
structurally with no import-time coupling; see :class:`_UnwiredLifecycleManager` below for the
honest degrade path used until the composition root (an integrate-phase wiring task — see
``daemon/app.py``'s module docstring) threads a real one in.

**One-sweep-per-user coalescing** (spec §7: "A running sweep coalesces new triggers rather than
stacking") has TWO layers: this loop's own in-process ``_inflight`` set is the cheap, ahead-of-lease
floor that stops MaintenanceLoop from ever launching two concurrent ``sweep_user`` calls for the
same user; ``MemoryLifecycleManager.sweep_user`` itself additionally acquires the cross-process,
user-prefix-grained ``LifecycleLeasePort`` lease (§4b) — the two are complementary, not redundant
(this loop's floor covers the in-process race a single daemon can hit between its own three
trigger paths; the lease covers a second daemon process / daemonless CLI invocation for the same
user, AC-1.1).
"""

from __future__ import annotations

import asyncio
import time
import uuid
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable

import structlog
from mu_contracts.domain.events import DegradeReason, DomainEvent, MemoryCaptured, MemoryPromoted
from mu_contracts.domain.model.lifecycle import JobHandle, UserPrefix
from mu_contracts.ports.bus import EventBusPort, Subscription
from mu_engine.lifecycle.settings import LifecycleSettings
from pydantic_settings import BaseSettings, SettingsConfigDict

from mu_client.observability.events import log_degraded

__all__ = [
    "LifecycleManagerPort",
    "MaintenanceEnvSettings",
    "MaintenanceLoop",
]

_log = structlog.get_logger("mu.client.daemon.maintenance")


@runtime_checkable
class LifecycleManagerPort(Protocol):
    """The ONLY surface :class:`MaintenanceLoop` needs from the Memory Lifecycle Manager (spec
    §5/§14 slice 1 — ``MemoryLifecycleManager.sweep_user``, S1-03, landing in parallel with this
    task). Deliberately narrow: ``sweep_user`` is a durable-write verb (spec §5 read/write split)
    — it internally acquires the user-grained ``LifecycleLeasePort`` lease (§4b), coalesces
    overlapping triggers cross-process, and submits through ``LifecycleWorkflowRunnerPort``. It
    returns a ``JobHandle`` FAST, never blocking on the sweep's own completion. Everything the
    sweep actually DOES once called — recompute salience, run the three promotion paths + the
    pre-TTL rescue scan (§7b), the MTM->STM demotion forgetting curve, the cursor-safe MTM->LTM
    consolidate pass (§13.1, the BQ2 fix) — lives entirely on the far side of this one call.
    """

    async def sweep_user(self, user_prefix: UserPrefix) -> JobHandle:
        """Durable enqueue of one lifecycle sweep for ``user_prefix``; returns fast (spec §5)."""
        ...


class MaintenanceEnvSettings(LifecycleSettings, BaseSettings):
    """Env-activated :class:`LifecycleSettings` (``MU_LIFECYCLE__*``) — mirrors MMA's demo
    cadence-override pattern (``mma/mma/memory/controller.py:405,412``,
    ``MU_LIFECYCLE__MAINTENANCE_INTERVAL_S=60``).

    ``LifecycleSettings`` (S0-07, ``mu_engine.lifecycle.settings``) is deliberately a plain frozen
    ``BaseModel`` — its own module docstring states the ``Settings.lifecycle``/
    ``ClientSettings.lifecycle`` composition-root wiring is "explicitly out of scope for this
    slice." No task in this build plan owns adding a ``lifecycle`` field to
    ``mu_client.config.ClientSettings`` yet. Until one does (an integrate-phase wiring TODO — see
    ``daemon/app.py``), this small ``BaseSettings`` mixin is the ONE place ``MU_LIFECYCLE__*`` is
    read: it inherits ``LifecycleSettings``'s exact field/default set (no duplicated literals to
    drift, DEV-STANDARDS rule 3) and layers on pydantic-settings' env-file/env-var resolution —
    never a bare ``os.environ`` read. A bare ``MaintenanceEnvSettings()`` reproduces
    ``LifecycleSettings()``'s defaults byte-for-byte when no ``MU_LIFECYCLE__*`` var is set.
    """

    model_config = SettingsConfigDict(
        env_prefix="MU_LIFECYCLE__",
        env_nested_delimiter="__",
        env_file=(".env", ".env.test"),
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )


class _UnwiredLifecycleManager:
    """Degrade-honest placeholder :class:`LifecycleManagerPort` (spec §7.13
    ``DegradeReason.HOST_WIRING_ABSENT``) used ONLY until the composition root threads a real
    ``MemoryLifecycleManager`` into :class:`MaintenanceLoop` — S1-03's orchestrator, built over
    S1-05's MLM composition factory, is a sibling Stage-1 task landing in parallel with this one
    and is not yet reachable through ``mu_client.host.LocalMemoryHost``'s public surface as of this
    task's build (see ``daemon/app.py``'s module docstring for the exact integrate-phase TODO).

    Never a silent no-op: every call emits a content-free ``DegradedModeEntered(component=
    "lifecycle", reason=HOST_WIRING_ABSENT)`` (the named-reason-over-bare-fallback discipline this
    codebase uses everywhere else, e.g. ``mu_local.composition``'s LLM=None heuristic-mode degrade)
    and returns a genuine ``JobHandle`` for a job that will never actually run — capture/recall/IPC
    keep working; no lifecycle sweep executes until the real MLM is wired in.
    """

    async def sweep_user(self, user_prefix: UserPrefix) -> JobHandle:
        log_degraded(
            component="lifecycle",
            mode="sweep_user",
            reason=DegradeReason.HOST_WIRING_ABSENT,
            detail=f"user_prefix={user_prefix}",
        )
        return JobHandle(job_id=f"unwired-{uuid.uuid4().hex}", submitted_at=datetime.now(UTC))


class MaintenanceLoop:
    """The 3rd supervised daemon task (``daemon/app.py::_supervise``) — see module docstring for
    the three concurrent trigger paths."""

    def __init__(
        self,
        *,
        bus: EventBusPort,
        lifecycle_manager: LifecycleManagerPort | None = None,
        settings: LifecycleSettings | None = None,
    ) -> None:
        self._bus = bus
        self._mlm: LifecycleManagerPort = lifecycle_manager or _UnwiredLifecycleManager()
        self._settings = settings or MaintenanceEnvSettings()
        self._stop = asyncio.Event()

        # Event-driven fast-fire state (§7).
        self._batch_counts: dict[UserPrefix, int] = {}
        # Hybrid discovery's "active-user registry" (§7): populated from bus events, read by both
        # periodic loops below. touched_at is monotonic wall-time, observability only (no decision
        # logic reads it) — NOT a substitute for the injected Clock the sweep itself uses (§19).
        self._active_users: dict[UserPrefix, float] = {}
        # One-sweep-per-user in-process floor (module docstring) — complements, never replaces,
        # sweep_user's own cross-process LifecycleLeasePort acquisition.
        self._inflight: set[UserPrefix] = set()

        self._sub_captured: Subscription | None = None
        self._sub_promoted: Subscription | None = None

        # Observability counters — content-free, read by tests/health checks, never gated logic.
        self.fast_fire_count = 0
        self.maintenance_tick_count = 0
        self.pre_ttl_tick_count = 0
        self.coalesced_count = 0

    @property
    def settings(self) -> LifecycleSettings:
        return self._settings

    @property
    def active_user_count(self) -> int:
        return len(self._active_users)

    # ------------------------------------------------------------------------- bus subscription
    def _subscribe(self) -> None:
        """Idempotent: a second ``run()`` on an already-subscribed loop does not double-subscribe
        (mirrors ``LocalMemoryHost.start()``'s idempotency discipline, ``host.py:70-74``)."""
        if self._sub_captured is not None:
            return
        self._sub_captured = self._bus.subscribe(MemoryCaptured, self._on_bus_event)
        self._sub_promoted = self._bus.subscribe(MemoryPromoted, self._on_bus_event)

    async def _unsubscribe(self) -> None:
        for sub in (self._sub_captured, self._sub_promoted):
            if sub is not None:
                await sub.unsubscribe()
        self._sub_captured = None
        self._sub_promoted = None

    async def _on_bus_event(self, event: DomainEvent) -> None:
        """Handler for both ``MemoryCaptured`` and ``MemoryPromoted`` (both carry ``namespace``,
        the structural field this handler actually reads — ``isinstance`` narrowing is not needed
        since neither branch reads a field the other type lacks)."""
        assert isinstance(event, MemoryCaptured | MemoryPromoted)  # noqa: S101 — structural guard,
        # not a test assertion: InprocBus.subscribe only ever dispatches the two types this loop
        # subscribed to (bus_inproc.py:50-60's isinstance-keyed dispatch), so this documents the
        # invariant for mypy/readers rather than gating unreachable behaviour.
        user_prefix = UserPrefix(event.namespace)
        self._active_users[user_prefix] = time.monotonic()
        count = self._batch_counts.get(user_prefix, 0) + 1
        if count >= self._settings.batch_size:
            self._batch_counts[user_prefix] = 0
            await self._fire_sweep(user_prefix)
        else:
            self._batch_counts[user_prefix] = count

    async def _fire_sweep(self, user_prefix: UserPrefix) -> None:
        if user_prefix in self._inflight:
            self.coalesced_count += 1
            return
        self._inflight.add(user_prefix)
        try:
            await self._mlm.sweep_user(user_prefix)
            self.fast_fire_count += 1
        finally:
            self._inflight.discard(user_prefix)

    # ---------------------------------------------------------------------------------- run/stop
    async def run(self) -> None:
        """Started as the 3rd ``TaskGroup`` member (``daemon/app.py::_supervise``, one additional
        ``tg.create_task(maintenance.run())`` line). Subscribes to the bus, then runs the two
        independent periodic cadences (§7b) concurrently; a cancellation (ordered shutdown,
        ``stop()``) unsubscribes cleanly before returning."""
        self._subscribe()
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._periodic_maintenance_loop())
                tg.create_task(self._pre_ttl_loop())
        finally:
            await self._unsubscribe()

    async def stop(self) -> None:
        """Signals both periodic loops to exit their current wait and return; ``run()``'s
        ``TaskGroup`` then completes on its own (no forced cancellation needed for the common
        case — mirrors ``WorkerPool``'s ``_stop`` event, ``pool.py:125,141``)."""
        self._stop.set()

    async def _periodic_maintenance_loop(self) -> None:
        """The slow safety-net sweep (§7): a cooperative-yield, bounded pass over the active-user
        registry every ``maintenance_interval_s`` — the same poll-loop shape as
        ``WorkerPool.run()`` (``pool.py:127-136``: body first, then a ``wait_for``-bounded stop
        check), so a fresh ``MaintenanceLoop`` fires its first backstop pass immediately rather
        than waiting a full ``maintenance_interval_s`` (24h prod default) before ever running."""
        while not self._stop.is_set():
            await self._sweep_active_users(cap=self._settings.max_users_per_sweep)
            self.maintenance_tick_count += 1
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self._settings.maintenance_interval_s
                )
            except TimeoutError:
                continue

    async def _pre_ttl_loop(self) -> None:
        """Independent, short cadence (§7b MAJOR-4 fix): ``pre_ttl_scan_interval_s`` runs
        completely decoupled from ``maintenance_interval_s`` so a demoted/low-volume item's
        ``pre_ttl_window_s``-wide rescue window actually contains a scan tick before Redis
        TTL-deletes it (AC-1.3a). The invariant this cadence must satisfy
        (``pre_ttl_scan_interval_s <= pre_ttl_window_s / 2``) is enforced at config-authoring time
        by ``LifecycleSettings``' own field defaults (S0-07, 120<=150); this loop only supplies
        the cadence — which items are inside their window and get rescued is entirely
        ``sweep_user``'s (PromotionService's) decision, not this loop's."""
        while not self._stop.is_set():
            await self._sweep_active_users(cap=self._settings.max_users_per_sweep)
            self.pre_ttl_tick_count += 1
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self._settings.pre_ttl_scan_interval_s
                )
            except TimeoutError:
                continue

    async def _sweep_active_users(self, *, cap: int) -> None:
        """Bounded per-tick pass (§7 S3 backpressure / AC-1.2): at most ``cap`` users, a
        cooperative ``asyncio.sleep(0)`` yield between each so a long tick never starves the
        event loop's other supervised tasks (``WorkerPool.run``'s capture-drain, ``IpcServer``'s
        accept loop).

        Discovery gap (integrate-phase note, spec §7's "hybrid discovery"): this loop's only user
        directory is the ACTIVE-user registry populated by bus events — the spec's
        ``full_scan_interval_s`` "slow full-scan backstop... so a user with no recent events still
        ages" needs a user-enumeration port no Stage-0/Stage-1 task in this plan yet exposes. Until
        one lands, a user who never triggers a single ``MemoryCaptured``/``MemoryPromoted`` event
        is invisible to both periodic loops here — tracked, not silently assumed complete.
        """
        for user_prefix in list(self._active_users)[:cap]:
            await self._fire_sweep(user_prefix)
            await asyncio.sleep(0)
