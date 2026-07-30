"""``LocalDaemon`` — the composition root + lifecycle (daemon-app-skeleton-spec.md §3), scoped to
THIS build stage: **capture + outbox + recall/inject + IPC front door + lifecycle maintenance**,
one ``TaskGroup``, ordered shutdown. Device-sync (§8), cross-plane Centrifugo listeners (§8/§9),
and bound-agent supervision are SHARED-plane / multi-device features the ALL-CLIENT-CRITERIA brief
scoped OUT of this stage (they need a running mu-server + Centrifugo this repo never talks to —
``client-has-no-server``); the daemon's shutdown order and TaskGroup-per-source discipline below
are still the REAL, load-bearing subset for a single-device LOCAL daemon.

**MaintenanceLoop wiring (spec §14 slice 1 / S1-07) — CLOSED at Stage-1 integrate.** ``start()``
below wires the REAL end-to-end lifecycle path, closing both seams S1-07's own build left open:

1. **Shared bus.** ``self._host.bus`` (``LocalMemoryHost.bus`` -> ``LocalMemory.bus`` ->
   ``LocalContainer.bus``, integrate-phase accessors added to all three) is the IDENTICAL
   ``InprocBus`` instance ``IngestService``/``DistillPipeline`` publish onto — ``MaintenanceLoop``
   now subscribes to the SAME bus captured memories actually publish to, not a second, dark one.
   (``OutboxWorker`` publishing a genuine ``MemoryCaptured`` onto this bus, vs. today's
   ``log_memory_captured`` structlog-only emission, remains a tracked follow-up outside this
   task's + this integrate pass's owned files — ``workers/pool.py`` is untouched.)
2. **Real ``MemoryLifecycleManager`` (S1-03) + real durable runner/lease (S1-06).** ``self._host.
   build_lifecycle_manager(lease=..., runner=...)`` constructs the genuine orchestrator over THIS
   host's own stores/distill, wired with a real cross-process ``SqliteWalLeaseAdapter``
   (``LifecycleLeasePort``) and ``SqliteWalRunner`` (``LifecycleWorkflowRunnerPort``) sharing ONE
   SQLite-WAL file next to the outbox DB (BQ1's "reuses a durable substrate" convention, a sibling
   file rather than the SAME file as the outbox — the outbox and the lifecycle job log are
   different durable logs with different schemas, spec §5/§4b own theirs). ``resume_pending()``
   replays any crash-interrupted job on boot (SqliteWalRunner's own crash-recovery contract).
   ``MaintenanceLoop`` receives this real MLM instead of falling back to the honest
   ``_UnwiredLifecycleManager`` degrade.
3. **Job execution.** ``SqliteWalRunner.submit()`` only durably LOGS a job (spec §5: "durable
   enqueue; returns fast") — something must drain ``claim_next()`` and call back into
   ``MemoryLifecycleManager.run_job`` for the job to actually execute; no Stage-0/Stage-1 task
   owned this worker loop (``sqlite_wal.py``'s own docstring: "a later Stage-1 worker loop (a task
   out of this task's owned files) drains through this"). :meth:`LocalDaemon._drain_lifecycle_jobs`
   below is that loop — a 4th supervised task, added at THIS integrate phase since it is pure
   cross-service glue with no single natural owner among the seven parallel tasks.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from mu_client.capture.hook import replay_spool
from mu_client.capture.parsers import ClaudeCodeParserV1, ParserRegistry
from mu_client.config import ClientSettings, get_client_settings
from mu_client.daemon.ipc import IpcServer
from mu_client.daemon.maintenance import MaintenanceLoop
from mu_client.host import LocalMemoryHost
from mu_client.inject.recall_bridge import RecallInjectBridge
from mu_client.outbox.sqlite_outbox import SqliteOutbox
from mu_client.runners.sqlite_wal import SqliteWalLeaseAdapter, SqliteWalRunner
from mu_client.workers.ingest_client import InProcessLocalIngest
from mu_client.workers.pool import OutboxWorker, WorkerPool

if TYPE_CHECKING:
    from mu_engine.lifecycle.manager import MemoryLifecycleManager

__all__ = ["LocalDaemon"]

# Drain-loop poll cadence when the durable job log is empty (technical drain cadence, not a
# lifecycle policy knob — mirrors SqliteWalRunner.await_result's own poll_interval_s=0.05 default,
# a function-default constant rather than a central-config field, for the identical reason: this
# is a mechanical "how often to check the queue" interval, not a threshold DEV-STANDARDS rule 3
# means to keep tunable business config).
_LIFECYCLE_JOB_POLL_INTERVAL_S = 0.2


class LocalDaemon:
    """The ONE wiring point (APP-scoped singleton, daemon-app-skeleton-spec.md §3.1). Coded
    against :class:`~mu_client.config.ClientSettings` — never a bare ``os.environ`` read anywhere
    downstream of construction."""

    def __init__(self, settings: ClientSettings | None = None) -> None:
        self._settings = settings or get_client_settings()
        self._host: LocalMemoryHost | None = None
        self._outbox: SqliteOutbox | None = None
        self._pool: WorkerPool | None = None
        self._ipc: IpcServer | None = None
        self._maintenance: MaintenanceLoop | None = None
        self._lifecycle_runner: SqliteWalRunner | None = None
        self._lifecycle_lease: SqliteWalLeaseAdapter | None = None
        self._lifecycle_manager: MemoryLifecycleManager | None = None
        self._drain_stop: asyncio.Event = asyncio.Event()
        self._tg: asyncio.TaskGroup | None = None
        self._run_task: asyncio.Task[None] | None = None

    @property
    def outbox(self) -> SqliteOutbox:
        if self._outbox is None:
            raise RuntimeError("LocalDaemon.start() was not called")
        return self._outbox

    @property
    def host(self) -> LocalMemoryHost:
        if self._host is None:
            raise RuntimeError("LocalDaemon.start() was not called")
        return self._host

    @contextlib.asynccontextmanager
    async def lifespan(self) -> AsyncIterator[LocalDaemon]:
        await self.start()
        try:
            yield self
        finally:
            await self.shutdown()

    async def start(self) -> None:
        # 1) ON-DEVICE ENGINE HOST — real mu-local, in-process (daemon-app-skeleton-spec.md §4).
        self._host = LocalMemoryHost(self._settings)
        await self._host.start()

        # 2) DURABILITY SPINE — the SQLite-WAL outbox (capture-spec.md §8.3). replay of any
        #    INFLIGHT->PENDING crash-recovery runs inside SqliteOutbox.open().
        self._outbox = SqliteOutbox(self._settings.outbox.outbox_path)
        await self._outbox.open()
        # Recover any hook-client spool (idempotent, UNIQUE(activity_id)).
        await replay_spool(self._settings, self._outbox)

        # 3) CAPTURE cluster — registry-selected parsers (Claude Code is the only host this
        #    stage wires; capture-spec.md §5.4 names Codex/Desktop as later-stage additions).
        registry = ParserRegistry()
        registry.register(
            ClaudeCodeParserV1(tool_outcome_max_chars=self._settings.capture.tool_outcome_max_chars)
        )

        # 4) WORKER POOL — drain -> LocalMemory.add -> ack (capture-spec.md §8.4).
        ingest = InProcessLocalIngest(self._host, user=self._settings.default_user)
        worker = OutboxWorker(
            self._outbox,
            ingest,
            settings=self._settings.outbox,
            org=self._settings.default_namespace,
            workspace=self._settings.default_workspace,
            user=self._settings.default_user,
        )
        self._pool = WorkerPool(worker, poll_interval_s=self._settings.outbox.poll_interval_s)

        # 5) INJECTION bridge (capture-spec.md §7.2), NOW wired onto the REAL bus (Stage-3
        #    integrate — was a bare PULL-only bridge before this pass, so a real MLM tier
        #    transition never pushed a refresh). ``self._host.bus`` is the SAME ``InprocBus``
        #    ``IngestService``/``DistillPipeline`` publish onto (module docstring point 1) — the
        #    bridge subscribes to the identical real event stream ``MaintenanceLoop`` does below,
        #    never a second, dark bus.
        bridge = RecallInjectBridge(self._host, settings=self._settings.inject, bus=self._host.bus)

        # 6) LIFECYCLE MANAGER — the real ``MemoryLifecycleManager`` (S1-03) + real durable
        #    runner/lease (S1-06), built BEFORE the IPC front door below (Stage-3 integrate
        #    reorder) so ``/state``/``/ready-context`` (S3-03) can thread it through
        #    ``IpcServer(lifecycle_manager=...)`` at construction rather than needing a mutable
        #    setter. The durable job log + cross-process lease share ONE sqlite-WAL file (BQ1)
        #    next to the outbox DB — a sibling file, not the outbox's own (different schema/owner,
        #    §5/§4b). Named from the outbox file's OWN stem (integrate-phase fix, not a bare
        #    "lifecycle.sqlite3" literal): two daemons that happen to share a parent directory but
        #    point at DIFFERENT outbox files (e.g. test_maintenance_int.py's AC-1.2 baseline/loaded
        #    daemons, both under one pytest tmp_path) must never collide on the identical
        #    lifecycle-WAL file — that collision reproduced verbatim as a real
        #    ``sqlite3.OperationalError: database is locked`` (BEGIN IMMEDIATE contention between
        #    two independent connections to one file) before this fix. A single real daemon's own
        #    outbox path is already unique per device (daemon-app-skeleton-spec.md §4), so this
        #    changes nothing for the production one-daemon-per-directory case.
        outbox_path = self._settings.outbox.outbox_path.expanduser()
        lifecycle_wal_path = outbox_path.parent / f"{outbox_path.stem}-lifecycle.sqlite3"
        self._lifecycle_runner = SqliteWalRunner(lifecycle_wal_path)
        await self._lifecycle_runner.open()
        resumed = await self._lifecycle_runner.resume_pending()  # crash-recovery replay on boot
        self._lifecycle_lease = SqliteWalLeaseAdapter(
            lifecycle_wal_path, device_id=self._settings.device_id
        )
        await self._lifecycle_lease.open()

        # ``warm_cache=bridge`` (Stage-3 integrate): the SAME ``RecallInjectBridge`` instance is
        # the ``WarmRecallCacheServicePort`` this manager's ``ready_context`` reads synchronously —
        # never a second, competing warm-cache service (CANONICAL §7.22 ONE owner).
        self._lifecycle_manager = self._host.build_lifecycle_manager(
            lease=self._lifecycle_lease, runner=self._lifecycle_runner, warm_cache=bridge
        )
        if resumed:
            # Any job resume_pending() reset PENDING -> re-drained by _drain_lifecycle_jobs below;
            # nothing further to do here (fail-loud logging already happened inside SqliteWalRunner
            # if a genuinely crashed row was found — this is just an observability breadcrumb).
            pass

        # 7) FRONT DOOR. Bound HERE (synchronously, awaited) — a caller of start()/lifespan() must
        #    be able to connect to the real socket the instant start() returns, never race the
        #    background accept loop's own startup. ``lifecycle_manager=self._lifecycle_manager``
        #    (S3-03, Stage-3 integrate) wires the real ``/state``/``/ready-context`` routes instead
        #    of the named-503 unwired degrade.
        self._ipc = IpcServer(
            self._settings.ipc,
            registry=registry,
            outbox=self._outbox,
            bridge=bridge,
            lifecycle_manager=self._lifecycle_manager,
        )
        await self._ipc.bind()

        # 8) MAINTENANCE LOOP — the 3rd supervised task (memory-lifecycle-manager-spec.md §14
        #    slice 1 / S1-07): event-driven fast-fire + the two decoupled periodic cadences (§7/
        #    §7b), over the REAL bus + the REAL MemoryLifecycleManager built in step 6 above.
        self._maintenance = MaintenanceLoop(
            bus=self._host.bus, lifecycle_manager=self._lifecycle_manager
        )

        await self._supervise()

    async def _drain_lifecycle_jobs(self) -> None:
        """The worker loop ``SqliteWalRunner.submit()``'s own docstring calls out as owned by
        nobody yet (module docstring, point 3): claims the oldest ``PENDING`` durable job, runs it
        through the real ``MemoryLifecycleManager.run_job``, then marks it terminal. Polls at
        ``_LIFECYCLE_JOB_POLL_INTERVAL_S`` when the queue is empty; exits once :meth:`shutdown`
        signals ``_drain_stop`` AND the queue is drained (mirrors ``WorkerPool.drain_and_stop``'s
        own "finish in-flight work before stopping" discipline)."""
        if self._lifecycle_runner is None or self._lifecycle_manager is None:
            raise RuntimeError("_drain_lifecycle_jobs() called before start() finished wiring")
        runner = self._lifecycle_runner
        mlm = self._lifecycle_manager
        while True:
            job = await runner.claim_next()
            if job is None:
                if self._drain_stop.is_set():
                    return
                try:
                    await asyncio.wait_for(
                        self._drain_stop.wait(), timeout=_LIFECYCLE_JOB_POLL_INTERVAL_S
                    )
                except TimeoutError:
                    continue
                else:
                    return
            result = await mlm.run_job(job)
            await runner.complete(job.job_id, error=result.error)

    async def _supervise(self) -> None:
        """One ``TaskGroup``, fixed task set (daemon-app-skeleton-spec.md §3.2): the pool, the IPC
        server, the maintenance loop (memory-lifecycle-manager-spec.md §14 slice 1 / S1-07), and
        the lifecycle job-drain loop (integrate-phase, closing ``SqliteWalRunner``'s own "a later
        worker loop drains through this" seam) are the four top-level supervised tasks THIS stage
        owns. Started as a background task (not awaited here) so ``start()`` returns once the
        daemon is live — ``lifespan()``'s caller awaits its own stop signal, then drives
        ``shutdown()``."""
        if self._pool is None or self._ipc is None or self._maintenance is None:
            raise RuntimeError("_supervise() called before start() finished wiring")
        pool, ipc, maintenance = self._pool, self._ipc, self._maintenance

        async def _run() -> None:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(pool.run())
                tg.create_task(ipc.serve())  # bind() already ran; this just accepts + serve_forever
                tg.create_task(maintenance.run())
                tg.create_task(self._drain_lifecycle_jobs())
                self._tg = tg

        # The socket is already bound (ipc.bind() ran above, awaited); this task just runs the
        # worker-pool poll loop + the accept loop. Yield once so both tasks get a chance to reach
        # their first await point before start() returns.
        self._run_task = asyncio.create_task(_run())
        await asyncio.sleep(0)

    async def shutdown(self) -> None:
        """Ordered shutdown (daemon-app-skeleton-spec.md §3.3): stop new inbound (1), drain
        outbound (2 — this stage has no per-source checkpoint to persist, capture is stateless
        request/response over IPC, not a tailer), release the engine LAST. A crash before step 2
        completes is safe — every record was fsync'd at append (capture-spec.md §8.3)."""
        if self._ipc is not None:
            await self._ipc.stop_accepting()  # 1. stop new /capture handoffs
        if self._maintenance is not None:
            await self._maintenance.stop()  # 1b. stop firing new lifecycle sweeps
        self._drain_stop.set()  # 1c. let the lifecycle job-drain loop finish its current poll+exit
        if self._pool is not None:
            await self._pool.drain_and_stop()  # 2. drain in-flight outbox -> remember -> ack
        if self._run_task is not None:
            self._run_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._run_task
        if self._outbox is not None:
            await self._outbox.aclose()
        if self._lifecycle_lease is not None:
            await self._lifecycle_lease.aclose()
        if self._lifecycle_runner is not None:
            await self._lifecycle_runner.aclose()
        if self._host is not None:
            await self._host.aclose()  # 3. release engine adapters LAST
