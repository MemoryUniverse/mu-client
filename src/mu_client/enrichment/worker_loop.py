"""``EnrichmentWorkerLoop`` — the CLIENT scheduling glue around ``EnrichmentWorker.run_once``
(ADR-0055; AD-241).

Deliberately small: this module owns WHEN ``run_once`` is called (a periodic tick, supervised so
one bad tick never kills the loop), never HOW enrichment behaves — that is entirely
``mu_engine.pipelines.enrichment_worker.EnrichmentWorker``'s business, mirroring the split
``MaintenanceLoop``'s own module docstring draws between itself and ``MemoryLifecycleManager``.

Wiring this into the daemon's supervised-task set (``mu_client.daemon.app``, alongside
``MaintenanceLoop``/the outbox drainer) is a tracked one-line follow-up
(``docs/tracking/ARCHITECTURE-DELTAS.md`` AD-241) — this class is a complete, independently
testable unit on its own (``start()``/``stop()``, real crash/cancellation safety) and does not
need daemon-internals surgery to be correct or tested.
"""

from __future__ import annotations

import asyncio

import structlog
from mu_engine.pipelines.enrichment_worker import EnrichmentBatchReport, EnrichmentWorker

__all__ = ["EnrichmentWorkerLoop"]

_log = structlog.get_logger("mu_client.enrichment.worker_loop")


class EnrichmentWorkerLoop:
    """Ticks ``worker.run_once()`` every ``interval_s`` until :meth:`stop`. A tick that raises is
    logged (content-free — exception class name only) and the loop backs off to the next tick
    rather than dying; ``asyncio.CancelledError`` always propagates (DEV-STANDARDS rule 1 —
    cancellation is never swallowed)."""

    def __init__(self, worker: EnrichmentWorker, *, interval_s: float = 5.0) -> None:
        self._worker = worker
        self._interval_s = interval_s
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        if self.is_running:
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def tick(self) -> EnrichmentBatchReport:
        """One drain, exposed directly for tests and for a caller that wants explicit control
        (e.g. a daemonless CLI flush) instead of the background cadence."""
        return await self._worker.run_once()

    async def _run(self) -> None:
        try:
            while not self._stop.is_set():
                try:
                    report = await self.tick()
                    if report.claimed:
                        _log.info(
                            "enrichment_tick",
                            claimed=report.claimed,
                            enriched=report.enriched,
                            skipped_deleted=report.skipped_deleted,
                            retried=report.retried,
                            dead_lettered=report.dead_lettered,
                        )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    _log.warning("enrichment_tick_failed", error=type(exc).__name__)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=self._interval_s)
                except TimeoutError:
                    pass
        except asyncio.CancelledError:
            raise
