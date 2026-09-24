"""``OutboxRetentionLoop`` — the policy the outbox never had (FAULT-HUNT-0924.md F4a).

``SqliteOutbox`` shipped ``put``/``ack``/``dead_letter``/``redrive_dead`` and NO ``DELETE``
statement anywhere: every successfully-delivered activity's raw ``activity_json`` sat on disk
forever. Measured on a real daemon: 374 ``acked`` rows, 307 KB, spanning 34 days, zero rows ever
removed. ``consent/residue.py``'s otherwise-exhaustive vocabulary did not even name the outbox as
something a revoke could not reach — it simply was not in the picture.

This module is the retention HALF of the fix (the targeted, caller-driven half is
``SqliteOutbox.delete_by_content_hash``, called from wherever a single memory's delete eventually
reaches the outbox — see that method's own docstring for the current gap: no ``mu-client`` verb
calls it yet, so today only THIS sweep actually reclaims space). A single independent supervised
task, deliberately NOT folded into ``mu_client.daemon.maintenance.MaintenanceLoop``: that class
owns the MEMORY lifecycle (promotion/demotion/consolidate, S1-03/S1-07) — the outbox is a
CAPTURE-side durability spine with its own, unrelated retention question, and giving it a second
loop keeps the two concerns from sharing one class's failure surface (a stuck outbox purge must
never be able to stall a lifecycle sweep, or the reverse).

Shape: the same periodic-loop-with-immediate-first-tick pattern every other supervised loop in
this codebase uses (``mu_client.daemon.maintenance.MaintenanceLoop._periodic_maintenance_loop``,
``mu_engine_server.lifecycle_runner.EngineLifecycleSweepRunner._periodic_loop``) — body first,
then a ``wait_for``-bounded stop check, so a fresh loop does its first sweep immediately rather
than waiting a full ``retention_sweep_interval_s`` before ever running.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import structlog
from mu_contracts.ports.time import Clock
from mu_engine.platform.clock import SystemClock

from mu_client.config import OutboxSettings
from mu_client.outbox.sqlite_outbox import SqliteOutbox

__all__ = ["OutboxRetentionLoop"]

_log = structlog.get_logger("mu.client.daemon.outbox_retention")


class OutboxRetentionLoop:
    """Periodically purges ``acked`` outbox rows older than ``settings.acked_retention_days``.

    Content-free observability only: the sweep logs a ROW COUNT and a cutoff timestamp, never any
    ``activity_json`` (DEV-STANDARDS rule 3 content-free discipline — this loop exists BECAUSE
    that column is the exact raw memory text the residue vocabulary now names)."""

    def __init__(
        self,
        outbox: SqliteOutbox,
        *,
        settings: OutboxSettings,
        clock: Clock | None = None,
    ) -> None:
        self._outbox = outbox
        self._settings = settings
        self._clock = clock or SystemClock()
        self._stop = asyncio.Event()
        # Observability counter — content-free, read by tests/health checks, never gated logic.
        self.sweep_count = 0
        self.rows_purged_total = 0

    async def run(self) -> None:
        """Started as one additional supervised task (``daemon/app.py::_supervise``, one more
        ``tg.create_task(outbox_retention.run())`` line)."""
        while not self._stop.is_set():
            await self.sweep_once()
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self._settings.retention_sweep_interval_s
                )
            except TimeoutError:
                continue

    async def stop(self) -> None:
        """Signals the loop to exit its current wait and return (mirrors every sibling loop's
        ``stop()``: no forced cancellation needed for the common case)."""
        self._stop.set()

    async def sweep_once(self) -> int:
        """Programmatic single-sweep hook (a test, or an operator CLI verb, calls this directly
        without waiting on the timer — the same "no need for `run()` to be active" seam
        ``EngineLifecycleSweepRunner.tick_once`` establishes one plane over). Returns the number
        of rows purged (0 is a valid, common answer)."""
        cutoff = self._clock.now() - timedelta(days=self._settings.acked_retention_days)
        purged = await self._outbox.purge_acked_before(cutoff)
        self.sweep_count += 1
        self.rows_purged_total += purged
        if purged:
            _log.info(
                "outbox_retention.purged",
                rows=purged,
                cutoff=cutoff.isoformat(),
                retention_days=self._settings.acked_retention_days,
            )
        return purged
