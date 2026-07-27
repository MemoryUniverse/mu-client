"""``OutboxWorker``/``WorkerPool`` — drain -> remember -> ack (capture-spec.md §8.4).

The double-reinforcement trap (capture-spec.md §8.4) is closed by construction: capture-side
``UNIQUE(activity_id)`` (outbox) means a given activity is drained+ingested at most once per
lifetime — ``ack`` only ever follows a successful (or intentionally-skipped) ``ingest``, never a
duplicate call into ``LocalMemory.add``.
"""

from __future__ import annotations

import asyncio

from mu_contracts.domain.errors import MemoryUniverseError
from mu_contracts.domain.events import DegradeReason
from mu_contracts.domain.model.memory import Namespace, Visibility
from pydantic import BaseModel, ConfigDict

from mu_client.config import OutboxSettings
from mu_client.observability.events import log_degraded, log_memory_captured
from mu_client.outbox.sqlite_outbox import SqliteOutbox
from mu_client.workers.ingest_client import ExtractionSkippedError, IngestClientPort

__all__ = ["OutboxWorker", "WorkerPool", "WorkerTick"]


class WorkerTick(BaseModel, frozen=True):
    model_config = ConfigDict(frozen=True, extra="forbid")

    drained: int = 0
    acked: int = 0
    skipped: int = 0
    retried: int = 0
    dead_lettered: int = 0


class OutboxWorker:
    """One drain/ingest/ack pass (capture-spec.md §8.4 ``run_once``)."""

    def __init__(
        self,
        outbox: SqliteOutbox,
        ingest: IngestClientPort,
        *,
        settings: OutboxSettings,
        org: str,
        workspace: str,
        user: str,
    ) -> None:
        self._outbox = outbox
        self._ingest = ingest
        self._settings = settings
        self._org = org
        self._workspace = workspace
        # The real η.user this worker's ingest actually stores under (data-representation
        # fidelity: the emitted MemoryCaptured event MUST report the same user slot as the store
        # write — never the activity's session_id, which is a different η component entirely).
        self._user = user

    async def run_once(self, *, batch_size: int | None = None) -> WorkerTick:
        batch = await self._outbox.drain(batch_size=batch_size or self._settings.batch_size)
        acked = skipped = retried = dead_lettered = 0
        for record in batch.records:
            try:
                await self._ingest.ingest(record.activity)
            except ExtractionSkippedError:
                # capture-spec.md §7.1 kind-gate: control kind / filtered slice — durable + ack'd,
                # never turned into a MemoryItem. Not an error.
                await self._outbox.ack([record.seq])
                skipped += 1
                continue
            except MemoryUniverseError as exc:
                if record.attempts + 1 >= self._settings.max_attempts:
                    await self._outbox.dead_letter(record.seq, error=str(exc))
                    # DegradeReason.SERVER_UNREACHABLE is a SYNC-CLASS device-sync reason — wrong
                    # here: this is a LOCAL-plane ingest dead-letter (no server in the local
                    # plane). FACT_DROPPED (the "PIPELINES" family) is the nearest closed-enum
                    # member: the activity permanently failed to become a memory after
                    # max_attempts retries and is durably dropped (dead-lettered), never a
                    # reachability/sync concept. See mu_contracts.domain.events.DegradeReason.
                    log_degraded(
                        component="ingest",
                        mode="dead_letter_after_max_attempts",
                        reason=DegradeReason.FACT_DROPPED,
                        detail=f"outbox_seq={record.seq}",
                    )
                    dead_lettered += 1
                else:
                    backoff = self._settings.base_backoff_s * (2**record.attempts)
                    await self._outbox.retry_later(record.seq, error=str(exc), backoff_s=backoff)
                    retried += 1
                continue
            await self._outbox.ack([record.seq])
            log_memory_captured(
                namespace=Namespace(
                    org=self._org,
                    workspace=self._workspace,
                    # The REAL ingest user (matches InProcessLocalIngest's own `user=` — the
                    # actual persisted partition), NOT record.activity.session_id (a different η
                    # slot entirely; data-review ISSUE — the emitted event previously misreported
                    # η.user as the session id).
                    user=self._user,
                    session=record.activity.session_id,
                    visibility=Visibility.PRIVATE,
                ),
                ids=[record.activity.activity_id],
            )
            acked += 1
        return WorkerTick(
            drained=len(batch.records),
            acked=acked,
            skipped=skipped,
            retried=retried,
            dead_lettered=dead_lettered,
        )


class WorkerPool:
    """Runs :class:`OutboxWorker` on a fixed poll cadence until stopped (daemon mode); the
    daemonless ``mu flush`` path instead calls ``run_once`` in a tight loop until the outbox is
    empty (see :mod:`mu_client.cli`) — same worker, two callers."""

    def __init__(self, worker: OutboxWorker, *, poll_interval_s: float) -> None:
        self._worker = worker
        self._poll_interval_s = poll_interval_s
        self._stop = asyncio.Event()

    async def run(self) -> None:
        while not self._stop.is_set():
            await self._worker.run_once()
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._poll_interval_s)
            except TimeoutError:
                # Expected: the poll cadence elapsed with no stop signal — loop again. A REAL
                # asyncio.CancelledError (task cancellation) is a different exception and still
                # propagates (DEV-STANDARDS rule 1).
                continue

    async def drain_and_stop(self) -> WorkerTick:
        """Ordered-shutdown step 3 (daemon-app-skeleton-spec.md §3.3): drain whatever remains
        PENDING, then stop the poll loop."""
        self._stop.set()
        total = WorkerTick()
        while True:
            tick = await self._worker.run_once()
            total = WorkerTick(
                drained=total.drained + tick.drained,
                acked=total.acked + tick.acked,
                skipped=total.skipped + tick.skipped,
                retried=total.retried + tick.retried,
                dead_lettered=total.dead_lettered + tick.dead_lettered,
            )
            if tick.drained == 0:
                return total
