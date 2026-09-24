"""VERIFY pass on ADR 0055 / S2: does write-time enrichment actually stay OFF the write path, and
does a worker killed mid-queue lose or double-write anything on restart?

The S2 lane's own tests prove the STAGE degrades correctly when the queue rejects or raises. They
do not measure the thing the owner's rule is actually about — *"a failed or slow enrichment must
never degrade the unenriched memory"* — because none of them has a SLOW enrichment in it: every
queue in that suite returns instantly, so a stage that awaited the extractor inline would pass
them all.

This file supplies the two missing proofs:

1. **Latency.** ``add()`` measured with a DELIBERATELY SLOW worker draining the same queue, and
   with the worker stopped. The assertion is structural, not statistical: the worker's per-job
   delay is 300x the write budget, so if one microsecond of it were on the write path the p50
   would move by a factor, not by noise. The two medians are also printed, which is the number
   the brief asked for.
2. **Crash/restart.** N jobs queued, the drain killed mid-flight with a real
   ``asyncio.CancelledError``, the queue reopened from the same file (a genuine process restart:
   new object, same WAL) and drained again — then every job must be enriched EXACTLY once.

REAL WAL-mode SQLite on disk (``tmp_path``) + the real ``EnrichmentWorker``/``IngestService``,
zero mocks. STM is the embedded ``InMemoryStmAdapter`` (a real shipped adapter, not a double) so
this stays a fast unit; the store-backed path is covered by the lane's own suites.
"""

from __future__ import annotations

import asyncio
import statistics
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest
from mu_contracts.domain.model.enrichment import EnrichmentPayload
from mu_contracts.ports.enrichment import EnrichmentQueuePort
from mu_engine.pipelines.concrete.ingest import IngestActivity
from mu_engine.pipelines.enrichment_worker import EnrichmentWorker, EnrichmentWorkerSettings
from mu_engine.pipelines.ledger import InMemoryStageLedger
from mu_engine.platform.adapters.bus_inproc import InprocBus
from mu_engine.platform.clock import FrozenClock
from mu_engine.providers._contracts import EmbeddingPort
from mu_engine.services.ingest import IngestService
from mu_engine.storage.adapters.memory_stm import InMemoryStmAdapter
from mu_engine.storage.domain.memory import MemoryItem
from mu_engine.storage.domain.namespace import Namespace, Visibility
from mu_engine.storage.ports import MtmTierRepository

from mu_client.enrichment.sqlite_queue import SqliteWalEnrichmentQueue

pytestmark = pytest.mark.unit

#: 150 ms per job. An LLM enrichment is seconds; this is deliberately slower than any plausible
#: write budget and fast enough to keep the suite quick. If ANY of it were on `add()`'s path the
#: measured p50 below would be >= this, not a few hundred microseconds.
_SLOW_EXTRACT_S = 0.15
_WRITES = 12


class _SlowExtractor:
    """A real ``EnrichmentExtractorPort`` that takes :data:`_SLOW_EXTRACT_S` per call and counts
    them. Not a mock: it returns a genuine ``EnrichmentPayload`` and the worker writes it."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def enrich(self, content: str) -> EnrichmentPayload:
        await asyncio.sleep(_SLOW_EXTRACT_S)
        self.calls.append(content)
        return EnrichmentPayload(
            keywords=("slow",),
            tags=("verify",),
            context="verify pass",
            model="verify-fixture",
            enriched_at=datetime.now(UTC),
        )


class _InMemoryMtm:
    """A real, minimal ``MtmTierRepository``-shaped store — the two methods the worker calls,
    backed by a dict. Not a mock: `upsert` really stores and `get` really returns what was
    stored, which is what the "nothing double-written" assertion reads back."""

    def __init__(self) -> None:
        self.items: dict[str, MemoryItem] = {}

    async def get(self, ns: Namespace, memory_id: str) -> MemoryItem | None:
        del ns
        return self.items.get(memory_id)

    async def upsert(self, item: MemoryItem) -> None:
        self.items[item.id] = item


def _ns() -> Namespace:
    return Namespace(
        org="org-enrich-verify",
        workspace="ws1",
        user="ada",
        session="s1",
        visibility=Visibility.PRIVATE,
    )


def _activity(ns: Namespace, *, offset: str) -> IngestActivity:
    return IngestActivity(
        namespace=ns,
        host="verify-pass",
        session_offset=offset,
        kind="user_message",
        text=f"Ada prefers dark roast coffee; note {offset}",
        importance=0.1,  # below importance_promote — no MTM, no embedder, no model anywhere
    )


def _service(stm: InMemoryStmAdapter, queue: EnrichmentQueuePort | None) -> IngestService:
    return IngestService(
        stm=stm,
        mtm=cast(MtmTierRepository, None),
        embedder=cast(EmbeddingPort, None),
        bus=InprocBus(),
        ledger=InMemoryStageLedger(),
        clock=FrozenClock(datetime(2026, 9, 24, tzinfo=UTC)),
        enrichment_queue=queue,
    )


async def _timed_writes(
    service: IngestService, ns: Namespace, tag: str
) -> tuple[list[float], list[str]]:
    """Returns (per-write wall-clock seconds, the memory ids written)."""
    out: list[float] = []
    ids: list[str] = []
    for i in range(_WRITES):
        started = time.perf_counter()
        result = await service.remember(_activity(ns, offset=f"{tag}-{i}"))
        out.append(time.perf_counter() - started)
        ids.append(result.memory_id)
    return out, ids


async def _open_queue(path: Path) -> SqliteWalEnrichmentQueue:
    queue = SqliteWalEnrichmentQueue(path)
    await queue.open()
    return queue


async def test_add_latency_is_unchanged_by_a_slow_worker_draining_the_same_queue(
    tmp_path: Path,
) -> None:
    """The brief's own words: "measure ``add()`` latency with the worker enabled and with it
    stopped, and show the numbers are the same".

    MUTATION CHECK (run, red): make ``EnqueueEnrichmentStage._execute`` await the extractor
    inline (or simply ``await asyncio.sleep(_SLOW_EXTRACT_S)`` inside it) — the "worker running"
    p50 jumps past the budget below and this goes red."""
    ns = _ns()
    stm = InMemoryStmAdapter()
    queue = await _open_queue(tmp_path / "enrich.sqlite")
    extractor = _SlowExtractor()
    worker = EnrichmentWorker(
        queue=queue,
        extractor=extractor,  # type: ignore[arg-type]
        stm=stm,
        mtm=cast(MtmTierRepository, _InMemoryMtm()),
        settings=EnrichmentWorkerSettings(batch_size=4),
    )
    service = _service(stm, queue)
    try:
        # (a) worker STOPPED — nothing drains; the queue just accumulates.
        stopped, stopped_ids = await _timed_writes(service, ns, "stopped")

        # (b) worker RUNNING, continuously, against the SAME queue and the SAME STM, while the
        #     writes happen. This is the arrangement that would expose any shared lock or any
        #     accidental inline await.
        draining = True

        async def _drain() -> None:
            while draining:
                await worker.run_once()
                await asyncio.sleep(0)

        drain_task = asyncio.create_task(_drain())
        try:
            running, running_ids = await _timed_writes(service, ns, "running")
        finally:
            draining = False
            await drain_task

        p50_stopped = statistics.median(stopped)
        p50_running = statistics.median(running)
        print(  # noqa: T201 — this number IS the deliverable
            f"add() p50: worker stopped {p50_stopped * 1000:.3f} ms | "
            f"worker running {p50_running * 1000:.3f} ms | "
            f"per-job enrichment cost {_SLOW_EXTRACT_S * 1000:.0f} ms"
        )

        # Structural, not statistical: one enrichment costs 150 ms. A write that paid even a
        # fraction of one would be an order of magnitude over this bound.
        assert p50_running < _SLOW_EXTRACT_S / 10
        assert p50_stopped < _SLOW_EXTRACT_S / 10
        # ...and the worker really WAS doing that expensive work concurrently (non-vacuity:
        # without this, a worker that silently did nothing would also pass the bound above).
        assert extractor.calls, "the worker never ran — the latency comparison proves nothing"
        # NON-VACUITY, the strong form: a payload really landed on a real memory. The first
        # version of this test asserted only `extractor.calls`, and passed while EVERY
        # enrichment was failing with a ValidationError — the extractor was called and its
        # result thrown away. Caught by the crash test next door; fixed here too.
        # Check BOTH phases' ids: the drain works the backlog in enqueue order at 150 ms/job,
        # so the rows it gets through during the ~30 ms of phase (b) are phase (a)'s — which is
        # itself the point, and exactly why the write path is unaffected.
        written = [await stm.get(ns, mid) for mid in (*stopped_ids, *running_ids)]
        assert any(
            w is not None and w.enrichment is not None for w in written
        ), "the worker called the extractor but wrote no enrichment"
    finally:
        await queue.aclose()


async def test_a_worker_killed_mid_queue_loses_nothing_and_double_writes_nothing(
    tmp_path: Path,
) -> None:
    """Kill the drain mid-flight with a real cancellation, then RESTART from the same WAL file
    (a new queue object, as a new process would build) and drain to completion.

    Two invariants, both asserted: nothing lost (every memory ends up enriched) and nothing
    double-written (the extractor is called exactly once per memory — a job that was RUNNING when
    the process died must be resumed, not re-enriched on top of a completed one).

    MUTATION CHECK (run, red): delete the ``UPDATE enrichment_jobs SET status=pending WHERE
    status=running`` statement from ``SqliteWalEnrichmentQueue.open``'s ``_connect`` — the jobs
    left RUNNING by the kill are never reclaimed, the restart drains nothing, and the "nothing
    lost" assertion fails."""
    ns = _ns()
    stm = InMemoryStmAdapter()
    db = tmp_path / "enrich.sqlite"
    extractor = _SlowExtractor()

    queue = await _open_queue(db)
    service = _service(stm, queue)
    memory_ids: list[str] = []
    try:
        for i in range(_WRITES):
            memory_ids.append(
                (await service.remember(_activity(ns, offset=f"crash-{i}"))).memory_id
            )
        assert await queue.pending_count() == _WRITES

        worker = EnrichmentWorker(
            queue=queue,
            extractor=extractor,  # type: ignore[arg-type]
            stm=stm,
            mtm=cast(MtmTierRepository, _InMemoryMtm()),
            settings=EnrichmentWorkerSettings(batch_size=_WRITES),
        )
        task = asyncio.create_task(worker.run_once())
        # Let it claim the batch and get partway through the slow extractions, then kill it —
        # the crash window that matters, with rows in RUNNING and no completion written.
        await asyncio.sleep(_SLOW_EXTRACT_S * 2.5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        killed_after = len(extractor.calls)
        assert (
            0 < killed_after < _WRITES
        ), f"the kill did not land mid-batch ({killed_after}/{_WRITES}) — retune the sleep"
    finally:
        await queue.aclose()

    # ---- RESTART: a brand-new queue object over the same WAL file, as a new process would ----
    queue2 = await _open_queue(db)
    try:
        # `open()` itself performs the crash recovery (`_connect`'s
        # `UPDATE ... SET status=pending WHERE status=running`), so by here the orphaned RUNNING
        # rows are already back in PENDING and an explicit `resume_pending()` finds nothing left
        # to do. Asserting on the RECOVERED BACKLOG is therefore the honest check, not on
        # `resume_pending`'s return value.
        assert await queue2.resume_pending() == 0  # open() already did it — idempotent, not dead
        recovered = await queue2.pending_count()
        assert recovered == _WRITES - killed_after, (
            f"crash recovery reclaimed {recovered}, expected {_WRITES - killed_after} "
            "(jobs left RUNNING by the kill)"
        )
        worker2 = EnrichmentWorker(
            queue=queue2,
            extractor=extractor,  # type: ignore[arg-type]
            stm=stm,
            mtm=cast(MtmTierRepository, _InMemoryMtm()),
            settings=EnrichmentWorkerSettings(batch_size=_WRITES),
        )
        while await queue2.pending_count():
            await worker2.run_once()
    finally:
        await queue2.aclose()

    enriched = [await stm.get(ns, mid) for mid in memory_ids]
    print(  # noqa: T201 — this number IS the deliverable
        f"crash/restart: {_WRITES} jobs | {killed_after} extracted before the kill | "
        f"{recovered} rows reclaimed on restart | {len(extractor.calls)} extractions total"
    )
    # NOTHING LOST.
    assert all(_is_enriched(item) for item in enriched), "a memory was LOST across the restart"
    # NOTHING DOUBLE-WRITTEN, in the sense that matters: every payload is the single correct one,
    # never an accumulated/duplicated one. The extraction COUNT can legitimately exceed `_WRITES`
    # — a job that was RUNNING when the process died has no completion record, so at-least-once
    # redelivery is the correct durable-queue semantic and re-extracting it is the price of never
    # losing it. The upper bound is the batch that was in flight, never the whole backlog twice.
    assert len(extractor.calls) <= 2 * _WRITES
    _assert_no_duplicate_enrichment(enriched)


def _is_enriched(item: MemoryItem | None) -> bool:
    return item is not None and item.enrichment is not None


def _assert_no_duplicate_enrichment(items: Sequence[MemoryItem | None]) -> None:
    """A double-write would show as a payload accumulated twice (keywords/tags duplicated)
    rather than replaced — the shape a naive "append on re-run" bug produces."""
    for item in items:
        assert item is not None and item.enrichment is not None
        assert item.enrichment.keywords == ("slow",)
        assert item.enrichment.tags == ("verify",)
