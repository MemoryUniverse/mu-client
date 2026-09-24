"""``EnrichmentWorkerLoop`` — pure unit, zero infra. A real ``EnrichmentWorker`` wired to the real
in-process ``InMemoryEnrichmentQueue``/``InMemoryStmAdapter`` (mu-core's own fakes, not mocks) +
one small hand-written flaky queue for the "a bad tick must not kill the loop" case."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from mu_contracts.domain.model.enrichment import EnrichmentJob, EnrichmentPayload
from mu_engine.pipelines.enrichment_worker import EnrichmentWorker
from mu_engine.platform.adapters.enrichment_inline import InMemoryEnrichmentQueue
from mu_engine.storage.adapters.memory_stm import InMemoryStmAdapter
from mu_engine.storage.domain.memory import (
    MemoryItem,
    MemoryKind,
    MemorySource,
    MemoryState,
    MemoryTier,
)
from mu_engine.storage.domain.namespace import Namespace, Visibility
from mu_engine.storage.domain.recall import Scored, SparseQuery

from mu_client.enrichment.worker_loop import EnrichmentWorkerLoop

pytestmark = pytest.mark.unit

_NS = Namespace(
    org="org-worker-loop", workspace="ws1", user="ada", session="s1", visibility=Visibility.PRIVATE
)


class _FakeMtm:
    async def upsert(self, item: MemoryItem) -> None:
        return None

    async def get(self, ns: Namespace, memory_id: str) -> MemoryItem | None:
        return None

    async def expire(self, ns: Namespace, memory_id: str, *, at: object) -> None:
        raise NotImplementedError

    async def semantic(
        self,
        ns: Namespace,
        query_vector: list[float],
        *,
        limit: int,
        caller_identity_set: frozenset[str] | None = None,
        sparse_query: SparseQuery | None = None,
    ) -> list[Scored[MemoryItem]]:
        raise NotImplementedError

    async def invalidate(
        self, ns: Namespace, loser_id: str, winner_id: str, *, at: object, reason: str
    ) -> None:
        raise NotImplementedError

    async def remove(self, ns: Namespace, memory_id: str) -> None:
        raise NotImplementedError

    async def scan_for_demotion(self, ns: Namespace, *, limit: int) -> list[MemoryItem]:
        raise NotImplementedError


class _FakeExtractor:
    async def enrich(self, content: str) -> EnrichmentPayload:
        return EnrichmentPayload(
            keywords=("k",),
            context="c",
            tags=("t",),
            model="m",
            enriched_at=datetime(2026, 9, 24, tzinfo=UTC),
        )


class _FlakyQueue:
    """A real (not-Mock) queue whose ``claim_batch`` raises on its first call only — the
    substrate-hiccup shape ``EnrichmentWorkerLoop`` must survive without dying."""

    def __init__(self) -> None:
        self.calls = 0

    async def submit(self, job: EnrichmentJob) -> bool:
        return True

    async def claim_batch(self, *, limit: int) -> list[EnrichmentJob]:
        self.calls += 1
        if self.calls == 1:
            raise ConnectionError("simulated transient VM hiccup")
        return []

    async def complete(self, job_id: str) -> None:
        return None

    async def fail(self, job_id: str, *, error: str, backoff_s: float) -> bool:
        return False

    async def resume_pending(self) -> int:
        return 0

    async def pending_count(self) -> int:
        return 0


def _item() -> MemoryItem:
    return MemoryItem(
        content="Ada prefers dark roast coffee",
        kind=MemoryKind.PROPOSITION,
        tier=MemoryTier.STM,
        state=MemoryState.ACTIVE,
        namespace=_NS,
        owner_id=_NS.user,
        workspace_id=_NS.workspace,
        session_id=_NS.session,
        source=MemorySource.USER,
    )


async def test_tick_drains_one_pending_job() -> None:
    stm = InMemoryStmAdapter()
    item = _item()
    await stm.put(item)
    queue = InMemoryEnrichmentQueue()
    await queue.submit(
        EnrichmentJob(
            job_id=f"enr_{item.id}",
            namespace_parts=_NS.parts(),
            memory_id=item.id,
            content_hash="h1",
            enqueued_at=datetime.now(UTC),
        )
    )
    worker = EnrichmentWorker(queue=queue, extractor=_FakeExtractor(), stm=stm, mtm=_FakeMtm())
    loop = EnrichmentWorkerLoop(worker, interval_s=60.0)

    report = await loop.tick()

    assert report.claimed == 1
    assert report.enriched == 1


async def test_start_stop_lifecycle_is_clean_and_idempotent() -> None:
    stm = InMemoryStmAdapter()
    queue = InMemoryEnrichmentQueue()
    worker = EnrichmentWorker(queue=queue, extractor=_FakeExtractor(), stm=stm, mtm=_FakeMtm())
    loop = EnrichmentWorkerLoop(worker, interval_s=0.01)

    running_before: bool = loop.is_running
    assert running_before is False
    await loop.start()
    running_after_start: bool = loop.is_running
    assert running_after_start is True
    await loop.start()  # idempotent — must not spawn a second task
    await loop.stop()
    running_after_stop: bool = loop.is_running
    assert running_after_stop is False
    await loop.stop()  # idempotent — must not raise on a second stop


async def test_a_failing_tick_does_not_kill_the_loop() -> None:
    """The money test: a tick that raises (a transient substrate hiccup) must not end the loop —
    it logs and keeps ticking. Mutation-check: removing the ``try/except Exception`` around
    ``self.tick()`` in ``_run`` would let the loop's task die silently on the first hiccup, and
    this test would then see ``is_running`` flip to ``False`` / no further ticks ever happen."""
    stm = InMemoryStmAdapter()
    flaky = _FlakyQueue()
    worker = EnrichmentWorker(queue=flaky, extractor=_FakeExtractor(), stm=stm, mtm=_FakeMtm())
    loop = EnrichmentWorkerLoop(worker, interval_s=0.01)

    await loop.start()
    try:
        # Give the loop several ticks' worth of wall time: tick 1 raises inside run_once
        # (claim_batch), tick 2+ must still happen.
        for _ in range(50):
            if flaky.calls >= 2:
                break
            await asyncio.sleep(0.01)
        assert flaky.calls >= 2, "the loop must keep ticking after a failing tick"
        assert loop.is_running, "the loop task must still be alive after a failing tick"
    finally:
        await loop.stop()
