"""``SqliteWalEnrichmentQueue`` — REAL WAL-mode SQLite, zero mocks (the same leaf-adapter
convention as ``test_sqlite_outbox.py``/``test_sqlite_wal.py``). Proves the three properties the
owner named as "where this shape goes wrong": idempotent + restart-safe, bounded (backpressure),
and correct crash-resume (``RUNNING`` -> ``PENDING`` on the next ``open()``, never a double-claim,
never a lost row)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from mu_contracts.domain.model.enrichment import EnrichmentJob

from mu_client.enrichment.sqlite_queue import SqliteWalEnrichmentQueue

pytestmark = pytest.mark.unit

_NS_PARTS = ("org1", "ws1", "ada", "s1", "private")


def _job(job_id: str, memory_id: str, *, content_hash: str = "hash1") -> EnrichmentJob:
    return EnrichmentJob(
        job_id=job_id,
        namespace_parts=_NS_PARTS,
        memory_id=memory_id,
        content_hash=content_hash,
        enqueued_at=datetime.now(UTC),
    )


async def _open(path: Path) -> SqliteWalEnrichmentQueue:
    q = SqliteWalEnrichmentQueue(path)
    await q.open()
    return q


async def test_submit_then_claim_then_complete_round_trips(tmp_path: Path) -> None:
    q = await _open(tmp_path / "enrich.sqlite")
    try:
        job = _job("enr_m1", "m1")
        assert await q.submit(job) is True

        claimed = await q.claim_batch(limit=10)
        assert [j.job_id for j in claimed] == ["enr_m1"]
        assert claimed[0].namespace_parts == _NS_PARTS
        assert claimed[0].memory_id == "m1"

        # a RUNNING row is not re-claimed by a second drain.
        assert await q.claim_batch(limit=10) == []

        await q.complete(job.job_id)
        assert await q.pending_count() == 0
    finally:
        await q.aclose()


async def test_submit_is_idempotent_on_job_id(tmp_path: Path) -> None:
    """A redelivered ``submit`` of the SAME ``job_id`` (the crash-replay shape
    ``EnqueueEnrichmentStage`` relies on) is a no-op — never a second row, never an error."""
    q = await _open(tmp_path / "enrich.sqlite")
    try:
        job = _job("enr_m1", "m1")
        assert await q.submit(job) is True
        assert await q.submit(job) is True  # redelivery
        assert await q.pending_count() == 1

        claimed = await q.claim_batch(limit=10)
        assert len(claimed) == 1
    finally:
        await q.aclose()


async def test_backpressure_rejects_past_max_pending(tmp_path: Path) -> None:
    """Bounded queue (DEV-STANDARDS: never unbounded). ``submit`` returns ``False`` — never
    raises — once ``PENDING`` is at capacity; a DIFFERENT job_id is genuinely refused, while the
    already-accepted rows are untouched."""
    q = SqliteWalEnrichmentQueue(tmp_path / "enrich.sqlite", max_pending=1)
    await q.open()
    try:
        assert await q.submit(_job("enr_m1", "m1")) is True
        assert await q.submit(_job("enr_m2", "m2")) is False  # at capacity — shed, not raised
        assert await q.pending_count() == 1

        # draining below capacity lets a new submit through again.
        claimed = await q.claim_batch(limit=10)
        assert len(claimed) == 1
        await q.complete(claimed[0].job_id)
        assert await q.submit(_job("enr_m2", "m2")) is True
    finally:
        await q.aclose()


async def test_crash_leaves_running_row_resumed_to_pending_on_reopen(tmp_path: Path) -> None:
    """The restart-safety property in the task's own words: a crashed worker must not double-write
    or lose the row. Simulated by claiming a batch (-> RUNNING) then opening a FRESH adapter
    instance against the SAME file WITHOUT ever completing it — the shape a killed-and-restarted
    daemon produces (a new process, same WAL file)."""
    path = tmp_path / "enrich.sqlite"
    q1 = await _open(path)
    job = _job("enr_m1", "m1")
    assert await q1.submit(job) is True
    claimed = await q1.claim_batch(limit=10)
    assert len(claimed) == 1
    await q1.aclose()  # crash: no complete()/fail() ever called

    q2 = SqliteWalEnrichmentQueue(path)
    await q2.open()  # open() itself resets RUNNING -> PENDING (see class docstring)
    try:
        assert await q2.pending_count() == 1
        reclaimed = await q2.claim_batch(limit=10)
        assert [j.job_id for j in reclaimed] == ["enr_m1"], "the row must be recovered, not lost"
    finally:
        await q2.aclose()


async def test_resume_pending_resets_running_rows_and_reports_the_count(tmp_path: Path) -> None:
    q = await _open(tmp_path / "enrich.sqlite")
    try:
        await q.submit(_job("enr_m1", "m1"))
        await q.submit(_job("enr_m2", "m2"))
        await q.claim_batch(limit=10)  # both -> RUNNING

        reset = await q.resume_pending()
        assert reset == 2
        assert await q.pending_count() == 2
    finally:
        await q.aclose()


async def test_fail_reschedules_then_permanently_dead_letters_after_max_attempts(
    tmp_path: Path,
) -> None:
    q = SqliteWalEnrichmentQueue(tmp_path / "enrich.sqlite", max_attempts=2)
    await q.open()
    try:
        job = _job("enr_m1", "m1")
        await q.submit(job)
        await q.claim_batch(limit=10)

        # attempt 1 of 2 — rescheduled PENDING (a real memory is unaffected either way, this
        # queue never touches it), never the terminal FAILED state yet.
        dead = await q.fail(job.job_id, error="TimeoutError", backoff_s=0.0)
        assert dead is False
        assert await q.pending_count() == 1

        await q.claim_batch(limit=10)
        # attempt 2 of 2 — max_attempts reached: PERMANENTLY failed, inspectable, not silently
        # dropped and not retried forever (never an unbounded retry loop).
        dead = await q.fail(job.job_id, error="TimeoutError", backoff_s=0.0)
        assert dead is True
        assert await q.pending_count() == 0
        assert await q.claim_batch(limit=10) == []
    finally:
        await q.aclose()


async def test_complete_on_unknown_job_id_is_a_harmless_noop(tmp_path: Path) -> None:
    """A worker retrying ``complete()`` after a crash between the write-back and the ack must be
    able to call it again safely — never a raise."""
    q = await _open(tmp_path / "enrich.sqlite")
    try:
        await q.complete("enr_never_existed")  # must not raise
    finally:
        await q.aclose()
