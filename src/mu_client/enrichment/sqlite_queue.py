"""``SqliteWalEnrichmentQueue`` — the CLIENT/FULL-LOCAL ``EnrichmentQueuePort`` adapter (ADR-0055;
AD-241; ``docs/superpowers/design/engine-core-spec.md`` §6.4 stage 4).

The owner's instruction, applied: "use the durable-execution substrate the architecture already
chose, per plane — the SQLite-WAL outbox for the client". This is a THIRD, independent SQLite-WAL
table (its own file/schema), replicating the EXACT WAL-mode pattern
``mu_client.outbox.sqlite_outbox.SqliteOutbox`` established (``PRAGMA journal_mode=WAL``,
``PRAGMA synchronous=FULL``, every blocking call off the event loop via ``asyncio.to_thread``, one
``asyncio.Lock`` serializing the shared connection) — NEVER the lifecycle job log
(``mu_client.runners.sqlite_wal.SqliteWalRunner``'s ``lifecycle_jobs`` table). S2's lane is
explicitly scoped to the ingest path + this new worker, NOT lifecycle internals; a shared table
would smuggle an unrelated write surface into a file another lane owns, and — the more concrete
reason — this queue's rows are keyed and shaped completely differently (job_id derived from
``memory_id``, no ``kind``/policy-version fields a lifecycle sweep needs). Reusing the WAL
*pattern* (proven, reviewed, crash-safe) while keeping the *table* separate is exactly the "adopt
the substrate, not a shared surface" reading of the instruction.

**Content-free (module docstring on ``mu_contracts.domain.model.enrichment`` — repeated here
because it is this file's own invariant too):** the schema below has no column for memory
content. Only ids, the namespace's 5-tuple, a hash, and status bookkeeping ever touch this file.

**Backpressure.** ``max_pending`` bounds the table's ``PENDING`` row count; ``submit`` returns
``False`` (never raises, never blocks) once that many rows are pending — the SAME shape
``EnrichmentQueuePort.submit`` documents. A write burst therefore sheds enrichment jobs, never
grows this file without limit, and never slows down ``add()`` (the check is one indexed COUNT
query, not a table scan).
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from mu_contracts.domain.model.enrichment import EnrichmentJob, EnrichmentJobStatus

from mu_client.errors import EnrichmentQueueCorruptionError

__all__ = ["SqliteWalEnrichmentQueue"]

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS enrichment_jobs (
  job_id TEXT PRIMARY KEY,
  namespace_parts TEXT NOT NULL,
  memory_id TEXT NOT NULL,
  content_hash TEXT,
  enqueued_at TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  ready_at TEXT,
  completed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_enrichment_status_seq ON enrichment_jobs(status, enqueued_at);
"""


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _row_to_job(row: tuple[Any, ...]) -> EnrichmentJob:
    job_id, namespace_parts_json, memory_id, content_hash, enqueued_at = row
    p0, p1, p2, p3, p4 = json.loads(namespace_parts_json)
    return EnrichmentJob(
        job_id=job_id,
        namespace_parts=(p0, p1, p2, p3, p4),
        memory_id=memory_id,
        content_hash=content_hash or "",
        enqueued_at=datetime.fromisoformat(enqueued_at),
    )


class SqliteWalEnrichmentQueue:
    """``EnrichmentQueuePort``. Idempotent on ``job.job_id`` (``INSERT OR IGNORE``, the SAME
    idempotent-redelivery primitive ``SqliteOutbox.append``/``SqliteWalRunner.submit`` use)."""

    def __init__(self, path: Path, *, max_pending: int = 2000, max_attempts: int = 5) -> None:
        self._path = path.expanduser()
        self._conn: sqlite3.Connection | None = None
        self._lock = asyncio.Lock()
        self._max_pending = max_pending
        self._max_attempts = max_attempts

    @property
    def is_open(self) -> bool:
        return self._conn is not None

    async def open(self) -> None:
        """Idempotent. Resets any row a crash left ``running`` back to ``pending`` — the SAME
        boot-time recovery :meth:`resume_pending` performs; called here too so a fresh open()
        alone (no explicit ``resume_pending`` call) never leaves a crash-orphaned row stuck."""
        if self._conn is not None:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)

        def _connect() -> sqlite3.Connection:
            try:
                conn = sqlite3.connect(
                    str(self._path), check_same_thread=False, isolation_level=None
                )
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=FULL")
                conn.executescript(_SCHEMA_SQL)
                conn.execute(
                    "UPDATE enrichment_jobs SET status=? WHERE status=?",
                    (EnrichmentJobStatus.PENDING.value, EnrichmentJobStatus.RUNNING.value),
                )
            except sqlite3.DatabaseError as exc:
                raise EnrichmentQueueCorruptionError(
                    f"enrichment queue WAL at {self._path} is unreadable/corrupt: {exc}"
                ) from exc
            return conn

        self._conn = await asyncio.to_thread(_connect)

    async def aclose(self) -> None:
        conn, self._conn = self._conn, None
        if conn is not None:
            await asyncio.to_thread(conn.close)

    def _require_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("SqliteWalEnrichmentQueue.open() was not called")
        return self._conn

    # ------------------------------------------------------------------------------- Port: submit
    async def submit(self, job: EnrichmentJob) -> bool:
        conn = self._require_conn()

        def _do() -> bool:
            conn.execute("BEGIN IMMEDIATE")
            try:
                existing = conn.execute(
                    "SELECT 1 FROM enrichment_jobs WHERE job_id=?", (job.job_id,)
                ).fetchone()
                if existing is not None:
                    conn.commit()
                    return True  # idempotent resubmit — no-op
                (pending,) = conn.execute(
                    "SELECT COUNT(*) FROM enrichment_jobs WHERE status=?",
                    (EnrichmentJobStatus.PENDING.value,),
                ).fetchone()
                if pending >= self._max_pending:
                    conn.commit()
                    return False  # backpressure — bounded queue at capacity
                conn.execute(
                    "INSERT INTO enrichment_jobs"
                    "(job_id, namespace_parts, memory_id, content_hash, enqueued_at, status)"
                    " VALUES (?,?,?,?,?,?)",
                    (
                        job.job_id,
                        json.dumps(list(job.namespace_parts)),
                        job.memory_id,
                        job.content_hash,
                        job.enqueued_at.isoformat(),
                        EnrichmentJobStatus.PENDING.value,
                    ),
                )
                conn.commit()
                return True
            except BaseException:
                conn.rollback()
                raise

        async with self._lock:
            return await asyncio.to_thread(_do)

    async def claim_batch(self, *, limit: int) -> list[EnrichmentJob]:
        conn = self._require_conn()

        def _do() -> list[tuple[Any, ...]]:
            conn.execute("BEGIN IMMEDIATE")
            try:
                rows = conn.execute(
                    "SELECT job_id, namespace_parts, memory_id, content_hash, enqueued_at "
                    "FROM enrichment_jobs WHERE status=? AND (ready_at IS NULL OR ready_at <= ?) "
                    "ORDER BY enqueued_at LIMIT ?",
                    (EnrichmentJobStatus.PENDING.value, _now_iso(), limit),
                ).fetchall()
                job_ids = [r[0] for r in rows]
                if job_ids:
                    placeholders = ",".join("?" for _ in job_ids)
                    conn.execute(
                        f"UPDATE enrichment_jobs SET status=? WHERE job_id IN ({placeholders})",  # noqa: S608
                        (EnrichmentJobStatus.RUNNING.value, *job_ids),
                    )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            return list(rows)

        async with self._lock:
            rows = await asyncio.to_thread(_do)
        return [_row_to_job(r) for r in rows]

    async def complete(self, job_id: str) -> None:
        conn = self._require_conn()

        def _do() -> None:
            conn.execute(
                "UPDATE enrichment_jobs SET status=?, completed_at=? WHERE job_id=?",
                (EnrichmentJobStatus.DONE.value, _now_iso(), job_id),
            )

        async with self._lock:
            await asyncio.to_thread(_do)

    async def fail(self, job_id: str, *, error: str, backoff_s: float) -> bool:
        conn = self._require_conn()
        ready_at = datetime.now(UTC).timestamp() + backoff_s

        def _do() -> bool:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT attempts FROM enrichment_jobs WHERE job_id=?", (job_id,)
                ).fetchone()
                if row is None:
                    conn.commit()
                    return False
                attempts = row[0] + 1
                if attempts >= self._max_attempts:
                    conn.execute(
                        "UPDATE enrichment_jobs SET status=?, attempts=?, last_error=?, "
                        "completed_at=? WHERE job_id=?",
                        (
                            EnrichmentJobStatus.FAILED.value,
                            attempts,
                            error,
                            _now_iso(),
                            job_id,
                        ),
                    )
                    conn.commit()
                    return True
                conn.execute(
                    "UPDATE enrichment_jobs SET status=?, attempts=?, last_error=?, "
                    "ready_at=datetime(?, 'unixepoch') WHERE job_id=?",
                    (
                        EnrichmentJobStatus.PENDING.value,
                        attempts,
                        error,
                        ready_at,
                        job_id,
                    ),
                )
                conn.commit()
                return False
            except BaseException:
                conn.rollback()
                raise

        async with self._lock:
            return await asyncio.to_thread(_do)

    async def resume_pending(self) -> int:
        conn = self._require_conn()

        def _do() -> int:
            conn.execute("BEGIN IMMEDIATE")
            try:
                cur = conn.execute(
                    "UPDATE enrichment_jobs SET status=? WHERE status=?",
                    (EnrichmentJobStatus.PENDING.value, EnrichmentJobStatus.RUNNING.value),
                )
                conn.commit()
                return cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0
            except BaseException:
                conn.rollback()
                raise

        async with self._lock:
            return await asyncio.to_thread(_do)

    async def pending_count(self) -> int:
        conn = self._require_conn()

        def _do() -> int:
            (count,) = conn.execute(
                "SELECT COUNT(*) FROM enrichment_jobs WHERE status=?",
                (EnrichmentJobStatus.PENDING.value,),
            ).fetchone()
            return int(count)

        async with self._lock:
            return await asyncio.to_thread(_do)
