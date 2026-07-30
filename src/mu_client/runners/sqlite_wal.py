"""``SqliteWalRunner`` + ``SqliteWalLeaseAdapter`` — the client durable job log
(``LifecycleWorkflowRunnerPort``, spec §5, lines 222-254) and the cross-OS-process lifecycle lease
(``LifecycleLeasePort``, spec §4b, lines 173-192) sharing ONE SQLite-WAL database file
(GAPSWEEP BQ1 closure, ``memory-lifecycle-manager-GAPSWEEP.md`` line 794 — "reuses the outbox
substrate", a second lock file / ``fcntl.flock`` is explicitly rejected).

WAL-mode pattern replicated verbatim from ``mu_client.outbox.sqlite_outbox.SqliteOutbox`` (the
"exact WAL-mode SQLite pattern" this task's packet points at): ``PRAGMA journal_mode=WAL``,
``PRAGMA synchronous=FULL``, every blocking ``sqlite3`` call off the event loop via
``asyncio.to_thread``, one ``asyncio.Lock`` serializing the shared connection so a cancelled
caller never leaves the connection mid-statement for the next caller (DEV-STANDARDS rule 1).

Both classes independently ``open()`` their own connection but execute the identical
``_SCHEMA_SQL`` (idempotent ``CREATE TABLE IF NOT EXISTS``) against the SAME file path — whichever
one is constructed first lays down both tables, satisfying "the job log and the lease table live
in the SAME sqlite WAL database file" (this task's acceptance) without coupling the two adapters'
Python object graphs (each is a genuine, independently-constructible adapter for its own Port —
``LifecycleWorkflowRunnerPort`` / ``LifecycleLeasePort``, spec §5 / §4b — and, critically for
AC-1.1, each OS process in the cross-process test constructs its OWN instance/connection against
the shared file; a SQLite connection cannot cross a process boundary anyway).

**Cross-process mutual exclusion (AC-1.1, BQ1).** ``SqliteWalLeaseAdapter.acquire()`` uses
``BEGIN IMMEDIATE`` — SQLite's own file-level exclusive-write-intent lock, portable across the
daemon/CLI OS-process boundary — unlike the in-process-only ``asyncio.Lock`` the pre-existing
``InProcessWriterLease`` uses (``mu_engine.pipelines.distill``, CANONICAL §7.5, unrelated/unchanged
distill lease). A live (unexpired) row for a different holder makes acquisition observe the row
and FAIL FAST (raises :class:`LifecycleLeaseBusyError`) rather than block/retry — spec §4b's own
words: "the caller backs off" is the caller's decision, not this adapter's.

**Lease naming (CANONICAL §7.5 plane-qualified convention, this task's canonical_rule).** The
lease's NAME (used in the busy-error message and available via :meth:`SqliteWalLeaseAdapter.
lease_name`) follows the identical ``{lease}:local:{device_id}:{grain}`` shape as the EXISTS
session-inclusive distill lease (``distill-writer-lease:local:{device_id}:{ns.to_prefix()}``,
CANONICAL §7.5) — here ``lifecycle-sweep-lease:local:{device_id}:{user_prefix}`` — a DISTINCT lease
name at a coarser, session-spanning grain (``UserPrefix``, not ``Namespace``), never a reshape of
the distill lease. The SQLite row's own primary key stays the bare ``user_prefix`` (spec §4b's
literal ``lifecycle_leases`` schema — "one row per ``user_prefix``"; the SQLite file is already
device-scoped by virtue of living on one device's disk, so ``device_id`` need not be folded into
the row's key for LOCAL uniqueness), with ``holder_kind = "local:{device_id}"`` stored per-row so
the plane+device qualifier stays inspectable — the same information the naming convention
requires, expressed as a row field instead of a compound primary key, so a bare-``user_prefix``
``SELECT``/``UPSERT`` (spec §4b's own acquire mechanics) needs no string-building at the SQL layer.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import sqlite3
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from mu_contracts.domain.model.lifecycle import (
    JobHandle,
    JobResult,
    JobStatus,
    LifecycleJob,
    LifecycleJobKind,
    UserPrefix,
)
from mu_contracts.ports.time import Clock
from mu_engine.lifecycle.settings import OwnershipSettings
from mu_engine.platform.clock import SystemClock

from mu_client.errors import ClientError

__all__ = ["LifecycleLeaseBusyError", "SqliteWalLeaseAdapter", "SqliteWalRunner"]

# Both tables live in the SAME file (BQ1) regardless of which class opens it first.
_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS lifecycle_jobs (
  job_id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  user_prefix TEXT NOT NULL,
  namespace TEXT,
  memory_id TEXT,
  after_offset INTEGER,
  submitted_at TEXT NOT NULL,
  config_version TEXT NOT NULL,
  policy_version TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  error TEXT,
  completed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_lifecycle_jobs_status ON lifecycle_jobs(status, submitted_at);
CREATE TABLE IF NOT EXISTS lifecycle_leases (
  user_prefix TEXT PRIMARY KEY,
  holder_kind TEXT NOT NULL,
  holder_pid INTEGER NOT NULL,
  acquired_at TEXT NOT NULL,
  expires_at TEXT NOT NULL
);
"""


def _open_wal_connection(path: Path) -> sqlite3.Connection:
    """The shared connect routine (outbox's exact pattern): WAL + synchronous=FULL + the full
    shared schema, idempotent regardless of which adapter opens the file first."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.executescript(_SCHEMA_SQL)
    return conn


def _row_to_job(row: tuple[Any, ...]) -> LifecycleJob:
    (
        job_id,
        kind,
        user_prefix,
        namespace,
        memory_id,
        after_offset,
        submitted_at,
        config_version,
        policy_version,
    ) = row
    return LifecycleJob(
        job_id=job_id,
        kind=LifecycleJobKind(kind),
        user_prefix=UserPrefix._from_validated_str(user_prefix),
        namespace=namespace,
        memory_id=memory_id,
        after_offset=after_offset,
        submitted_at=datetime.fromisoformat(submitted_at),
        config_version=config_version,
        policy_version=policy_version,
    )


class SqliteWalRunner:
    """``LifecycleWorkflowRunnerPort`` (spec §5) — the CLIENT durable job log. Crash-resume;
    at-least-once; idempotent on ``job.job_id`` (``INSERT OR IGNORE``, mirroring the outbox's
    ``UNIQUE(activity_id)`` idempotent-redelivery pattern).

    ``claim_next``/``complete`` are ADDITIVE beyond the Port (structural ``Protocol`` conformance
    permits extra public methods) — the Port itself only durably logs+replays; something else
    (a later Stage-1 worker loop, out of this task's owned files) drains ``PENDING`` rows and
    executes them. They exist here so this adapter is a complete, testable durable-log substrate
    on its own and so the crash-recovery acceptance test can simulate "mid-job" realistically.
    """

    def __init__(self, path: Path, *, clock: Clock | None = None) -> None:
        self._path = path.expanduser()
        self._conn: sqlite3.Connection | None = None
        self._lock = asyncio.Lock()
        self._clock: Clock = clock or SystemClock()

    @property
    def is_open(self) -> bool:
        return self._conn is not None

    async def open(self) -> None:
        """Idempotent. Does NOT itself reset crashed ``RUNNING`` rows — that is
        :meth:`resume_pending`'s explicit job (spec: "resume_pending() replays ... on a fresh
        construction"), called by whoever owns daemon boot, mirroring the port's own naming."""
        if self._conn is not None:
            return
        self._conn = await asyncio.to_thread(_open_wal_connection, self._path)

    async def aclose(self) -> None:
        conn, self._conn = self._conn, None
        if conn is not None:
            await asyncio.to_thread(conn.close)

    def _require_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("SqliteWalRunner.open() was not called")
        return self._conn

    # ------------------------------------------------------------------------ Port: durable write
    async def submit(self, job: LifecycleJob) -> JobHandle:
        """Durable enqueue; returns fast. Idempotent on ``job.job_id`` (a redelivered submit of
        the same ``job_id`` is a no-op, the existing row is left untouched)."""
        conn = self._require_conn()

        def _do() -> None:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO lifecycle_jobs"
                    "(job_id, kind, user_prefix, namespace, memory_id, after_offset,"
                    " submitted_at, config_version, policy_version, status)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (
                        job.job_id,
                        job.kind.value,
                        str(job.user_prefix),
                        job.namespace,
                        job.memory_id,
                        job.after_offset,
                        job.submitted_at.isoformat(),
                        job.config_version,
                        job.policy_version,
                        JobStatus.PENDING.value,
                    ),
                )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise

        async with self._lock:
            await asyncio.to_thread(_do)
        return JobHandle(job_id=job.job_id, submitted_at=job.submitted_at)

    async def await_result(
        self,
        handle: JobHandle,
        *,
        poll_interval_s: float = 0.05,
        timeout_s: float | None = None,
    ) -> JobResult:
        """Optional; reads never call this. Polls the durable row until terminal
        (``SUCCEEDED``/``FAILED``) or ``timeout_s`` elapses (``None`` = wait indefinitely)."""
        conn = self._require_conn()
        loop = asyncio.get_running_loop()
        deadline = None if timeout_s is None else loop.time() + timeout_s

        def _fetch() -> tuple[Any, ...] | None:
            row = conn.execute(
                "SELECT status, error, completed_at FROM lifecycle_jobs WHERE job_id=?",
                (handle.job_id,),
            ).fetchone()
            return tuple(row) if row is not None else None

        while True:
            async with self._lock:
                row = await asyncio.to_thread(_fetch)
            if row is None:
                raise LookupError(f"unknown job_id={handle.job_id!r}")
            status = JobStatus(row[0])
            if status in (JobStatus.SUCCEEDED, JobStatus.FAILED):
                completed_at = datetime.fromisoformat(row[2]) if row[2] is not None else None
                return JobResult(
                    job_id=handle.job_id,
                    status=status,
                    error=row[1],
                    completed_at=completed_at,
                )
            if deadline is not None and loop.time() >= deadline:
                raise TimeoutError(f"await_result timed out for job_id={handle.job_id!r}")
            await asyncio.sleep(poll_interval_s)

    async def resume_pending(self) -> int:
        """Crash-recovery replay on boot. Any ``RUNNING`` row is presumed crashed-mid-job (the
        process that ``claim_next``'d it died before calling :meth:`complete`) and is reset to
        ``PENDING`` — the load-bearing crash-safety property, mirroring
        ``SqliteOutbox.open()``'s ``INFLIGHT -> PENDING`` recovery exactly. Returns the count of
        jobs now eligible to run (``PENDING``, including any freshly reset by this call)."""
        conn = self._require_conn()

        def _do() -> int:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "UPDATE lifecycle_jobs SET status=? WHERE status=?",
                    (JobStatus.PENDING.value, JobStatus.RUNNING.value),
                )
                row = conn.execute(
                    "SELECT COUNT(*) FROM lifecycle_jobs WHERE status=?",
                    (JobStatus.PENDING.value,),
                ).fetchone()
                conn.commit()
                return int(row[0])
            except BaseException:
                conn.rollback()
                raise

        async with self._lock:
            return await asyncio.to_thread(_do)

    # ------------------------------------------------------------------------- additive: execution
    async def claim_next(self) -> LifecycleJob | None:
        """Atomically claims the oldest ``PENDING`` job (``PENDING -> RUNNING``), returning it, or
        ``None`` if none is pending. NOT part of ``LifecycleWorkflowRunnerPort`` — a worker loop
        (a later Stage-1 task) drains through this; kept here because the durable log substrate
        that owns the schema is the natural, single owner of the state-machine transition."""
        conn = self._require_conn()

        def _do() -> tuple[Any, ...] | None:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT job_id, kind, user_prefix, namespace, memory_id, after_offset,"
                    " submitted_at, config_version, policy_version FROM lifecycle_jobs"
                    " WHERE status=? ORDER BY submitted_at LIMIT 1",
                    (JobStatus.PENDING.value,),
                ).fetchone()
                if row is not None:
                    conn.execute(
                        "UPDATE lifecycle_jobs SET status=? WHERE job_id=?",
                        (JobStatus.RUNNING.value, row[0]),
                    )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            return tuple(row) if row is not None else None

        async with self._lock:
            row = await asyncio.to_thread(_do)
        return None if row is None else _row_to_job(row)

    async def complete(self, job_id: str, *, error: str | None = None) -> None:
        """Marks a ``RUNNING`` job terminal: ``SUCCEEDED`` if ``error is None`` else ``FAILED``.
        Additive (see :meth:`claim_next`)."""
        conn = self._require_conn()
        status = JobStatus.FAILED.value if error is not None else JobStatus.SUCCEEDED.value
        completed_at = self._clock.now().isoformat()

        def _do() -> None:
            conn.execute(
                "UPDATE lifecycle_jobs SET status=?, error=?, completed_at=? WHERE job_id=?",
                (status, error, completed_at, job_id),
            )

        async with self._lock:
            await asyncio.to_thread(_do)

    async def __aenter__(self) -> SqliteWalRunner:
        await self.open()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()


class LifecycleLeaseBusyError(ClientError):
    """A live (unexpired) row already exists for a different holder — ``acquire()`` fails fast
    (spec §4b: "the caller backs off"; no built-in retry/backoff inside the adapter itself)."""

    def __init__(self, lease_name: str) -> None:
        self.lease_name = lease_name
        super().__init__(f"lifecycle-sweep-lease busy: {lease_name}")


class SqliteWalLeaseAdapter:
    """``LifecycleLeasePort`` (spec §4b) — a ``BEGIN IMMEDIATE`` row lock on the SAME local
    job-log database :class:`SqliteWalRunner` opens (BQ1 closure). Acquire-or-defer,
    plane-qualified; keyed on :class:`UserPrefix` (session-spanning grain), NOT ``Namespace`` —
    a distinct Protocol/lease from CANONICAL §7.5's session-inclusive distill lease
    (``mu_engine.pipelines.distill.WriterLeasePort``/``InProcessWriterLease``, untouched)."""

    def __init__(
        self,
        path: Path,
        *,
        device_id: str,
        clock: Clock | None = None,
        ownership: OwnershipSettings | None = None,
    ) -> None:
        settings = ownership or OwnershipSettings()
        self._path = path.expanduser()
        self._device_id = device_id
        self._clock: Clock = clock or SystemClock()
        self._lease_ttl_s = settings.lease_ttl_s
        self._lease_heartbeat_s = settings.lease_heartbeat_s
        self._conn: sqlite3.Connection | None = None
        self._lock = asyncio.Lock()

    @property
    def is_open(self) -> bool:
        return self._conn is not None

    async def open(self) -> None:
        if self._conn is not None:
            return
        self._conn = await asyncio.to_thread(_open_wal_connection, self._path)

    async def aclose(self) -> None:
        conn, self._conn = self._conn, None
        if conn is not None:
            await asyncio.to_thread(conn.close)

    def _require_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("SqliteWalLeaseAdapter.open() was not called")
        return self._conn

    def lease_name(self, prefix: UserPrefix) -> str:
        """The plane-qualified lease NAME (CANONICAL §7.5 convention; this task's canonical_rule)
        — ``lifecycle-sweep-lease:local:{device_id}:{user_prefix}``. Distinct lease name/grain
        from the distill lease's ``distill-writer-lease:local:{device_id}:{ns.to_prefix()}``."""
        return f"lifecycle-sweep-lease:local:{self._device_id}:{prefix}"

    # ------------------------------------------------------------------------------ Port: acquire
    @contextlib.asynccontextmanager
    async def acquire(self, prefix: UserPrefix) -> AsyncIterator[None]:
        """``BEGIN IMMEDIATE`` -> SELECT the row -> if absent/expired, UPSERT with
        ``expires_at = Clock.now() + lease_ttl_s`` -> COMMIT (spec §4b acquire mechanics exactly).
        A live row for a different holder makes acquisition observe it and FAIL FAST with
        :class:`LifecycleLeaseBusyError` (spec: "the caller backs off"). While held, a background
        heartbeat renews ``expires_at`` every ``lease_heartbeat_s`` (real-time cadence; the
        written value itself is computed off the injected :class:`Clock`, so a test can prove
        renewal deterministically via :meth:`renew` without waiting on the real-time loop)."""
        conn = self._require_conn()
        now = self._clock.now()
        pid = os.getpid()
        holder_kind = f"local:{self._device_id}"
        expires_at = (now + timedelta(seconds=self._lease_ttl_s)).isoformat()
        key = str(prefix)

        def _try_acquire() -> bool:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT expires_at FROM lifecycle_leases WHERE user_prefix=?", (key,)
                ).fetchone()
                if row is not None and datetime.fromisoformat(row[0]) >= now:
                    conn.rollback()
                    return False
                conn.execute(
                    "INSERT INTO lifecycle_leases"
                    "(user_prefix, holder_kind, holder_pid, acquired_at, expires_at)"
                    " VALUES (?,?,?,?,?)"
                    " ON CONFLICT(user_prefix) DO UPDATE SET"
                    " holder_kind=excluded.holder_kind, holder_pid=excluded.holder_pid,"
                    " acquired_at=excluded.acquired_at, expires_at=excluded.expires_at",
                    (key, holder_kind, pid, now.isoformat(), expires_at),
                )
                conn.commit()
                return True
            except BaseException:
                conn.rollback()
                raise

        async with self._lock:
            acquired = await asyncio.to_thread(_try_acquire)
        if not acquired:
            raise LifecycleLeaseBusyError(self.lease_name(prefix))

        renew_task = asyncio.create_task(self._renew_loop(prefix))
        try:
            yield
        finally:
            renew_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await renew_task
            await self._release(prefix, pid)

    async def _renew_loop(self, prefix: UserPrefix) -> None:
        """Background heartbeat: real-time cadence (``lease_heartbeat_s`` seconds of wall clock
        between renewals — the holder must stay alive+scheduled to renew, exactly the property
        that makes a genuinely dead/crashed holder eventually lose the row), but the value
        written into ``expires_at`` is always ``self._clock.now() + lease_ttl_s`` (injected Clock,
        spec §19) — never ``time.time()``."""
        while True:
            await asyncio.sleep(self._lease_heartbeat_s)
            await self.renew(prefix)

    async def renew(self, prefix: UserPrefix) -> bool:
        """One heartbeat cycle: ``BEGIN IMMEDIATE -> UPDATE expires_at -> COMMIT`` against THIS
        process's own row (``WHERE holder_pid=?``) — spec §4b's exact renewal mechanism. Returns
        ``False`` (a no-op) if this process no longer owns the row (e.g. the lease already lapsed
        and a different holder reclaimed it) so a stale holder can never resurrect an expired
        lease out from under a legitimate new holder."""
        conn = self._require_conn()
        pid = os.getpid()
        new_expires_at = (self._clock.now() + timedelta(seconds=self._lease_ttl_s)).isoformat()
        key = str(prefix)

        def _do() -> bool:
            conn.execute("BEGIN IMMEDIATE")
            try:
                cur = conn.execute(
                    "UPDATE lifecycle_leases SET expires_at=? WHERE user_prefix=? AND holder_pid=?",
                    (new_expires_at, key, pid),
                )
                conn.commit()
                return cur.rowcount > 0
            except BaseException:
                conn.rollback()
                raise

        async with self._lock:
            return await asyncio.to_thread(_do)

    async def _release(self, prefix: UserPrefix, pid: int) -> None:
        """Clean release: DELETE the row (spec §4b "on clean sweep completion, DELETE the row").
        Only deletes if still owned by ``pid`` — never clobbers a different holder's row (e.g. one
        that has since reclaimed an expired lease this process no longer owns)."""
        conn = self._require_conn()
        key = str(prefix)

        def _do() -> None:
            conn.execute(
                "DELETE FROM lifecycle_leases WHERE user_prefix=? AND holder_pid=?", (key, pid)
            )

        async with self._lock:
            await asyncio.to_thread(_do)

    async def __aenter__(self) -> SqliteWalLeaseAdapter:
        await self.open()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()
