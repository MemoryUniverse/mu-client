"""``SqliteOutbox`` — the durability spine (capture-spec.md §8.3). REAL WAL-mode SQLite;
``append`` writes the record AND advances the source checkpoint in ONE transaction, fsyncs
(``synchronous=FULL``), THEN the caller returns — the durability boundary is BEFORE the host is
acked. ``UNIQUE(activity_id)`` makes a redelivery a no-op (idempotent, at-least-once).

Cancellation-safe (DEV-STANDARDS rule 1): every blocking sqlite3 call runs off the event loop via
``asyncio.to_thread``; a single ``asyncio.Lock`` serializes access to the one shared connection
(sqlite3 connections are not safe for concurrent use from multiple threads even with
``check_same_thread=False``) so a cancelled caller never leaves the connection mid-statement for
the next caller.
"""

from __future__ import annotations

import asyncio
import hashlib
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from mu_client.capture.model import CaptureCheckpoint, HostKind, RawActivity
from mu_client.errors import OutboxCorruptionError
from mu_client.outbox.model import OutboxBatch, OutboxRecord, RecordState

__all__ = ["SqliteOutbox"]

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS outbox (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  activity_id TEXT NOT NULL,
  host TEXT NOT NULL,
  kind TEXT NOT NULL,
  session_id TEXT NOT NULL,
  content_hash TEXT,
  activity_json TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  ready_at TEXT,
  enqueued_at TEXT NOT NULL,
  UNIQUE(activity_id)
);
CREATE INDEX IF NOT EXISTS idx_outbox_state_seq ON outbox(state, seq);
CREATE TABLE IF NOT EXISTS checkpoints (
  source_id TEXT PRIMARY KEY, file_path TEXT, byte_offset INTEGER, updated_at TEXT
);
CREATE TABLE IF NOT EXISTS dead_letter (
  id INTEGER PRIMARY KEY AUTOINCREMENT, host TEXT, reason TEXT, raw_sha256 TEXT, raw BLOB, at TEXT
);
"""


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


class SqliteOutbox:
    """capture-spec.md §8.3 ``OutboxPort`` — the ONE implementation the daemon and the daemonless
    ``mu capture-once``/``mu flush`` CLI paths share (daemon-app-skeleton-spec.md §6 "Skeleton
    obligation": one outbox, one substrate, differing only in trigger)."""

    def __init__(self, path: Path) -> None:
        self._path = path.expanduser()
        self._conn: sqlite3.Connection | None = None
        self._lock = asyncio.Lock()

    @property
    def is_open(self) -> bool:
        return self._conn is not None

    async def open(self) -> None:
        """Idempotent. Recovers any row a crash left ``INFLIGHT`` (a worker died between
        ``drain`` and ``ack``) back to ``PENDING`` — the load-bearing crash-safety property
        (capture-spec.md §13: "crash between append and ack re-drives on next start with no
        double remember", guaranteed downstream by ``UNIQUE(activity_id)`` on re-``remember``)."""
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
                    "UPDATE outbox SET state=? WHERE state=?",
                    (RecordState.PENDING.value, RecordState.INFLIGHT.value),
                )
            except sqlite3.DatabaseError as exc:
                raise OutboxCorruptionError(
                    f"outbox WAL at {self._path} is unreadable/corrupt: {exc}"
                ) from exc
            return conn

        self._conn = await asyncio.to_thread(_connect)

    async def aclose(self) -> None:
        conn, self._conn = self._conn, None
        if conn is not None:
            await asyncio.to_thread(conn.close)

    def _require_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("SqliteOutbox.open() was not called")
        return self._conn

    # --------------------------------------------------------------------------------- durability
    async def append(
        self, activity: RawActivity, *, checkpoint: CaptureCheckpoint | None = None
    ) -> OutboxRecord:
        """One transaction: record + checkpoint advance together, fsync'd (``synchronous=FULL``)
        before returning. ``INSERT OR IGNORE`` + ``UNIQUE(activity_id)`` makes a redelivery a
        no-op — the SAME row (not a duplicate) is returned either way."""
        conn = self._require_conn()

        def _do() -> tuple[int, str, int, str]:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO outbox"
                    "(activity_id, host, kind, session_id, content_hash, activity_json, state,"
                    " attempts, enqueued_at) VALUES (?,?,?,?,?,?,?,0,?)",
                    (
                        activity.activity_id,
                        activity.host.value,
                        activity.kind.value,
                        activity.session_id,
                        activity.content_hash,
                        activity.model_dump_json(),
                        RecordState.PENDING.value,
                        _now_iso(),
                    ),
                )
                if checkpoint is not None:
                    conn.execute(
                        "INSERT INTO checkpoints(source_id, file_path, byte_offset, updated_at) "
                        "VALUES (?,?,?,?) ON CONFLICT(source_id) DO UPDATE SET "
                        "file_path=excluded.file_path, byte_offset=excluded.byte_offset, "
                        "updated_at=excluded.updated_at",
                        (
                            checkpoint.source_id,
                            checkpoint.file_path,
                            checkpoint.byte_offset,
                            checkpoint.updated_at.isoformat(),
                        ),
                    )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            row = conn.execute(
                "SELECT seq, state, attempts, enqueued_at FROM outbox WHERE activity_id=?",
                (activity.activity_id,),
            ).fetchone()
            seq, state, attempts, enqueued_at = row
            return seq, state, attempts, enqueued_at

        async with self._lock:
            seq, state, attempts, enqueued_at = await asyncio.to_thread(_do)
        return OutboxRecord(
            seq=seq,
            activity=activity,
            state=RecordState(state),
            attempts=attempts,
            enqueued_at=datetime.fromisoformat(enqueued_at),
        )

    async def drain(self, *, batch_size: int) -> OutboxBatch:
        """``PENDING -> INFLIGHT`` atomically; a row past its backoff ``ready_at`` is eligible."""
        conn = self._require_conn()

        def _do() -> list[tuple[int, str, str, int, str]]:
            conn.execute("BEGIN IMMEDIATE")
            try:
                rows = conn.execute(
                    "SELECT seq, activity_json, state, attempts, enqueued_at FROM outbox "
                    "WHERE state=? AND (ready_at IS NULL OR ready_at <= ?) ORDER BY seq LIMIT ?",
                    (RecordState.PENDING.value, _now_iso(), batch_size),
                ).fetchall()
                seqs = [r[0] for r in rows]
                if seqs:
                    placeholders = ",".join("?" for _ in seqs)
                    conn.execute(
                        f"UPDATE outbox SET state=? WHERE seq IN ({placeholders})",  # noqa: S608
                        (RecordState.INFLIGHT.value, *seqs),
                    )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            return list(rows)

        async with self._lock:
            rows = await asyncio.to_thread(_do)
        return OutboxBatch(
            records=[
                OutboxRecord(
                    seq=r[0],
                    activity=RawActivity.model_validate_json(r[1]),
                    state=RecordState.INFLIGHT,
                    attempts=r[3],
                    enqueued_at=datetime.fromisoformat(r[4]),
                )
                for r in rows
            ]
        )

    async def ack(self, seqs: list[int]) -> None:
        if not seqs:
            return
        conn = self._require_conn()
        placeholders = ",".join("?" for _ in seqs)

        def _do() -> None:
            conn.execute(
                f"UPDATE outbox SET state=? WHERE seq IN ({placeholders})",  # noqa: S608
                (RecordState.ACKED.value, *seqs),
            )

        async with self._lock:
            await asyncio.to_thread(_do)

    async def retry_later(self, seq: int, *, error: str, backoff_s: float) -> None:
        conn = self._require_conn()
        ready_at = datetime.now(UTC).timestamp() + backoff_s

        def _do() -> None:
            conn.execute(
                "UPDATE outbox SET state=?, attempts=attempts+1, last_error=?, "
                "ready_at=datetime(?, 'unixepoch') WHERE seq=?",
                (RecordState.PENDING.value, error, ready_at, seq),
            )

        async with self._lock:
            await asyncio.to_thread(_do)

    async def dead_letter(self, seq: int, *, error: str) -> None:
        conn = self._require_conn()

        def _do() -> None:
            conn.execute(
                "UPDATE outbox SET state=?, last_error=? WHERE seq=?",
                (RecordState.DEAD.value, error, seq),
            )

        async with self._lock:
            await asyncio.to_thread(_do)

    async def redrive_dead(self, *, limit: int) -> int:
        conn = self._require_conn()

        def _do() -> int:
            conn.execute("BEGIN IMMEDIATE")
            try:
                rows = conn.execute(
                    "SELECT seq FROM outbox WHERE state=? ORDER BY seq LIMIT ?",
                    (RecordState.DEAD.value, limit),
                ).fetchall()
                seqs = [r[0] for r in rows]
                if seqs:
                    placeholders = ",".join("?" for _ in seqs)
                    conn.execute(
                        f"UPDATE outbox SET state=?, ready_at=NULL WHERE seq IN ({placeholders})",  # noqa: S608
                        (RecordState.PENDING.value, *seqs),
                    )
                conn.commit()
                return len(seqs)
            except BaseException:
                conn.rollback()
                raise

        async with self._lock:
            return await asyncio.to_thread(_do)

    async def quarantine_raw(self, host: HostKind, raw: bytes, *, reason: str) -> None:
        """Dead-letter an un-parseable raw envelope, hash-indexed + retained (capture-spec.md
        §8.3) — this is LOCAL disk the user already owns, not a bus payload, so retaining the raw
        bytes (for a later manual/engineer replay) does not violate the bus's content-free rule."""
        conn = self._require_conn()
        raw_sha256 = hashlib.sha256(raw).hexdigest()

        def _do() -> None:
            conn.execute(
                "INSERT INTO dead_letter(host, reason, raw_sha256, raw, at) VALUES (?,?,?,?,?)",
                (host.value, reason, raw_sha256, raw, _now_iso()),
            )

        async with self._lock:
            await asyncio.to_thread(_do)

    async def load_checkpoint(self, source_id: str) -> CaptureCheckpoint | None:
        conn = self._require_conn()

        def _do() -> tuple[Any, ...] | None:
            row = conn.execute(
                "SELECT source_id, file_path, byte_offset, updated_at FROM checkpoints "
                "WHERE source_id=?",
                (source_id,),
            ).fetchone()
            return tuple(row) if row is not None else None

        async with self._lock:
            row = await asyncio.to_thread(_do)
        if row is None:
            return None
        return CaptureCheckpoint(
            source_id=row[0],
            file_path=row[1],
            byte_offset=row[2],
            updated_at=datetime.fromisoformat(row[3]),
        )

    async def undelivered_count(self) -> int:
        """``COUNT(state='dead')`` — the ONE ``undelivered_count`` source (capture-spec.md §8.3's
        ``undelivered`` view)."""
        return await self._count(RecordState.DEAD)

    async def outbox_depth(self) -> int:
        """``COUNT(state IN ('pending','inflight'))`` — daemon-app-skeleton-spec.md §5.2
        ``HealthzView.outbox_depth``."""
        conn = self._require_conn()

        def _do() -> int:
            row = conn.execute(
                "SELECT COUNT(*) FROM outbox WHERE state IN (?,?)",
                (RecordState.PENDING.value, RecordState.INFLIGHT.value),
            ).fetchone()
            return int(row[0])

        async with self._lock:
            return await asyncio.to_thread(_do)

    async def _count(self, state: RecordState) -> int:
        conn = self._require_conn()

        def _do() -> int:
            row = conn.execute(
                "SELECT COUNT(*) FROM outbox WHERE state=?", (state.value,)
            ).fetchone()
            return int(row[0])

        async with self._lock:
            return await asyncio.to_thread(_do)

    async def __aenter__(self) -> SqliteOutbox:
        await self.open()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()
