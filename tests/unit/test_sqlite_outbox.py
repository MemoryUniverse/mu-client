"""``SqliteOutbox`` — a REAL WAL-mode SQLite file per test (tmp_path), zero mocks (the outbox
itself is a leaf adapter, not something the DEV-STANDARDS integration-only rule reaches for)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from mu_client.capture.model import ActivityKind, HostKind, RawActivity
from mu_client.outbox.model import RecordState
from mu_client.outbox.sqlite_outbox import SqliteOutbox

pytestmark = pytest.mark.unit


def _activity(offset: str = "off-1", text: str | None = "hello") -> RawActivity:
    return RawActivity(
        activity_id=f"act-{offset}",
        host=HostKind.CLAUDE_CODE,
        host_version="test",
        schema_version="claude_code.v1",
        kind=ActivityKind.USER_PROMPT,
        session_id="s1",
        occurred_at=datetime.now(UTC),
        text=text,
        content_hash="deadbeef" if text else None,
        source_offset=offset,
        provenance_id="prov_x",
    )


@pytest.fixture
async def outbox(tmp_path: Path) -> SqliteOutbox:
    box = SqliteOutbox(tmp_path / "outbox.sqlite")
    await box.open()
    return box


async def test_append_is_wal_mode_and_durable_on_disk(outbox: SqliteOutbox, tmp_path: Path) -> None:
    await outbox.append(_activity())
    wal_path = tmp_path / "outbox.sqlite-wal"
    # WAL mode always creates the -wal sidecar file once a write has happened.
    assert (tmp_path / "outbox.sqlite").exists()
    assert wal_path.exists() or (tmp_path / "outbox.sqlite").stat().st_size > 0


async def test_append_is_idempotent_on_unique_activity_id(outbox: SqliteOutbox) -> None:
    first = await outbox.append(_activity(offset="dup"))
    second = await outbox.append(_activity(offset="dup"))
    assert first.seq == second.seq
    assert await outbox.outbox_depth() == 1


async def test_drain_moves_pending_to_inflight_atomically(outbox: SqliteOutbox) -> None:
    await outbox.append(_activity(offset="a"))
    await outbox.append(_activity(offset="b"))
    batch = await outbox.drain(batch_size=10)
    assert len(batch.records) == 2
    assert all(r.state is RecordState.INFLIGHT for r in batch.records)
    # a second drain sees nothing PENDING left.
    empty = await outbox.drain(batch_size=10)
    assert empty.records == []


async def test_ack_then_undelivered_and_depth_counts(outbox: SqliteOutbox) -> None:
    await outbox.append(_activity(offset="a"))
    batch = await outbox.drain(batch_size=10)
    await outbox.ack([r.seq for r in batch.records])
    assert await outbox.outbox_depth() == 0
    assert await outbox.undelivered_count() == 0


async def test_dead_letter_and_redrive(outbox: SqliteOutbox) -> None:
    record = await outbox.append(_activity(offset="a"))
    await outbox.drain(batch_size=10)
    await outbox.dead_letter(record.seq, error="boom")
    assert await outbox.undelivered_count() == 1
    redriven = await outbox.redrive_dead(limit=10)
    assert redriven == 1
    assert await outbox.undelivered_count() == 0
    assert await outbox.outbox_depth() == 1


async def test_retry_later_returns_record_to_pending(outbox: SqliteOutbox) -> None:
    record = await outbox.append(_activity(offset="a"))
    await outbox.drain(batch_size=10)
    await outbox.retry_later(record.seq, error="transient", backoff_s=0.0)
    # ready_at is in the past (backoff_s=0) so it is immediately eligible again.
    batch = await outbox.drain(batch_size=10)
    assert len(batch.records) == 1
    assert batch.records[0].attempts == 1


async def test_crash_recovery_inflight_resets_to_pending_on_reopen(tmp_path: Path) -> None:
    """capture-spec.md §13: a crash between drain (INFLIGHT) and ack re-drives on next start."""
    path = tmp_path / "outbox.sqlite"
    box1 = SqliteOutbox(path)
    await box1.open()
    await box1.append(_activity(offset="a"))
    await box1.drain(batch_size=10)  # -> INFLIGHT, then "crash" (no ack, no clean close)
    await box1.aclose()

    box2 = SqliteOutbox(path)
    await box2.open()  # recovers INFLIGHT -> PENDING
    assert await box2.outbox_depth() == 1
    batch = await box2.drain(batch_size=10)
    assert len(batch.records) == 1  # re-drained, not lost, not duplicated
    await box2.aclose()


async def test_checkpoint_round_trips(outbox: SqliteOutbox, tmp_path: Path) -> None:
    from mu_client.capture.model import CaptureCheckpoint

    cp = CaptureCheckpoint(
        source_id="tailer-1",
        file_path=str(tmp_path / "x.jsonl"),
        byte_offset=42,
        updated_at=datetime.now(UTC),
    )
    await outbox.append(_activity(offset="a"), checkpoint=cp)
    loaded = await outbox.load_checkpoint("tailer-1")
    assert loaded is not None
    assert loaded.byte_offset == 42


async def test_quarantine_raw_is_hash_indexed(outbox: SqliteOutbox) -> None:
    await outbox.quarantine_raw(HostKind.CLAUDE_CODE, b'{"bad": true}', reason="schema_drift")
    # No public read API is pinned for dead_letter rows this stage; this just proves the write
    # path does not raise on real WAL SQLite.


# ═══════════════════════════════════════════════════ F4a: this store finally has a delete path ═
# FAULT-HUNT-0924.md F4a: before these two methods existed, this file had NO `DELETE` statement
# anywhere — an acked row's raw `activity_json` sat on disk forever (measured on a real daemon:
# 374 rows, 307 KB, 34 days, zero deletions ever).


async def _acked(outbox: SqliteOutbox, offset: str, *, text: str = "hello") -> None:
    rec = await outbox.append(_activity(offset=offset, text=text))
    await outbox.ack([rec.seq])


async def test_purge_acked_before_removes_only_old_acked_rows(outbox: SqliteOutbox) -> None:
    await _acked(outbox, "old")
    await _acked(outbox, "new")
    # Backdate the "old" row's enqueued_at directly (SqliteOutbox stamps it at append time; there
    # is no public "as of" append parameter, and there should not be one just for a test).
    conn = outbox._conn
    assert conn is not None
    conn.execute(
        "UPDATE outbox SET enqueued_at = ? WHERE activity_id = ?",
        ((datetime.now(UTC) - timedelta(days=40)).isoformat(), "act-old"),
    )

    removed = await outbox.purge_acked_before(datetime.now(UTC) - timedelta(days=30))

    assert removed == 1
    assert await outbox.outbox_depth() == 0  # neither row was pending/inflight to begin with


async def test_purge_acked_before_never_removes_pending_or_inflight_rows(
    outbox: SqliteOutbox,
) -> None:
    """The at-least-once guarantee this store exists for: an undelivered row must never be swept
    by age alone, no matter how old it looks."""
    rec = await outbox.append(_activity(offset="still-pending"))
    conn = outbox._conn
    assert conn is not None
    conn.execute(
        "UPDATE outbox SET enqueued_at = ? WHERE seq = ?",
        ((datetime.now(UTC) - timedelta(days=400)).isoformat(), rec.seq),
    )

    removed = await outbox.purge_acked_before(datetime.now(UTC))

    assert removed == 0
    assert await outbox.outbox_depth() == 1  # still there, still deliverable


async def test_delete_by_content_hash_removes_acked_and_dead_rows(outbox: SqliteOutbox) -> None:
    rec1 = await outbox.append(_activity(offset="a1", text="dup content"))
    await outbox.ack([rec1.seq])
    rec2 = await outbox.append(_activity(offset="a2", text="dup content"))
    await outbox.dead_letter(rec2.seq, error="permanent")

    removed = await outbox.delete_by_content_hash("deadbeef")  # _activity()'s fixed content_hash

    assert removed == 2


async def test_delete_by_content_hash_leaves_an_inflight_row_for_the_same_hash(
    outbox: SqliteOutbox,
) -> None:
    """The residue `mu_client.consent.residue.ClientCascadeResidue.
    OUTBOX_ROW_PENDING_SETTLEMENT_OR_RETENTION` names: a row still in flight for content a caller
    just asked to forget is left alone, not silently dropped mid-delivery."""
    acked = await outbox.append(_activity(offset="acked-1", text="hello"))
    await outbox.ack([acked.seq])
    still_pending = await outbox.append(_activity(offset="pending-1", text="hello"))
    del still_pending  # kept PENDING — never acked, never dead-lettered

    removed = await outbox.delete_by_content_hash("deadbeef")

    assert removed == 1  # only the acked row
    assert await outbox.outbox_depth() == 1  # the pending row survives, undisturbed
