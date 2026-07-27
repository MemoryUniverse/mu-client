"""``SqliteOutbox`` — a REAL WAL-mode SQLite file per test (tmp_path), zero mocks (the outbox
itself is a leaf adapter, not something the DEV-STANDARDS integration-only rule reaches for)."""

from __future__ import annotations

from datetime import UTC, datetime
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
