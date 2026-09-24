"""``OutboxRetentionLoop`` — the policy half of FAULT-HUNT-0924.md F4a (see that module's own
docstring). A REAL WAL-mode ``SqliteOutbox`` file (``tmp_path``), zero mocks, same discipline as
``test_sqlite_outbox.py``; only the clock is a double, so "days old" is controllable without a
real sleep.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from mu_client.capture.model import ActivityKind, HostKind, RawActivity
from mu_client.config import OutboxSettings
from mu_client.outbox.retention import OutboxRetentionLoop
from mu_client.outbox.sqlite_outbox import SqliteOutbox

pytestmark = pytest.mark.unit


class _FrozenClock:
    def __init__(self, now: datetime) -> None:
        self._now = now

    def now(self) -> datetime:
        return self._now


def _activity(offset: str) -> RawActivity:
    return RawActivity(
        activity_id=f"act-{offset}",
        host=HostKind.CLAUDE_CODE,
        host_version="test",
        schema_version="claude_code.v1",
        kind=ActivityKind.USER_PROMPT,
        session_id="s1",
        occurred_at=datetime.now(UTC),
        text="hello",
        content_hash="deadbeef",
        source_offset=offset,
        provenance_id="prov_x",
    )


@pytest.fixture
async def outbox(tmp_path: Path) -> SqliteOutbox:
    box = SqliteOutbox(tmp_path / "outbox.sqlite")
    await box.open()
    return box


async def test_sweep_once_purges_rows_past_the_configured_retention_window(
    outbox: SqliteOutbox,
) -> None:
    rec = await outbox.append(_activity("old"))
    await outbox.ack([rec.seq])
    conn = outbox._conn
    assert conn is not None
    conn.execute(
        "UPDATE outbox SET enqueued_at = ? WHERE seq = ?",
        ((datetime.now(UTC) - timedelta(days=40)).isoformat(), rec.seq),
    )
    loop = OutboxRetentionLoop(
        outbox,
        settings=OutboxSettings(acked_retention_days=30),
        clock=_FrozenClock(datetime.now(UTC)),
    )

    purged = await loop.sweep_once()

    assert purged == 1
    assert loop.sweep_count == 1
    assert loop.rows_purged_total == 1


async def test_sweep_once_leaves_a_row_inside_the_retention_window(outbox: SqliteOutbox) -> None:
    rec = await outbox.append(_activity("recent"))
    await outbox.ack([rec.seq])  # enqueued_at defaults to "now" — well inside 30 days
    loop = OutboxRetentionLoop(
        outbox,
        settings=OutboxSettings(acked_retention_days=30),
        clock=_FrozenClock(datetime.now(UTC)),
    )

    purged = await loop.sweep_once()

    assert purged == 0
    assert await outbox.outbox_depth() == 0  # not pending either — just correctly retained


async def test_stop_makes_run_return_promptly(outbox: SqliteOutbox) -> None:
    """NON-VACUITY CONTROL on the loop shape itself: a loop that never checks its stop event
    would hang this test. `retention_sweep_interval_s` is set high on purpose — `stop()` must be
    what ends `run()`, not the interval elapsing."""
    import asyncio

    loop = OutboxRetentionLoop(
        outbox,
        settings=OutboxSettings(retention_sweep_interval_s=3600.0),
    )
    task = asyncio.create_task(loop.run())
    await asyncio.sleep(0)  # let the first immediate tick complete
    await loop.stop()
    await asyncio.wait_for(task, timeout=5.0)
    assert loop.sweep_count >= 1
