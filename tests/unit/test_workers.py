"""``InProcessLocalIngest`` + ``OutboxWorker`` — isolated logic, mocks permitted (DEV-STANDARDS:
mocks ONLY in pure unit tests). Real round-trips (real embedder, real stores) live in
``tests/integration/``."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from mu_client.capture.model import ActivityKind, HostKind, RawActivity
from mu_client.config import OutboxSettings
from mu_client.host import LocalMemoryHost
from mu_client.outbox.sqlite_outbox import SqliteOutbox
from mu_client.workers.ingest_client import ExtractionSkippedError, InProcessLocalIngest
from mu_client.workers.pool import OutboxWorker

pytestmark = pytest.mark.unit


def _activity(
    *, kind: ActivityKind = ActivityKind.USER_PROMPT, text: str | None = "hello", offset: str = "o1"
) -> RawActivity:
    return RawActivity(
        activity_id=f"act-{offset}",
        host=HostKind.CLAUDE_CODE,
        host_version="test",
        schema_version="claude_code.v1",
        kind=kind,
        session_id="s1",
        occurred_at=datetime.now(UTC),
        text=text,
        content_hash="deadbeef" if text else None,
        source_offset=offset,
        provenance_id="prov_x",
    )


@pytest.fixture
async def started_host(monkeypatch: pytest.MonkeyPatch) -> LocalMemoryHost:
    fake_memory = AsyncMock()
    fake_memory.add.return_value = AsyncMock(
        memory_id="m1", content_hash="deadbeef", promoted=True, tiers_written=["stm"]
    )
    monkeypatch.setattr("mu_client.host.LocalMemory", lambda *a, **kw: fake_memory)
    host = LocalMemoryHost()
    await host.start()
    return host


async def test_ingest_control_kind_raises_extraction_skipped(started_host: LocalMemoryHost) -> None:
    ingest = InProcessLocalIngest(started_host, user="u1")
    with pytest.raises(ExtractionSkippedError):
        await ingest.ingest(_activity(kind=ActivityKind.SESSION_META, text=None))


async def test_ingest_none_text_raises_extraction_skipped_even_for_a_real_kind(
    started_host: LocalMemoryHost,
) -> None:
    """A dropped slash-command: kind=USER_PROMPT but text=None (capture/parsers.py's kind gate)."""
    ingest = InProcessLocalIngest(started_host, user="u1")
    with pytest.raises(ExtractionSkippedError):
        await ingest.ingest(_activity(kind=ActivityKind.USER_PROMPT, text=None))


async def test_ingest_real_activity_calls_host_add(started_host: LocalMemoryHost) -> None:
    ingest = InProcessLocalIngest(started_host, user="u1")
    await ingest.ingest(_activity(text="Ada lives in Paris"))
    started_host._memory.add.assert_awaited_once()  # type: ignore[union-attr]
    _, kwargs = started_host._memory.add.await_args  # type: ignore[union-attr]
    assert kwargs["session"] == "s1"


async def test_worker_run_once_acks_control_kind_without_calling_ingest(
    tmp_path: Path, started_host: LocalMemoryHost
) -> None:
    outbox = SqliteOutbox(tmp_path / "outbox.sqlite")
    await outbox.open()
    await outbox.append(_activity(kind=ActivityKind.SESSION_END, text=None, offset="ctrl"))
    ingest = InProcessLocalIngest(started_host, user="u1")
    worker = OutboxWorker(
        outbox, ingest, settings=OutboxSettings(), org="org", workspace="ws", user="u1"
    )

    tick = await worker.run_once()

    assert tick.drained == 1
    assert tick.skipped == 1
    assert tick.acked == 0
    assert await outbox.outbox_depth() == 0  # ack'd, not stuck pending
    started_host._memory.add.assert_not_awaited()  # type: ignore[union-attr]
    await outbox.aclose()


async def test_worker_run_once_ingests_and_acks_real_activity(
    tmp_path: Path, started_host: LocalMemoryHost
) -> None:
    outbox = SqliteOutbox(tmp_path / "outbox.sqlite")
    await outbox.open()
    await outbox.append(_activity(text="Ada lives in Paris", offset="real1"))
    ingest = InProcessLocalIngest(started_host, user="u1")
    worker = OutboxWorker(
        outbox, ingest, settings=OutboxSettings(), org="org", workspace="ws", user="u1"
    )

    tick = await worker.run_once()

    assert tick.acked == 1
    assert tick.skipped == 0
    started_host._memory.add.assert_awaited_once()  # type: ignore[union-attr]
    assert await outbox.outbox_depth() == 0
    await outbox.aclose()


async def test_worker_emits_memory_captured_event_with_the_real_ingest_user_not_session_id(
    tmp_path: Path, started_host: LocalMemoryHost, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Data-review ISSUE fix: the emitted ``MemoryCaptured`` event's ``Namespace.user`` must match
    the user the ingest actually stored under (``InProcessLocalIngest(..., user=...)``), never
    ``record.activity.session_id`` — a different η slot entirely. Activity ``session_id`` is
    deliberately "s1" here (see ``_activity``) while the real ingest/worker user is "realuser99" —
    two distinct values, so a regression that reintroduces the session_id/user mix-up fails loud."""
    captured: dict[str, object] = {}

    def _spy_log_memory_captured(*, namespace: object, ids: list[str]) -> None:
        captured["namespace"] = namespace
        captured["ids"] = ids

    monkeypatch.setattr("mu_client.workers.pool.log_memory_captured", _spy_log_memory_captured)

    outbox = SqliteOutbox(tmp_path / "outbox.sqlite")
    await outbox.open()
    await outbox.append(_activity(text="Ada lives in Paris", offset="real2"))
    ingest = InProcessLocalIngest(started_host, user="realuser99")
    worker = OutboxWorker(
        outbox,
        ingest,
        settings=OutboxSettings(),
        org="org",
        workspace="ws",
        user="realuser99",
    )

    tick = await worker.run_once()

    assert tick.acked == 1
    namespace = captured["namespace"]
    assert namespace.user == "realuser99"  # type: ignore[attr-defined]
    assert namespace.session == "s1"  # type: ignore[attr-defined]  # activity.session_id, unchanged
    await outbox.aclose()
