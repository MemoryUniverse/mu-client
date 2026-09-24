"""``SqliteOutbox.redrive_dead`` — REAL flush into mu-local's real ``mu-dev-*`` stores, ZERO
mocks (DEV-STANDARDS non-negotiable). AD-270a fix (ADR 0075, PROTOTYPE-DEBT-0924.md B3): the
method was built, tested, and had NO CALLER — a dead-lettered capture was unrecoverable. The unit
tier (``tests/unit/test_outbox_redrive_route_unit.py``) proves the IPC route/store logic; THIS
file proves the task's own "prove it" instruction — dead-letter a REAL capture, redrive it, and
confirm it actually lands in the store (recallable), not merely that its row's ``state`` column
changed.

If a container is not up, this RAISES (BLOCKED, never faked).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
import pytest_asyncio
from falkordb.asyncio import FalkorDB
from mu_contracts.contracts.recall import RecallItemView
from qdrant_client import AsyncQdrantClient
from redis.asyncio import Redis

from mu_client.capture.model import ActivityKind, HostKind, RawActivity
from mu_client.config import ClientSettings, OutboxSettings
from mu_client.host import LocalMemoryHost, daemonless_host
from mu_client.outbox.sqlite_outbox import SqliteOutbox
from mu_client.workers.ingest_client import InProcessLocalIngest
from mu_client.workers.pool import OutboxWorker

pytestmark = pytest.mark.integration

_SESSION = "redrive-s1"


async def _teardown(settings: ClientSettings, uid: str) -> None:
    """Same isolated-η cleanup ``test_capture_daemon_int.py`` uses (duplicated, not shared — that
    file's own comment: "no shared test-util package exists yet across the two integration
    modules")."""
    qdrant = AsyncQdrantClient(url=settings.storage.vector.url)
    try:
        for coll in (await qdrant.get_collections()).collections:
            if uid in coll.name:
                with contextlib.suppress(Exception):
                    await qdrant.delete_collection(coll.name)
    finally:
        await qdrant.close()

    db = FalkorDB(host=settings.storage.graph.host, port=settings.storage.graph.port)
    try:
        for g in await db.list_graphs():
            name = g.decode() if isinstance(g, bytes) else g
            if uid in name:
                with contextlib.suppress(Exception):
                    await db.select_graph(name).delete()
    finally:
        with contextlib.suppress(Exception):
            await db.connection.aclose()

    redis: Redis = Redis.from_url(settings.storage.cache.url, decode_responses=False)
    try:
        keys = [k async for k in redis.scan_iter(match=f"*{uid}*".encode())]
        if keys:
            await redis.delete(*keys)
    finally:
        await redis.aclose()


@pytest_asyncio.fixture
async def isolated_settings(
    client_settings: ClientSettings, uid: str, tmp_path: Path
) -> AsyncIterator[ClientSettings]:
    settings = client_settings.model_copy(
        update={
            "default_workspace": f"ws{uid}",
            "default_namespace": f"org{uid}",
            "outbox": OutboxSettings(outbox_path=tmp_path / "outbox.sqlite"),
        }
    )
    try:
        yield settings
    finally:
        await _teardown(settings, uid)


async def _eventually_recall(
    host: LocalMemoryHost, query: str, *, session: str = _SESSION
) -> list[RecallItemView]:
    for _ in range(40):
        listing = await host.recall(query, user="default", session=session)
        if listing.items:
            return list(listing.items)
        await asyncio.sleep(0.2)
    return []


async def test_a_dead_lettered_capture_is_recoverable_via_redrive(
    isolated_settings: ClientSettings,
) -> None:
    """The full "prove it" scenario: append a real capture -> drain it once (delivery attempt,
    simulating one failed try) -> dead-letter it (retries exhausted) -> ``redrive_dead`` (the
    fix's own caller) -> drain AGAIN -> the content is now recallable from the REAL stores.
    Before this fix there was no step between "dead-lettered" and "gone forever" — the same
    real, long-lived, never-cleaned outbox table FAULT-HUNT-0924 F4a measured (374 ``acked`` rows
    / 307 KB / 34 days, zero rows ever removed) had no redrive path for a ``dead`` row either.

    **MUTATION:** skip the ``redrive_dead`` call -> the final ``_eventually_recall`` returns
    ``[]`` and the test goes RED, proving the redrive step — not the drain alone — is what
    recovers the memory."""
    outbox = SqliteOutbox(isolated_settings.outbox.outbox_path)
    await outbox.open()
    try:
        # (1) A real capture lands in the outbox, PENDING.
        activity = RawActivity(
            activity_id="redrive-act-1",
            host=HostKind.CLAUDE_CODE,
            host_version="test",
            schema_version="claude_code.v1",
            kind=ActivityKind.USER_PROMPT,
            session_id=_SESSION,
            occurred_at=datetime.now(UTC),
            text="Ada's favourite city is Vienna.",
            content_hash="redrive-hash-1",
            source_offset="off-1",
            provenance_id="prov_redrive",
        )
        record = await outbox.append(activity)
        assert await outbox.outbox_depth() == 1

        # (2) A delivery attempt is made and fails; the retry budget is treated as exhausted —
        #     DEAD, content still on disk, no path back before this fix.
        drained = await outbox.drain(batch_size=10)
        assert len(drained.records) == 1
        await outbox.dead_letter(record.seq, error="simulated permanent parser failure")
        assert await outbox.undelivered_count() == 1
        assert await outbox.outbox_depth() == 0, "a DEAD row must not count toward live depth"

        # (3) THE FIX'S OWN CALLER: redrive it back to PENDING.
        redriven = await outbox.redrive_dead(limit=10)
        assert redriven == 1
        assert await outbox.undelivered_count() == 0
        assert await outbox.outbox_depth() == 1

        # (4) The normal delivery path picks the redriven row up exactly like any fresh capture —
        #     no second mechanism — and it lands in the REAL mu-dev-* stores.
        async with daemonless_host(isolated_settings) as host:
            ingest = InProcessLocalIngest(host, user="default")
            worker = OutboxWorker(
                outbox,
                ingest,
                settings=isolated_settings.outbox,
                org=isolated_settings.default_namespace,
                workspace=isolated_settings.default_workspace,
                user="default",
            )
            tick = await worker.run_once()
            assert tick.acked == 1

        async with daemonless_host(isolated_settings) as host:
            recalled = await _eventually_recall(host, "Where is Ada's favourite city?")
            print(  # noqa: T201
                "REDRIVEN + RECALLED "
                + "; ".join(f"{it.memory_id}|{it.content}" for it in recalled)
            )
            assert any(
                "Vienna" in it.content for it in recalled
            ), "a redriven capture must actually reach the store, not merely change state"
    finally:
        await outbox.aclose()


async def test_redrive_on_a_fresh_outbox_with_no_dead_rows_is_a_true_no_op(
    isolated_settings: ClientSettings,
) -> None:
    outbox = SqliteOutbox(isolated_settings.outbox.outbox_path)
    await outbox.open()
    try:
        assert await outbox.redrive_dead(limit=10) == 0
    finally:
        await outbox.aclose()
