"""Phase 0B ACCEPTANCE — REAL stores, ZERO mocks (DEV-STANDARDS: non-negotiable).

Feeds a REAL-shape Claude Code 2.1.226 session transcript JSONL (a thinking block carrying a
decision, a pure-noise thinking block, a mid-turn intermediate message, and a final text-only
answer) through the ACTUAL Phase 0B spine —
``backfill_thinking`` → SqliteOutbox → InProcessLocalIngest → OutboxWorker → mu-local over the real
mu-dev-cache (valkey/STM) + mu-dev-qdrant (MTM) — then proves, by DIRECT store reads:

1. the DECISION lands attributed as thinking (``[thinking] ...staging-eu-west...``) — read straight
   out of valkey (the STM blob) AND surfaced through the federated ``recall`` from real stores;
2. it PROMOTED STM→MTM (a real qdrant point exists) because its importance (0.7) cleared the
   engine's own ``importance_promote`` gate (0.6) — the SAME gate, not a second one;
3. pure-noise reasoning lines did NOT each become a memory (exactly the 2 decisions were captured
   from a transcript whose thinking is mostly boilerplate);
4. the turn's FINAL answer (text-only record) was NOT captured by the tailer (Stop-hook territory).

If a container is down the test RAISES (BLOCKED, never faked)."""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio
from falkordb.asyncio import FalkorDB
from qdrant_client import AsyncQdrantClient
from redis.asyncio import Redis

from mu_client.capture.claude_tailer import backfill_thinking
from mu_client.config import ClientSettings
from mu_client.host import daemonless_host
from mu_client.outbox.sqlite_outbox import SqliteOutbox
from mu_client.workers.ingest_client import InProcessLocalIngest
from mu_client.workers.pool import OutboxWorker

pytestmark = pytest.mark.integration

_USER = "u1"
_SESSION = "sess-p0b"


def _assistant(*, uuid: str, blocks: list[dict[str, object]]) -> dict[str, object]:
    """A REAL Claude Code 2.1.226 assistant transcript record shape (verified on-disk)."""
    return {
        "type": "assistant",
        "uuid": uuid,
        "parentUuid": "p-" + uuid,
        "isSidechain": False,
        "sessionId": _SESSION,
        "timestamp": "2026-08-09T23:45:30.230Z",
        "cwd": "/home/user/project",
        "version": "2.1.226",
        "gitBranch": "main",
        "requestId": "req-" + uuid,
        "message": {"role": "assistant", "content": blocks},
    }


def _write_fixture(path: Path) -> None:
    records = [
        # (1) thinking block: ONE decision buried in boilerplate.
        _assistant(
            uuid="u1",
            blocks=[
                {
                    "type": "thinking",
                    "thinking": (
                        "The prod cluster is frozen this week. "
                        "I've decided to use staging-eu-west because it has spare capacity. "
                        "Let me open the pipeline config. Now let me check the credentials."
                    ),
                    "signature": "sig-xyz",
                },
                {"type": "tool_use", "id": "t1", "name": "Read", "input": {}},
            ],
        ),
        # (2) pure-noise thinking block — must yield ZERO memories.
        _assistant(
            uuid="u2",
            blocks=[
                {
                    "type": "thinking",
                    "thinking": (
                        "Let me read the file. Okay. Now let me look at the config. "
                        "First I need to understand the structure. Let me check the tests."
                    ),
                    "signature": "sig-noise",
                },
                {"type": "tool_use", "id": "t2", "name": "Bash", "input": {}},
            ],
        ),
        # (3) mid-turn intermediate message (text + tool_use) — ONE decision.
        _assistant(
            uuid="u3",
            blocks=[
                {"type": "text", "text": "The root cause is a stale lease TTL; patching it now."},
                {"type": "tool_use", "id": "t3", "name": "Edit", "input": {}},
            ],
        ),
        # (4) final answer (text-only) — Stop hook owns it; tailer must NOT capture it.
        _assistant(
            uuid="u4",
            blocks=[{"type": "text", "text": "I've deployed to staging-eu-west. All green."}],
        ),
    ]
    path.write_text("".join(json.dumps(r) + "\n" for r in records), encoding="utf-8")


@pytest_asyncio.fixture
async def settings(
    client_settings: ClientSettings, uid: str, tmp_path: Path
) -> AsyncIterator[ClientSettings]:
    """Isolated η partition + a throwaway outbox + thinking-backfill ENABLED."""
    s = client_settings.model_copy(
        update={
            "default_workspace": f"ws{uid}",
            "default_namespace": f"org{uid}",
            "capture": client_settings.capture.model_copy(
                update={"thinking_backfill_enabled": True}
            ),
            "outbox": client_settings.outbox.model_copy(
                update={"outbox_path": tmp_path / "outbox.sqlite"}
            ),
        }
    )
    try:
        yield s
    finally:
        await _teardown(s, uid)


async def _teardown(settings: ClientSettings, uid: str) -> None:
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


async def _drain(settings: ClientSettings) -> int:
    """Drive the outbox through the SAME worker the daemonless ``mu flush`` uses — into real
    stores. Returns the acked count."""
    outbox = SqliteOutbox(settings.outbox.outbox_path)
    await outbox.open()
    try:
        async with daemonless_host(settings) as host:
            ingest = InProcessLocalIngest(host, user=settings.default_user)
            worker = OutboxWorker(
                outbox,
                ingest,
                settings=settings.outbox,
                org=settings.default_namespace,
                workspace=settings.default_workspace,
                user=settings.default_user,
            )
            acked = 0
            while True:
                tick = await worker.run_once()
                if tick.drained == 0:
                    break
                acked += tick.acked
            return acked
    finally:
        await outbox.aclose()


async def _stm_contents(settings: ClientSettings) -> list[str]:
    """DIRECT valkey (STM) read: pull every STM memory blob in this η and decode its content."""
    redis: Redis = Redis.from_url(settings.storage.cache.url, decode_responses=False)
    contents: list[str] = []
    try:
        async for key in redis.scan_iter(match=b"*:stm:mem:*"):
            blob = await redis.get(key)
            if blob is None:
                continue
            item = json.loads(blob)
            ns = str(item.get("namespace", {}))
            if settings.default_namespace in ns or settings.default_workspace in ns:
                contents.append(str(item.get("content", "")))
    finally:
        await redis.aclose()
    return contents


async def _mtm_point_count(settings: ClientSettings, uid: str) -> int:
    """DIRECT qdrant (MTM) read: count points across this run's promoted collections."""
    qdrant = AsyncQdrantClient(url=settings.storage.vector.url)
    total = 0
    try:
        for coll in (await qdrant.get_collections()).collections:
            if uid in coll.name:
                total += (await qdrant.count(coll.name, exact=True)).count
    finally:
        await qdrant.close()
    return total


async def test_thinking_decision_lands_attributed_and_noise_filtered(
    settings: ClientSettings, uid: str, tmp_path: Path
) -> None:
    transcript = tmp_path / f"{_SESSION}.jsonl"
    _write_fixture(transcript)

    # (1) BACKFILL — tail the real-shape transcript into the durable outbox.
    result = await backfill_thinking(settings, transcript_path=transcript, session_id=_SESSION)
    print(  # noqa: T201 — required evidence
        f"BACKFILL appended={result.appended} records_scanned={result.records_scanned} "
        f"thinking_seen={result.thinking_blocks_seen} plaintext={result.thinking_blocks_plaintext}"
    )
    # Two decisions extracted (thinking block #1 + intermediate msg #3); noise block #2 and the
    # final answer #4 contributed ZERO — pure-noise lines did NOT each become a memory.
    assert result.appended == 2, "expected exactly the 2 decisions, not one-per-noise-line"

    # (2) DRAIN into the REAL stores through the actual ingest spine.
    acked = await _drain(settings)
    print(f"DRAINED acked={acked}")  # noqa: T201
    assert acked == 2

    # (3) DIRECT valkey (STM) read — the decision physically in the store, attributed as thinking.
    stm = await _stm_contents(settings)
    print("STM CONTENTS " + " || ".join(stm))  # noqa: T201
    thinking_rows = [c for c in stm if c.startswith("[thinking] ")]
    assert thinking_rows, "no [thinking]-attributed memory found in STM"
    assert any(
        "staging-eu-west" in c for c in thinking_rows
    ), "the decision text did not land attributed as thinking in the real STM store"
    assert not any(
        "Let me read the file" in c or "Now let me look" in c for c in stm
    ), "a pure-noise reasoning line leaked into the store"
    assert not any(
        "All green" in c for c in stm
    ), "the FINAL answer was captured by the tailer (must be Stop-hook territory only)"

    # (4) DIRECT qdrant (MTM) read — the decision PROMOTED (importance 0.7 ≥ gate 0.6).
    mtm = await _mtm_point_count(settings, uid)
    print(f"MTM point_count={mtm}")  # noqa: T201
    assert mtm >= 1, "the decision did not promote STM->MTM (importance gate not honored)"

    # (5) FEDERATED recall from the real stores — the public read a caller actually gets. The
    #     worker ingests under settings.default_user (the daemonless flush partition), so recall
    #     under THAT user — a different η.user would be a different partition (real product
    #     behaviour). Qdrant upserts are eventually consistent (same guard the daemonless roundtrip
    #     test uses), so poll a bounded number of times before failing.
    async with daemonless_host(settings) as host:
        recalled = None
        for _ in range(40):  # ~8s ceiling
            recalled = await host.recall(
                "where did we decide to deploy",
                user=settings.default_user,
                session=_SESSION,
                limit=10,
            )
            if any("[thinking]" in it.content for it in recalled.items):
                break
            await asyncio.sleep(0.2)
    assert recalled is not None
    print(  # noqa: T201
        "RECALLED " + "; ".join(f"{it.tier}/{it.channel}|{it.content}" for it in recalled.items)
    )
    assert any(
        "[thinking]" in it.content and "staging-eu-west" in it.content for it in recalled.items
    ), "the thinking-attributed decision did not round-trip through federated recall"
