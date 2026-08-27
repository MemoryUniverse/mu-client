"""Phase 4 ACCEPTANCE — REAL stores, ZERO mocks (DEV-STANDARDS: non-negotiable).

Feeds codex rollout JSONL through the ACTUAL Phase-4 spine —
``backfill_codex`` → SqliteOutbox → InProcessLocalIngest → OutboxWorker → mu-local over the real
mu-dev-cache (valkey/STM) + mu-dev-qdrant (MTM) + mu-dev-graph — then proves, by DIRECT store reads
and a federated recall, that a codex turn's fact LANDS and is recallable. Two rollouts are used:

1. a REAL-SHAPE rollout (verified codex 0.146.0 line shapes) carrying a DISTINCTIVE fact + a tool
   call — proves user-prompt/assistant/tool capture lands in the real store and round-trips recall;
2. the GENUINELY-CAPTURED real rollout file (``tests/fixtures/codex_rollout_real.jsonl``, written by
   a live ``codex exec`` run) — proves a real on-disk rollout flows end-to-end unchanged.

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

from mu_client.capture.codex import backfill_codex
from mu_client.config import ClientSettings
from mu_client.host import daemonless_host
from mu_client.outbox.sqlite_outbox import SqliteOutbox
from mu_client.workers.ingest_client import InProcessLocalIngest
from mu_client.workers.pool import OutboxWorker

pytestmark = pytest.mark.integration

_SESSION = "019fe954-5a48-7992-885e-ede757dbd3eb"
_REAL_FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "codex_rollout_real.jsonl"


def _session_meta() -> dict[str, object]:
    return {
        "timestamp": "2026-08-10T04:40:47.000Z",
        "type": "session_meta",
        "payload": {
            "session_id": _SESSION,
            "id": _SESSION,
            "cwd": "/home/user/project",
            "originator": "codex_exec",
            "cli_version": "0.146.0",
            "source": "exec",
        },
    }


def _event(ptype: str, **fields: object) -> dict[str, object]:
    return {
        "timestamp": "2026-08-10T04:40:51.000Z",
        "type": "event_msg",
        "payload": {"type": ptype, **fields},
    }


def _resp(payload: dict[str, object]) -> dict[str, object]:
    return {"timestamp": "2026-08-10T04:40:52.000Z", "type": "response_item", "payload": payload}


def _write_realshape_rollout(path: Path) -> None:
    """A REAL codex-0.146.0 rollout shape carrying a distinctive fact + a tool call."""
    records = [
        _session_meta(),
        _event("task_started"),  # control — skipped
        _event("user_message", message="my codex deploy target is staging-eu-west-42", images=[]),
        _resp({"type": "reasoning", "id": "rs_1", "summary": [], "encrypted_content": "gAAAA…"}),
        _resp(
            {
                "type": "custom_tool_call",
                "id": "ctc_1",
                "status": "completed",
                "call_id": "call_1",
                "name": "exec",
                "input": 'const r = await tools.exec_command({"cmd":"cat pipeline.yaml"});',
            }
        ),
        _event(
            "agent_message",
            message="Got it — I'll deploy to staging-eu-west-42 as the codex target.",
            phase="final_answer",
            memory_citation=None,
        ),
        _event("token_count"),  # control — skipped
        _event("task_complete"),  # control — skipped
    ]
    path.write_text("".join(json.dumps(r) + "\n" for r in records), encoding="utf-8")


@pytest_asyncio.fixture
async def settings(
    client_settings: ClientSettings, uid: str, tmp_path: Path
) -> AsyncIterator[ClientSettings]:
    s = client_settings.model_copy(
        update={
            "default_workspace": f"ws{uid}",
            "default_namespace": f"org{uid}",
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


async def test_realshape_codex_rollout_lands_and_is_recallable(
    settings: ClientSettings, uid: str, tmp_path: Path
) -> None:
    rollout = tmp_path / f"rollout-2026-08-10T04-40-47-{_SESSION}.jsonl"
    _write_realshape_rollout(rollout)

    # (1) BACKFILL — tail the real-shape rollout into the durable outbox.
    result = await backfill_codex(settings, rollout_path=rollout)
    print(  # noqa: T201 — required evidence
        f"BACKFILL appended={result.appended} records_scanned={result.records_scanned} "
        f"session_id={result.session_id}"
    )
    # user_message + custom_tool_call + agent_message = 3 captures; reasoning/token/task_* skipped.
    assert result.appended == 3, "expected exactly user + tool + assistant captures"
    assert result.session_id == _SESSION

    # (2) DRAIN into the REAL stores through the actual ingest spine.
    acked = await _drain(settings)
    print(f"DRAINED acked={acked}")  # noqa: T201
    assert acked == 3

    # (3) DIRECT valkey (STM) read — the codex fact physically in the store.
    stm = await _stm_contents(settings)
    print("STM CONTENTS " + " || ".join(stm))  # noqa: T201
    assert any("staging-eu-west-42" in c for c in stm), "codex fact not found in real STM store"
    assert any(c.startswith("exec:") for c in stm), "codex tool call not captured into the store"

    # (4) FEDERATED recall from the real stores (STM floor surfaces the just-added fact). Qdrant/
    #     recall is eventually consistent — poll a bounded number of times before failing.
    async with daemonless_host(settings) as host:
        recalled = None
        for _ in range(40):  # ~8s ceiling
            recalled = await host.recall(
                "what is the codex deploy target",
                user=settings.default_user,
                session=_SESSION,
                limit=10,
            )
            if any("staging-eu-west-42" in it.content for it in recalled.items):
                break
            await asyncio.sleep(0.2)
    assert recalled is not None
    print("RECALLED " + "; ".join(f"{it.tier}/{it.channel}|{it.content}" for it in recalled.items))  # noqa: T201
    assert any(
        "staging-eu-west-42" in it.content for it in recalled.items
    ), "the codex turn's fact did not round-trip through federated recall from real stores"


async def test_genuinely_captured_real_rollout_file_flows_end_to_end(
    settings: ClientSettings, uid: str
) -> None:
    """The checked-in fixture was written by a LIVE ``codex exec`` run. Tailing that real on-disk
    file drives its real turn into the real stores unchanged."""
    assert _REAL_FIXTURE.exists(), "real codex rollout fixture missing"
    result = await backfill_codex(settings, rollout_path=_REAL_FIXTURE)
    print(f"REAL-FILE BACKFILL appended={result.appended} session_id={result.session_id}")  # noqa: T201
    assert result.appended >= 2  # the real 'pong' turn: user prompt + assistant answer
    acked = await _drain(settings)
    assert acked >= 2
    stm = await _stm_contents(settings)
    print("REAL-FILE STM " + " || ".join(stm))  # noqa: T201
    assert any("pong" in c for c in stm), "the real captured codex turn did not land in the store"
