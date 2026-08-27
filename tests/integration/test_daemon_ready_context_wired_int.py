"""Stage-3 INTEGRATE acceptance — the daemon composition root (``daemon/app.py``) now actually
wires ``RecallInjectBridge`` (S3-02, the real ``WarmRecallCacheServicePort``) into
``MemoryLifecycleManager(warm_cache=...)`` AND threads the built ``MemoryLifecycleManager`` into
``IpcServer(lifecycle_manager=...)`` — closing both seams ``test_ipc_state_ready_context_int.py``
(S3-03's own suite) deliberately left unwired (that file's own ``mlm = host.build_lifecycle_
manager()`` call passes no ``warm_cache=``, and its ``ready_resp["wired"] is False`` assertion is
pinned to exactly that unwired construction — this file does NOT touch that one, it proves the
OTHER, now-real, path: the actual resident ``LocalDaemon``).

REAL unix-socket daemon (``LocalDaemon.lifespan()``) + REAL mu-dev-cache/mu-dev-qdrant/
mu-dev-falkordb, ZERO mocks (DEV-STANDARDS non-negotiable). No SLM needed — this is pure
warm-cache/IPC plumbing, no LLM extraction/adjudication on this path.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio
from falkordb.asyncio import FalkorDB
from qdrant_client import AsyncQdrantClient
from redis.asyncio import Redis

from mu_client.capture.hook import capture_once
from mu_client.capture.model import HostKind
from mu_client.config import ClientSettings, DaemonIpcSettings, OutboxSettings
from mu_client.daemon.app import LocalDaemon

pytestmark = pytest.mark.integration

_SESSION = "ready-ctx-wired-s1"


async def _teardown(settings: ClientSettings, uid: str) -> None:
    """Duplicated per this integration package's own convention (no shared test-util package
    exists yet — see ``test_capture_daemon_int.py``'s identical docstring note)."""
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
    socket_path = Path(f"/tmp/mu-test-rcw-{uid}.sock")  # noqa: S108 — short flat AF_UNIX path
    settings = client_settings.model_copy(
        update={
            "default_workspace": f"wsrcw{uid}",
            "default_namespace": f"orgrcw{uid}",
            "outbox": OutboxSettings(
                outbox_path=tmp_path / "outbox.sqlite", poll_interval_s=0.2, batch_size=64
            ),
            "ipc": DaemonIpcSettings(socket_path=socket_path),
        }
    )
    try:
        yield settings
    finally:
        socket_path.unlink(missing_ok=True)
        await _teardown(settings, uid)


def _hook_json(*, event: str, session_id: str, **fields: object) -> bytes:
    payload = {
        "hook_event_name": event,
        "session_id": session_id,
        "cwd": "/home/user/D/mu_project/mu-client",
        "transcript_path": f"/home/user/.claude/projects/x/{session_id}.jsonl",
        **fields,
    }
    return json.dumps(payload).encode("utf-8")


async def _send_ipc(socket_path: Path, payload: dict[str, object]) -> dict[str, object]:
    reader, writer = await asyncio.open_unix_connection(str(socket_path))
    try:
        writer.write((json.dumps(payload) + "\n").encode("utf-8"))
        await writer.drain()
        line = await reader.readline()
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
    response: dict[str, object] = json.loads(line)
    return response


async def test_ready_context_reads_the_real_warm_cache_once_a_render_happened(
    isolated_settings: ClientSettings,
) -> None:
    """The acceptance this file exists for: with the daemon's REAL composition wiring (``bridge =
    RecallInjectBridge(host, bus=host.bus)`` + ``build_lifecycle_manager(warm_cache=bridge)`` +
    ``IpcServer(lifecycle_manager=mlm)``), a session that has never been rendered starts cold
    (``wired=False``, matching the pre-render honest stub — same contract as the never-wired
    case), then FLIPS to ``wired=True`` with the real rendered body the instant something actually
    pulls ``/recall`` for that session — no fabricated content, no polling of a non-existent
    counter, just ``RecallInjectBridge``'s own courtesy cache read back through
    ``MemoryLifecycleManager.ready_context``."""
    daemon = LocalDaemon(isolated_settings)
    async with daemon.lifespan():
        socket_path = isolated_settings.ipc.socket_path

        # (0) COLD — nothing rendered yet for this session.
        cold = await _send_ipc(socket_path, {"route": "ready-context", "session_id": _SESSION})
        print(f"READY-CONTEXT (cold) = {cold!r}")  # noqa: T201
        assert cold["status"] == 200
        assert cold["wired"] is False
        assert cold["rendered"] == ""

        # (1) CAPTURE a real fact through the daemon's own IPC front door + worker pool.
        raw = _hook_json(
            event="UserPromptSubmit",
            session_id=_SESSION,
            prompt="Ada lives in Paris and works at Acme",
        )
        await capture_once(isolated_settings, host=HostKind.CLAUDE_CODE, raw=raw)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and await daemon.outbox.outbox_depth() > 0:  # noqa: ASYNC110
            await asyncio.sleep(0.2)
        assert await daemon.outbox.outbox_depth() == 0, "worker pool never drained the outbox"

        # (2) PULL — a real /recall through the SAME IPC front door populates the bridge's
        # courtesy cache for this session (qdrant upsert is eventually consistent — poll).
        rendered_body = ""
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            recall_resp = await _send_ipc(
                socket_path,
                {"route": "recall", "session_id": _SESSION, "query": "Where does Ada live?"},
            )
            body = str(recall_resp.get("body", ""))
            if "Paris" in body:
                rendered_body = body
                break
            await asyncio.sleep(0.3)
        print(f"RECALL body = {rendered_body!r}")  # noqa: T201
        assert "Paris" in rendered_body, "the real recall never surfaced the captured fact"

        # (3) /ready-context now reads that SAME real courtesy-cache entry back — wired=True, the
        # identical body /recall just rendered, no second render triggered.
        warm = await _send_ipc(socket_path, {"route": "ready-context", "session_id": _SESSION})
        print(f"READY-CONTEXT (warm) = {warm!r}")  # noqa: T201
        assert warm["status"] == 200
        assert warm["wired"] is True
        assert warm["rendered"] == rendered_body
