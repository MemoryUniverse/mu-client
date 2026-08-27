"""The memory-health + pinning surface is WIRED — proven through the real IPC front door.

This file exists because "the feature is built" and "the feature answers" were, for one review
cycle, two different things. mu-core's ``MemoryRepository`` façade landed and
``mu_local.composition.LocalContainer`` built ``MemoryHealthService``/``PinService`` over it — but
``LocalMemory`` exposed no accessor for them and ``daemon/app.py`` passed no ``health=``/``pin=``
to ``IpcServer``, so ``/health`` ``/pin`` ``/unpin`` still answered the named 503 in every
configuration. The engine was live and every user-facing surface was inert.

So the assertion here is deliberately made at the SOCKET, not at the container: a real
``LocalDaemon.lifespan()`` over real mu-dev-cache / mu-dev-qdrant / mu-dev-falkordb, a real memory
captured through the daemon's own capture path, and the three routes answering ``200`` over the
unix socket. ZERO mocks (DEV-STANDARDS non-negotiable). No SLM is needed — health and pin are
fully deterministic (memory-health-pinning-spec §3.2 line 181: *"No LLMProviderPort, no
EmbeddingPort"*).

Content-free: the assertions read statuses, ids, booleans and counts. ``MemoryHealthView`` carries
no memory text by construction, and this file never prints a captured body.
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
from mu_client.memory_health import DEFAULT_SESSION, HEALTH_ROUTE, PIN_ROUTE, UNPIN_ROUTE

pytestmark = pytest.mark.integration

_SESSION = DEFAULT_SESSION


async def _teardown(settings: ClientSettings, uid: str) -> None:
    """Scoped by this test's own ``uid``, never a blanket sweep — the dev stores are shared with
    other lanes. (Duplicated per this package's convention; no shared test-util package exists.)"""
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
        for graph in await db.list_graphs():
            name = graph.decode() if isinstance(graph, bytes) else graph
            if uid in name:
                with contextlib.suppress(Exception):
                    await db.select_graph(name).delete()
    finally:
        with contextlib.suppress(Exception):
            await db.connection.aclose()

    redis: Redis = Redis.from_url(settings.storage.cache.url, decode_responses=False)
    try:
        keys = [key async for key in redis.scan_iter(match=f"*{uid}*".encode())]
        if keys:
            await redis.delete(*keys)
    finally:
        await redis.aclose()


@pytest_asyncio.fixture
async def isolated_settings(
    client_settings: ClientSettings, uid: str, tmp_path: Path
) -> AsyncIterator[ClientSettings]:
    socket_path = Path(f"/tmp/mu-test-hp-{uid}.sock")  # noqa: S108 — short flat AF_UNIX path
    settings = client_settings.model_copy(
        update={
            "default_workspace": f"wshp{uid}",
            "default_namespace": f"orghp{uid}",
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


def _namespace(settings: ClientSettings) -> list[str]:
    """The 5-part η every namespaced route carries, built exactly as ``memory_health
    .namespace_for`` does — ``default_namespace`` fills ``org`` and ``default_workspace`` fills
    ``workspace``. Reversing the two addresses an empty partition, which would make a green
    ``/health`` prove nothing."""
    return [
        settings.default_namespace,
        settings.default_workspace,
        settings.default_user,
        _SESSION,
        "private",
    ]


async def test_health_pin_and_unpin_answer_over_the_real_socket(
    isolated_settings: ClientSettings,
) -> None:
    """The gap this lane was defined by, closed end to end.

    Every assertion below returned ``{"status": 503, "error": "health_service_not_wired"}`` /
    ``"pin_service_not_wired"`` before the wiring landed, in EVERY configuration of the build.
    """
    daemon = LocalDaemon(isolated_settings)
    async with daemon.lifespan():
        socket_path = isolated_settings.ipc.socket_path
        ns = _namespace(isolated_settings)

        # (1) A real memory, captured through the daemon's own front door and worker pool.
        await capture_once(
            isolated_settings,
            host=HostKind.CLAUDE_CODE,
            raw=_hook_json(
                event="UserPromptSubmit",
                session_id=_SESSION,
                prompt="Ada lives in Paris and works at Acme",
            ),
        )
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and await daemon.outbox.outbox_depth() > 0:  # noqa: ASYNC110
            await asyncio.sleep(0.2)
        assert await daemon.outbox.outbox_depth() == 0, "worker pool never drained the outbox"

        # (2) /health ANSWERS — a real bounded page over the real three-store partition.
        health: dict[str, object] = {}
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            health = await _send_ipc(socket_path, {"route": HEALTH_ROUTE, "namespace": ns})
            assert health["status"] == 200, f"/health did not answer: {health!r}"
            summary = health["summary"]
            assert isinstance(summary, dict)
            if int(summary["total"]) > 0:
                break
            await asyncio.sleep(0.3)  # qdrant upsert is eventually consistent
        summary = health["summary"]
        assert isinstance(summary, dict)
        assert int(summary["total"]) > 0, "the health lens saw nothing the capture had written"
        assert health["partial"] is False, "no tier was down; the view must not claim otherwise"

        # (3) An id to pin. Taken from a REAL recall through this daemon's own host rather than
        #     from ``view.entries``: ``HealthSettings.include_healthy`` defaults to False (spec
        #     §8 — "default surface is at-risk only"), so a freshly-captured, perfectly healthy
        #     memory is counted in ``summary.total`` and deliberately NOT listed. Reading the id
        #     from the engine keeps that default intact instead of asserting against a knob this
        #     test would have had to change. The assertions that matter stay on the socket.
        recalled = await daemon.host.recall("Where does Ada live?", session=_SESSION, limit=5)
        assert recalled.items, "the real recall never surfaced the captured memory"
        memory_id = recalled.items[0].memory_id

        # (4) /pin and /unpin ANSWER, and the pin is visible to the next health read.
        pinned = await _send_ipc(
            socket_path,
            {"route": PIN_ROUTE, "namespace": ns, "memory_id": memory_id, "reason": "policy"},
        )
        assert pinned["status"] == 200, f"/pin did not answer: {pinned!r}"
        assert pinned["pinned"] is True

        after = await _send_ipc(socket_path, {"route": HEALTH_ROUTE, "namespace": ns})
        after_summary = after["summary"]
        assert isinstance(after_summary, dict)
        assert int(after_summary["pinned_count"]) >= 1, "the pin never reached the stores"

        released = await _send_ipc(
            socket_path, {"route": UNPIN_ROUTE, "namespace": ns, "memory_id": memory_id}
        )
        assert released["status"] == 200, f"/unpin did not answer: {released!r}"
        assert released["pinned"] is False
