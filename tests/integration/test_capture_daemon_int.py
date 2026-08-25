"""REAL capture -> outbox (WAL SQLite) -> flush -> mu-local, AND real daemon capture/recall over
its real unix socket — ZERO mocks (DEV-STANDARDS non-negotiable). This is the task-acceptance
slice for the capture/outbox/inject/daemon stage: feed a REAL Claude Code hook-event JSON through
the actual `capture_once` entrypoint, prove the SQLite-WAL outbox is durable across a simulated
crash, flush into the real mu-dev-* stores, then start the resident daemon and prove capture +
recall round-trip over its real unix socket. PRINTs the real outbox rows + recalled/injected
context (required evidence) at every stage. If a container is down the test RAISES (BLOCKED,
never faked).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import sqlite3
import time
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio
from falkordb.asyncio import FalkorDB
from mu_contracts.contracts.recall import RecallItemView
from qdrant_client import AsyncQdrantClient
from redis.asyncio import Redis

from mu_client.capture.hook import capture_once
from mu_client.capture.model import HostKind
from mu_client.config import ClientSettings, DaemonIpcSettings, OutboxSettings
from mu_client.daemon.app import LocalDaemon
from mu_client.host import LocalMemoryHost, daemonless_host
from mu_client.outbox.sqlite_outbox import SqliteOutbox
from mu_client.workers.ingest_client import InProcessLocalIngest
from mu_client.workers.pool import OutboxWorker

pytestmark = pytest.mark.integration


async def _teardown(settings: ClientSettings, uid: str) -> None:
    """Drop every qdrant collection / falkordb graph / redis key this test's isolated η
    partition created (duplicated from ``test_daemonless_roundtrip_int.py`` — no shared test-util
    package exists yet across the two integration modules; both stay independently runnable)."""
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


_SESSION = "capture-s1"


@pytest_asyncio.fixture
async def isolated_settings(
    client_settings: ClientSettings, uid: str, tmp_path: Path
) -> AsyncIterator[ClientSettings]:
    """Isolated η partition (uid-tagged workspace/namespace, cleaned up in real stores) AND
    isolated on-disk paths (a fresh outbox/spool under tmp_path; the unix socket lives at a SHORT
    flat ``/tmp`` path instead — ``AF_UNIX``'s ``sun_path`` is capped at ~108 bytes on Linux and
    pytest's own nested ``tmp_path`` routinely exceeds that once ``daemon.sock`` is appended).
    NEVER the user's real ``~/.memory-universe``; never collides with a real resident daemon."""
    socket_path = Path(f"/tmp/mu-test-{uid}.sock")  # noqa: S108 — deliberate, see docstring
    settings = client_settings.model_copy(
        update={
            "default_workspace": f"ws{uid}",
            "default_namespace": f"org{uid}",
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
    """A REAL Claude Code hook stdin payload shape (host-capture-integration-devdoc.md §2.1)."""
    payload = {
        "hook_event_name": event,
        "session_id": session_id,
        "cwd": "/home/user/D/mu_project/mu-client",
        "transcript_path": f"/home/user/.claude/projects/x/{session_id}.jsonl",
        **fields,
    }
    return json.dumps(payload).encode("utf-8")


async def test_daemonless_capture_outbox_durability_and_flush_round_trip(
    isolated_settings: ClientSettings,
) -> None:
    # (1) CAPTURE — a real UserPromptSubmit hook event, no daemon listening (direct WAL append).
    raw = _hook_json(
        event="UserPromptSubmit", session_id=_SESSION, prompt="Ada lives in Paris and works at Acme"
    )
    response = await capture_once(isolated_settings, host=HostKind.CLAUDE_CODE, raw=raw)
    print(f"CAPTURE-ONCE (no daemon) response={response!r}")  # noqa: T201
    assert response == {}  # daemonless: durable capture, NO live inject this invocation (§2.3)

    outbox_path = isolated_settings.outbox.outbox_path
    assert outbox_path.exists(), "SqliteOutbox did not create a real WAL file on disk"

    # PRINT the real outbox row — required evidence, read straight off the SQLite file.
    conn = sqlite3.connect(str(outbox_path))
    rows = conn.execute(
        "SELECT seq, activity_id, host, kind, session_id, state FROM outbox"
    ).fetchall()
    conn.close()
    print(f"OUTBOX ROWS (raw SQLite read) = {rows}")  # noqa: T201
    assert len(rows) == 1
    assert rows[0][2] == "claude_code"
    assert rows[0][5] == "pending"

    # (2) DURABILITY — simulate a crash: drain to INFLIGHT, then close WITHOUT acking.
    box = SqliteOutbox(outbox_path)
    await box.open()
    batch = await box.drain(batch_size=10)
    assert len(batch.records) == 1
    await box.aclose()  # "crash" — no ack ran

    # A fresh SqliteOutbox instance over the SAME file recovers INFLIGHT -> PENDING on open().
    box2 = SqliteOutbox(outbox_path)
    await box2.open()
    depth_after_recovery = await box2.outbox_depth()
    print(f"OUTBOX DEPTH after simulated crash + reopen = {depth_after_recovery}")  # noqa: T201
    assert (
        depth_after_recovery == 1
    ), "crash recovery must re-drive INFLIGHT -> PENDING, not lose it"
    await box2.aclose()

    # (3) FLUSH — drain into the REAL mu-local engine (real embedder, real mu-dev-* stores).
    outbox = SqliteOutbox(outbox_path)
    await outbox.open()
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
        print(f"FLUSH tick={tick!r}")  # noqa: T201
        assert tick.acked == 1
        assert tick.drained == 1
    await outbox.aclose()
    box3 = SqliteOutbox(outbox_path)  # fresh handle, no leaked connection from `outbox` above
    await box3.open()
    assert await box3.outbox_depth() == 0
    await box3.aclose()

    # (4) PROVE the round trip: recall the captured fact from the REAL stores.
    async with daemonless_host(isolated_settings) as host:
        recalled = await _eventually_recall(host, "Where does Ada live?")
        print(  # noqa: T201
            "RECALLED " + "; ".join(f"{it.memory_id}|{it.content}" for it in recalled)
        )
        assert any("Paris" in it.content for it in recalled), "captured content did not round-trip"


async def _eventually_recall(host: LocalMemoryHost, query: str) -> list[RecallItemView]:
    for _ in range(40):
        listing = await host.recall(query, user="default", session=_SESSION)
        if listing.items:
            return list(listing.items)
        await asyncio.sleep(0.2)
    return []


async def test_daemon_serves_capture_and_recall_over_real_unix_socket(
    isolated_settings: ClientSettings,
) -> None:
    daemon = LocalDaemon(isolated_settings)
    async with daemon.lifespan():
        socket_path = isolated_settings.ipc.socket_path
        assert socket_path.exists(), "IpcServer did not bind the real unix socket"

        # (1) CAPTURE a real UserPromptSubmit event THROUGH the daemon's IPC front door.
        raw = _hook_json(
            event="UserPromptSubmit",
            session_id=_SESSION,
            prompt="Ada lives in Paris and works at Acme",
        )
        capture_resp = await capture_once(isolated_settings, host=HostKind.CLAUDE_CODE, raw=raw)
        print(f"DAEMON CAPTURE response={capture_resp!r}")  # noqa: T201

        # The worker pool (inside the daemon) drains on its own poll cadence — wait for the
        # outbox to empty rather than sleeping a fixed guess.
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and await daemon.outbox.outbox_depth() > 0:  # noqa: ASYNC110
            await asyncio.sleep(0.2)
        assert (
            await daemon.outbox.outbox_depth() == 0
        ), "daemon worker pool never drained the outbox"

        conn = sqlite3.connect(str(isolated_settings.outbox.outbox_path))
        rows = conn.execute("SELECT seq, activity_id, host, kind, state FROM outbox").fetchall()
        conn.close()
        print(f"DAEMON OUTBOX ROWS (post-drain) = {rows}")  # noqa: T201
        assert rows and rows[0][4] == "acked"

        # (2) RECALL/INJECT over the SAME real socket — poll (qdrant upsert is eventually
        # consistent, same real-store behaviour every integration test in this repo guards).
        rendered = None
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            raw2 = _hook_json(
                event="UserPromptSubmit", session_id=_SESSION, prompt="Where does Ada live?"
            )
            resp = await capture_once(isolated_settings, host=HostKind.CLAUDE_CODE, raw=raw2)
            body = resp.get("hookSpecificOutput", {}).get("additionalContext", "")
            if "Paris" in body:
                rendered = resp
                break
            await asyncio.sleep(0.3)
        print(f"DAEMON INJECTED additionalContext = {rendered!r}")  # noqa: T201
        assert (
            rendered is not None
        ), "daemon never served the captured fact back as additionalContext"


async def test_real_daemon_captures_a_record_far_past_asyncios_default_stream_limit(
    isolated_settings: ClientSettings,
) -> None:
    """END-TO-END durability of a BIG record against the REAL resident daemon (real unix socket,
    real SQLite-WAL outbox, real worker pool, real mu-dev-* stores) — the shape that was being
    silently dropped in production.

    A ``PostToolUse`` carries the UNTRUNCATED ``tool_response`` (the
    ``capture.tool_outcome_max_chars`` slice is taken daemon-side, after parsing), so an ordinary
    file-read tool result pushes the JSON line past asyncio's 64 KiB ``StreamReader`` default.
    The daemon's ``readline()`` then raised, the connection closed with NO response, and
    ``capture_once`` — which never inspected the reply — reported success and skipped its
    durability fallback. Nothing was written anywhere, and nothing was logged.

    This asserts only the product-level claim (the activity is durable, retrievable from the real
    outbox by content); WHICH of the two paths made it durable is pinned by the provenance-split
    unit tier in ``tests/unit/test_capture_ipc_framing.py``.
    """
    daemon = LocalDaemon(isolated_settings)
    async with daemon.lifespan():
        assert isolated_settings.ipc.socket_path.exists()

        marker = "oversize-marker-8f2a"
        raw = _hook_json(
            event="PostToolUse",
            session_id=_SESSION,
            tool_name="Read",
            tool_use_id="toolu_oversize_1",
            tool_response=marker + ("x" * 80_000),
        )
        assert len(raw) > 64 * 1024, "payload must exceed the asyncio default it is testing"

        response = await capture_once(isolated_settings, host=HostKind.CLAUDE_CODE, raw=raw)
        print(f"BIG-RECORD CAPTURE response={response!r} raw_bytes={len(raw)}")  # noqa: T201

        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and await daemon.outbox.outbox_depth() > 0:  # noqa: ASYNC110
            await asyncio.sleep(0.2)

        conn = sqlite3.connect(str(isolated_settings.outbox.outbox_path))
        rows = conn.execute("SELECT kind, state, activity_json FROM outbox").fetchall()
        conn.close()
        print(f"BIG-RECORD OUTBOX ROWS = {[(r[0], r[1]) for r in rows]}")  # noqa: T201
        assert rows, "the big record was silently dropped — nothing reached the outbox at all"
        assert any(
            marker in r[2] for r in rows
        ), "an outbox row exists but it is not the big record we captured"
