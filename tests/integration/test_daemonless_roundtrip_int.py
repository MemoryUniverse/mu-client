"""REAL daemonless round-trip — mu-dev-cache + mu-dev-qdrant + mu-dev-falkordb, REAL MiniLM
embedder, ZERO mocks (DEV-STANDARDS: non-negotiable). This is the task-acceptance slice for THIS
stage: "daemonless `add` a memory then `recall`/`search` it against the REAL mu-local + real
mu-dev-* stores; assert round-trip on real data; PRINT the stored+recalled data."

Two proofs, both through the actual public surface a Product-B developer gets:

1. The PROGRAMMATIC API (:func:`mu_client.host.daemonless_host`) — construct, add, recall, search,
   tear down; then construct a SECOND, brand-new host over the SAME settings and prove the data is
   still there (durability lives in the real stores, not an in-process cache).
2. The actual `mu` CONSOLE SCRIPT, invoked as a real subprocess (no daemon, no socket) — proves the
   installed entrypoint, not just the library function underneath it, round-trips real data.

If a container is down the test RAISES (BLOCKED, never faked).
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import subprocess
import sys
import time
from collections.abc import AsyncIterator, Awaitable, Callable

import pytest
import pytest_asyncio
from falkordb.asyncio import FalkorDB
from mu_local.views import MemoryListView
from qdrant_client import AsyncQdrantClient
from redis.asyncio import Redis

from mu_client.config import ClientSettings
from mu_client.host import daemonless_host

pytestmark = pytest.mark.integration

_USER = "u1"
_SESSION = "s1"
_FACT = "Ada lives in Paris and works at Acme"
# ``LocalMemory.add`` correctly NO LONGER hardcodes ``promote=True`` (the A6 fix): STM->MTM
# promotion is now GATED on ``importance >= IngestSettings.importance_promote`` (0.6), and add()'s
# own default importance is 0.5 — so a bare ``add(_FACT)`` lands STM-only (``promoted=False``),
# which is the CORRECT gated contract, not a bug (the fact is still durably in STM). This test
# exercises the MTM round-trip (it asserts ``promoted`` and ``"mtm" in tiers_written``), so it
# must supply a SALIENT importance to earn the promotion — driving the real gate above threshold
# rather than weakening the (still load-bearing) ``promoted`` assertion.
_SALIENT_IMPORTANCE = 0.9


@pytest_asyncio.fixture
async def isolated_settings(
    client_settings: ClientSettings, uid: str
) -> AsyncIterator[ClientSettings]:
    """A ClientSettings bound to a unique workspace/namespace so its η partition is isolated;
    teardown drops every qdrant collection / falkordb graph / redis key the run created."""
    settings = client_settings.model_copy(
        update={"default_workspace": f"ws{uid}", "default_namespace": f"org{uid}"}
    )
    try:
        yield settings
    finally:
        await _teardown(settings, uid)


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


async def _eventually(read: Callable[[], Awaitable[MemoryListView]]) -> MemoryListView:
    """Poll a recall until it returns hits (qdrant applies upserts asynchronously — the store's
    real eventual-consistency model, NOT a masked bug; bounded so a genuine empty still fails)."""
    last = await read()
    for _ in range(40):  # ~8s ceiling
        if last.items:
            return last
        await asyncio.sleep(0.2)
        last = await read()
    return last


async def test_daemonless_add_then_recall_and_search_round_trip_real_data(
    isolated_settings: ClientSettings,
) -> None:
    # (1) WRITE — the daemonless one-shot path: construct, add, tear down. Supply a salient
    #     importance so the real DeterministicPromoteStage gate promotes STM->MTM (see
    #     _SALIENT_IMPORTANCE rationale) — the assertion below still fully checks that it did.
    async with daemonless_host(isolated_settings) as host:
        write = await host.add(
            _FACT, user=_USER, session=_SESSION, importance_score=_SALIENT_IMPORTANCE
        )
    print(  # noqa: T201 — required evidence: PRINT the stored data
        f"STORED  memory_id={write.memory_id} content_hash={write.content_hash} "
        f"promoted={write.promoted} tiers_written={write.tiers_written}"
    )
    assert write.promoted, "STM->MTM deterministic promotion did not fire"
    assert "mtm" in write.tiers_written, "qdrant MTM write not recorded in the receipt"
    assert write.memory_id and write.content_hash

    # (2) READ — a FRESH daemonless host (new process-equivalent construction) proves the write is
    #     durable in the real stores, not merely cached on the host object that wrote it.
    async with daemonless_host(isolated_settings) as host:
        recalled = await _eventually(
            lambda: host.recall("Where does Ada live?", user=_USER, session=_SESSION)
        )
        print(  # noqa: T201 — required evidence: PRINT the recalled data
            "RECALLED "
            + "; ".join(
                f"{it.memory_id}|{it.tier}/{it.channel}|score={it.fused_score:.4f}|{it.content}"
                for it in recalled.items
            )
        )
        assert recalled.items, "federated recall returned nothing after a real add"
        assert any(
            "Paris" in it.content for it in recalled.items
        ), "added content did not round-trip through recall"

        # search() is a behaviour-identical alias for recall() — same real data, same ids.
        searched = await host.search("Where does Ada live?", user=_USER, session=_SESSION)
        print(  # noqa: T201
            "SEARCHED " + "; ".join(f"{it.memory_id}|{it.content}" for it in searched.items)
        )
        assert {it.memory_id for it in searched.items} == {it.memory_id for it in recalled.items}


def test_mu_console_script_round_trips_real_data_with_no_daemon(uid: str) -> None:
    """The installed ``mu`` entrypoint itself (real subprocess, no daemon, no mocks) — proves the
    console script a Product-B developer actually runs, not merely the library call beneath it."""
    env = {
        **os.environ,
        "MU_DEFAULT_WORKSPACE": f"cliws{uid}",
        "MU_DEFAULT_NAMESPACE": f"cliorg{uid}",
    }

    add_proc = subprocess.run(  # noqa: S603 — fixed argv, test-only, no shell
        # --importance drives the real STM->MTM gate above threshold (add() no longer force-promotes
        # — see _SALIENT_IMPORTANCE); the promoted=True assertion below still fully verifies it.
        [
            sys.executable,
            "-m",
            "mu_client",
            "add",
            _FACT,
            "--user",
            _USER,
            "--session",
            _SESSION,
            "--importance",
            str(_SALIENT_IMPORTANCE),
        ],
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
        check=False,
    )
    print(f"CLI STDOUT (add): {add_proc.stdout!r}")  # noqa: T201
    assert add_proc.returncode == 0, add_proc.stderr
    assert "memory_id=" in add_proc.stdout
    assert "promoted=True" in add_proc.stdout

    recall_proc = _run_cli_recall_with_retry(env)
    print(f"CLI STDOUT (recall): {recall_proc.stdout!r}")  # noqa: T201
    assert recall_proc.returncode == 0, recall_proc.stderr
    assert "Paris" in recall_proc.stdout

    async def _cleanup() -> None:
        settings = ClientSettings(default_workspace=f"cliws{uid}", default_namespace=f"cliorg{uid}")
        await _teardown(settings, uid)

    asyncio.run(_cleanup())


def _run_cli_recall_with_retry(env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """Qdrant upserts are eventually consistent (same real-store behaviour ``_eventually`` guards
    above); retry the recall subprocess a few times rather than sleeping inside one invocation."""
    last: subprocess.CompletedProcess[str] | None = None
    for _ in range(20):
        last = subprocess.run(  # noqa: S603
            [
                sys.executable,
                "-m",
                "mu_client",
                "recall",
                "Where does Ada live?",
                "--user",
                _USER,
                "--session",
                _SESSION,  # MUST match the add's --session — a different session is a
                # different η partition (a real test bug caught by the actual product
                # behaviour, not a bug in mu_client itself).
            ],
            capture_output=True,
            text=True,
            env=env,
            timeout=60,
            check=False,
        )
        if "Paris" in last.stdout:
            return last
        time.sleep(0.5)
    assert last is not None
    return last
