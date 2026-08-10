"""REAL MCP-client round-trip over stdio against the REAL stores — Phase 1 acceptance gate.

ZERO mocks, ZERO fakes (DEV-STANDARDS non-negotiable). This is a genuine JSON-RPC-over-stdio
round-trip: the ``mcp`` Python SDK's stdio CLIENT spawns the ACTUAL ``mu-mcp`` server as a
subprocess and speaks the MCP protocol to it; the server delegates to the real embedded
``LocalMemory`` over the real mu-dev-* stores (valkey/qdrant/falkordb) + the real MiniLM embedder.

Proofs, all over the wire:
1. ``list_tools`` advertises exactly {add, recall, get, consolidate, search, build_context, ask} —
   the real verb set.
2. ``add`` over MCP LANDS the fact in the REAL store — asserted by a DIRECT valkey GET of the STM
   key (bypassing all MU code), not by trusting the tool's own receipt.
3. ``recall`` over MCP returns that fact; ``search`` (the mem0 alias) returns it too;
   ``build_context`` renders a context window containing it; ``ask`` synthesises a real answer over
   it via the local SLM.
4. The ``SharedPrivateGuard`` rejects a shared/visibility argument over the wire (isError).

If a container is down the test RAISES (BLOCKED, never faked).
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import sys
import uuid
from collections.abc import AsyncIterator
from datetime import timedelta

import pytest
import pytest_asyncio
from falkordb.asyncio import FalkorDB
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from qdrant_client import AsyncQdrantClient
from redis.asyncio import Redis

from mu_client.config import ClientSettings

pytestmark = pytest.mark.integration

_USER = "mcp_u1"
_SESSION = "mcp_s1"
_FACT = "The production deploy target is staging-eu-west and the on-call is Ada"
_QUERY = "what is the deploy target?"


@pytest_asyncio.fixture
async def isolated() -> AsyncIterator[tuple[ClientSettings, str, dict[str, str]]]:
    """A unique η partition (workspace/namespace) + the env a spawned server inherits to bind it.
    Teardown drops every store artifact the run created."""
    uid = uuid.uuid4().hex[:12]
    settings = ClientSettings(default_workspace=f"mcpws{uid}", default_namespace=f"mcporg{uid}")
    env = {
        **os.environ,
        "MU_DEFAULT_WORKSPACE": f"mcpws{uid}",
        "MU_DEFAULT_NAMESPACE": f"mcporg{uid}",
    }
    try:
        yield settings, uid, env
    finally:
        await _teardown(settings, uid)


def _server_params(env: dict[str, str]) -> StdioServerParameters:
    # Spawn the REAL server module with the venv's own interpreter (the installed entrypoint's
    # code path), stdio transport, cwd = repo root so `.env.test` (store endpoints) loads.
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return StdioServerParameters(
        command=sys.executable, args=["-m", "mu_client.mcp"], env=env, cwd=repo_root
    )


async def test_mcp_stdio_client_round_trips_real_data(
    isolated: tuple[ClientSettings, str, dict[str, str]],
) -> None:
    settings, uid, env = isolated
    timeout = timedelta(seconds=120)  # first start loads the real MiniLM embedder (CPU-bound)

    async with stdio_client(_server_params(env)) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # (1) list_tools — the advertised real verb set, over the protocol.
            listed = await session.list_tools()
            names = {t.name for t in listed.tools}
            print(f"MCP list_tools -> {sorted(names)}")  # noqa: T201 — required evidence
            assert names == {
                "add",
                "recall",
                "get",
                "consolidate",
                "search",
                "build_context",
                "ask",
                "promote",
                "demote",
                "update",
                "delete",
            }, names

            # (2) add over MCP.
            add_res = await session.call_tool(
                "add",
                {"content": _FACT, "user": _USER, "session": _SESSION},
                read_timeout_seconds=timeout,
            )
            assert not add_res.isError, add_res.content
            receipt = add_res.structuredContent
            assert receipt is not None
            print(  # noqa: T201 — required evidence: the write receipt from over the wire
                f"MCP add -> memory_id={receipt['memory_id']} promoted={receipt['promoted']} "
                f"tiers={receipt['tiers_written']} ns={receipt['namespace']}"
            )
            memory_id = str(receipt["memory_id"])
            namespace = str(receipt["namespace"])

            # (2b) DIRECT store-landing proof — a raw valkey GET of the STM key, bypassing all MU
            #      code. The physical key is `mu/{ns.to_prefix()}:stm:mem:{memory_id}`
            #      (RedisMapper.memory_key, _KEY_PREFIX="mu").
            blob = await _direct_valkey_get(settings, f"mu/{namespace}:stm:mem:{memory_id}")
            assert blob is not None, "fact did NOT land in the real valkey STM store"
            assert "staging-eu-west" in blob, f"stored blob missing the fact: {blob[:200]}"
            print(f"DIRECT valkey GET -> {blob[:160]}...")  # noqa: T201 — required evidence

            # (3) recall over MCP returns the fact (poll: qdrant MTM is eventually consistent, but
            #     the STM floor makes this fast).
            recalled = await _recall_until_hit(session, read_timeout=timeout)
            items = (recalled.structuredContent or {}).get("items", [])
            print(  # noqa: T201 — required evidence: the recalled data from over the wire
                "MCP recall -> "
                + "; ".join(f"{it['memory_id']}|{it['channel']}|{it['content']}" for it in items)
            )
            assert any("staging-eu-west" in it["content"] for it in items), items

            # (3b) search over MCP — the mem0 alias returns the same fact as ranked hits.
            searched = await session.call_tool(
                "search",
                {"query": _QUERY, "user": _USER, "session": _SESSION},
                read_timeout_seconds=timeout,
            )
            assert not searched.isError, searched.content
            search_items = (searched.structuredContent or {}).get("items", [])
            print(  # noqa: T201 — required evidence
                "MCP search -> "
                + "; ".join(f"{it['channel']}|{it['content']}" for it in search_items)
            )
            assert any("staging-eu-west" in it["content"] for it in search_items), search_items

            # (3c) build_context over MCP — a rendered context window containing the fact.
            ctx = await session.call_tool(
                "build_context",
                {"query": _QUERY, "user": _USER, "session": _SESSION},
                read_timeout_seconds=timeout,
            )
            assert not ctx.isError, ctx.content
            ctx_out = ctx.structuredContent or {}
            print(  # noqa: T201 — required evidence
                f"MCP build_context -> text={ctx_out.get('text', '')[:160]!r} "
                f"n_items={len(ctx_out.get('items', []))} degraded={ctx_out.get('degraded')}"
            )
            assert "staging-eu-west" in ctx_out.get("text", ""), ctx_out

            # (3d) ask over MCP — a synthesised answer from the local SLM, citing the fact.
            asked = await session.call_tool(
                "ask",
                {"question": _QUERY, "user": _USER, "session": _SESSION},
                read_timeout_seconds=timeout,
            )
            assert not asked.isError, asked.content
            ask_out = asked.structuredContent or {}
            answer = str(ask_out.get("answer", ""))
            print(f"MCP ask -> {answer[:200]!r}")  # noqa: T201 — required evidence
            assert answer.strip(), "ask returned an empty synthesised answer"
            assert "staging" in answer.lower() or "ada" in answer.lower(), answer

            # (3e) update over MCP — SUPERSEDE the fact with new content; the NEW version is
            #      returned (new memory_id, superseded_id = old), all over the wire against real
            #      stores. Then recall returns the NEW content.
            updated = await session.call_tool(
                "update",
                {
                    "memory_id": memory_id,
                    "new_content": "The production deploy target is now prod-eu-west",
                    "user": _USER,
                    "session": _SESSION,
                },
                read_timeout_seconds=timeout,
            )
            assert not updated.isError, updated.content
            upd_out = updated.structuredContent or {}
            print(  # noqa: T201 — required evidence
                f"MCP update -> new_id={upd_out.get('memory_id')} "
                f"superseded_id={upd_out.get('superseded_id')} verb={upd_out.get('verb')}"
            )
            assert upd_out.get("verb") == "update"
            assert upd_out.get("superseded_id") == memory_id
            new_id = str(upd_out["memory_id"])
            assert new_id != memory_id, "update must mint a NEW memory id"
            # DIRECT valkey proof: the NEW version landed in STM; the OLD STM row was evicted.
            new_blob = await _direct_valkey_get(settings, f"mu/{namespace}:stm:mem:{new_id}")
            assert new_blob is not None and "prod-eu-west" in new_blob, new_blob
            old_blob = await _direct_valkey_get(settings, f"mu/{namespace}:stm:mem:{memory_id}")
            assert old_blob is None, "update did not evict the old STM row"
            print(f"DIRECT valkey GET (new) -> {new_blob[:120]}...")  # noqa: T201

            # (3f) delete over MCP — soft-delete the NEW version; direct valkey proof the STM row is
            #      evicted (no longer in active recall).
            deleted = await session.call_tool(
                "delete",
                {"memory_id": new_id, "user": _USER, "session": _SESSION},
                read_timeout_seconds=timeout,
            )
            assert not deleted.isError, deleted.content
            del_out = deleted.structuredContent or {}
            print(  # noqa: T201 — required evidence
                f"MCP delete -> verb={del_out.get('verb')} invalidated={del_out.get('invalidated')} "
                f"tiers={del_out.get('tiers_affected')}"
            )
            assert del_out.get("verb") == "delete" and del_out.get("invalidated") is True
            gone = await _direct_valkey_get(settings, f"mu/{namespace}:stm:mem:{new_id}")
            assert gone is None, "delete did not remove the STM row from active recall"

            # (4) guard — a shared/visibility argument is rejected over the wire.
            guarded = await session.call_tool(
                "add",
                {"content": "x", "user": _USER, "session": _SESSION, "visibility": "shared"},
                read_timeout_seconds=timeout,
            )
            assert guarded.isError, "guard did NOT reject a shared-plane argument"
            guard_text = " ".join(getattr(block, "text", "") for block in guarded.content).lower()
            print(f"MCP guard rejection -> {guard_text[:200]}")  # noqa: T201 — required evidence
            assert "shared" in guard_text and "private" in guard_text


async def _recall_until_hit(session: ClientSession, *, read_timeout: timedelta) -> object:
    last = None
    for _ in range(40):  # ~8s ceiling
        last = await session.call_tool(
            "recall",
            {"query": _QUERY, "user": _USER, "session": _SESSION},
            read_timeout_seconds=read_timeout,
        )
        items = (last.structuredContent or {}).get("items", [])  # type: ignore[union-attr]
        if any("staging-eu-west" in it["content"] for it in items):
            return last
        await asyncio.sleep(0.2)
    assert last is not None
    return last


async def _direct_valkey_get(settings: ClientSettings, key: str) -> str | None:
    redis: Redis = Redis.from_url(settings.storage.cache.url, decode_responses=False)
    try:
        raw = await redis.get(key.encode())
        if raw is None:
            return None
        return raw.decode() if isinstance(raw, bytes) else str(raw)
    finally:
        await redis.aclose()


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
