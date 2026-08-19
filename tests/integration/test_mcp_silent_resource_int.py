"""REAL MCP-client read of the SILENT auto-inject resource over stdio against the REAL stores —
Phase 2 acceptance gate (AGENT-INTEGRATION-AUDIT-AND-PLAN §4 Phase 2 + validation gap D).

ZERO mocks, ZERO fakes (DEV-STANDARDS non-negotiable). A genuine JSON-RPC-over-stdio round-trip:
the ``mcp`` Python SDK's stdio CLIENT spawns the ACTUAL ``mu-mcp`` server as a subprocess; the
server delegates to the real embedded ``LocalMemory`` + ``RecallInjectBridge`` over the real
mu-dev-* stores (valkey/qdrant/falkordb) + the real MiniLM embedder.

Two proofs, both over the wire, on REAL data:
1. The silent resource ``memory-universe://silent/{session}`` is advertised on the resource-template
   surface — an MCP host can auto-attach it with NO explicit tool call.
2. Gap D: a session seeded with mixed real memories (2 human facts + a ``Write:`` tool-capture + a
   ``Bash:`` output line) renders, when READ as the silent resource, a DISTILLED body that CONTAINS
   the facts and DROPS the tool-noise — proven before→after against the raw STM recall of the SAME
   session.

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
from pydantic import AnyUrl
from qdrant_client import AsyncQdrantClient
from redis.asyncio import Redis

from mu_client.config import ClientSettings

pytestmark = pytest.mark.integration

_SESSION = "silent_s1"
# The silent resource renders under the server's DEFAULT user (the daemon/one-host default), so the
# seed adds omit an explicit user to land in the same partition the resource reads.
_FACT_A = "My production deploy target is staging-eu-west"
_FACT_B = "The on-call engineer this week is Ada"
_NOISE_WRITE = 'Write: {"file_path": "/app/main.py", "content": "print(hello)"}'
_NOISE_BASH = (
    "Bash: total 48\ndrwxr-xr-x 2 user user 4096 Aug 10 main.py\n-rw-r--r-- 1 user user 12"
)
_SILENT_URI = f"memory-universe://silent/{_SESSION}"


@pytest_asyncio.fixture
async def isolated() -> AsyncIterator[tuple[ClientSettings, str, dict[str, str]]]:
    uid = uuid.uuid4().hex[:12]
    settings = ClientSettings(default_workspace=f"silws{uid}", default_namespace=f"silorg{uid}")
    env = {
        **os.environ,
        "MU_DEFAULT_WORKSPACE": f"silws{uid}",
        "MU_DEFAULT_NAMESPACE": f"silorg{uid}",
    }
    try:
        yield settings, uid, env
    finally:
        await _teardown(settings, uid)


def _server_params(env: dict[str, str]) -> StdioServerParameters:
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return StdioServerParameters(
        command=sys.executable, args=["-m", "mu_client.mcp"], env=env, cwd=repo_root
    )


async def test_silent_resource_returns_distilled_context_no_tool_call(
    isolated: tuple[ClientSettings, str, dict[str, str]],
) -> None:
    _settings, _uid, env = isolated
    timeout = timedelta(seconds=120)  # first start loads the real MiniLM embedder (CPU-bound)

    async with stdio_client(_server_params(env)) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # (1) the silent resource is advertised as a template — no tool call needed to attach.
            templates = await session.list_resource_templates()
            uris = {t.uriTemplate for t in templates.resourceTemplates}
            print(f"MCP resources/templates/list -> {sorted(uris)}")  # noqa: T201 — evidence
            assert "memory-universe://silent/{session}" in uris, uris

            # (2) seed the session with mixed real memories (default user; same session).
            for content in (_FACT_A, _NOISE_WRITE, _FACT_B, _NOISE_BASH):
                res = await session.call_tool(
                    "add", {"content": content, "session": _SESSION}, read_timeout_seconds=timeout
                )
                assert not res.isError, res.content
            await _stm_has_all(session, timeout)

            # (2b) BEFORE — the raw STM recall of the session still carries the tool noise.
            raw = await session.call_tool(
                "recall",
                {"query": _SESSION, "session": _SESSION, "tier": "stm", "limit": 20},
                read_timeout_seconds=timeout,
            )
            raw_items = [it["content"] for it in (raw.structuredContent or {}).get("items", [])]
            print(f"BEFORE raw STM recall ({len(raw_items)} items):")  # noqa: T201 — evidence
            for c in raw_items:
                print(f"  - {c!r}")  # noqa: T201 — evidence
            assert any(c.startswith("Write:") for c in raw_items), "seed noise missing from STM"

            # (3) AFTER — READ the silent resource (no tool call): DISTILLED body, noise dropped.
            got = await session.read_resource(AnyUrl(_SILENT_URI))
            body = "".join(getattr(block, "text", "") for block in got.contents)
            print(f"AFTER silent resource read ({_SILENT_URI}):\n{body}")  # noqa: T201 — evidence

            assert _FACT_A in body, body
            assert _FACT_B in body, body
            assert "Write:" not in body, f"tool-capture noise leaked into inject: {body}"
            assert "Bash:" not in body, f"bash-output noise leaked into inject: {body}"


# ASYNC109: `timeout` here is a POLL BUDGET for a bounded readiness loop over a real store, not
# a per-call cancellation deadline delegated to a callee — the ruff-suggested `asyncio.timeout`
# rewrite would change this helper's contract (it must report WHICH ids were missing on expiry).
async def _stm_has_all(session: ClientSession, timeout: timedelta) -> None:  # noqa: ASYNC109
    """Poll STM recall until every seeded item is durably readable back (STM floor is fast)."""
    for _ in range(40):
        res = await session.call_tool(
            "recall",
            {"query": _SESSION, "session": _SESSION, "tier": "stm", "limit": 20},
            read_timeout_seconds=timeout,
        )
        contents = [it["content"] for it in (res.structuredContent or {}).get("items", [])]
        if _FACT_A in contents and _FACT_B in contents and any(
            c.startswith("Write:") for c in contents
        ):
            return
        await asyncio.sleep(0.2)
    raise AssertionError("seeded memories did not become STM-readable in time")


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
