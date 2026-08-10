"""REAL subagent agent-scoped partition — Phase 1.5 acceptance, ZERO mocks (DEV-STANDARDS:
integration = real running mu-dev-* containers). Proves, on the real Redis/Qdrant/FalkorDB stores,
the three things the audit's §6 "cut 2" requires:

  1. A REAL ``SubagentStop`` capture (parsed by the actual ``ClaudeCodeParserV1``, ingested by the
     actual ``InProcessLocalIngest``) lands in a DISTINCT agent-scoped ``η`` partition — shown by a
     DIRECT Redis STM key read (the key carries the ``…/{owner}/{owner_session}.sub.{agt_…}``
     partition), NOT merely a ``[subagent:…]`` text prefix.
  2. The OWNER (parent) session RECALLS the subagent's memory via the existing federate-live recall
     (a promoted subagent finding surfaces at MTM through the owner's user-prefix arm).
  3. A DIFFERENT user never sees it (cross-user isolation preserved).

If a container is down the test RAISES (BLOCKED, never faked).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable

import pytest
import pytest_asyncio
from falkordb.asyncio import FalkorDB
from mu_contracts.contracts.recall import RecallResult
from mu_contracts.domain.model.agent import (
    resolve_subagent_identity,
    subagent_partition_session,
)
from mu_engine.storage.domain.memory import MemoryTier
from qdrant_client import AsyncQdrantClient
from redis.asyncio import Redis

from mu_client.capture.parsers import ClaudeCodeParserV1
from mu_client.config import ClientSettings
from mu_client.host import daemonless_host
from mu_client.workers.ingest_client import InProcessLocalIngest

pytestmark = pytest.mark.integration

_OWNER = "owner_alice"
_OWNER_SESSION = "sess_parent"
_OTHER_USER = "intruder_carol"
_AGENT_TYPE = "researcher"


@pytest_asyncio.fixture
async def isolated_settings(
    client_settings: ClientSettings, uid: str
) -> AsyncIterator[ClientSettings]:
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


async def _eventually(read: Callable[[], Awaitable[RecallResult]]) -> RecallResult:
    """Poll until hits land (qdrant upserts are eventually consistent — bounded, real behaviour)."""
    last = await read()
    for _ in range(40):  # ~8s ceiling
        if last.items:
            return last
        await asyncio.sleep(0.2)
        last = await read()
    return last


async def test_real_subagent_capture_lands_in_distinct_agent_partition(
    isolated_settings: ClientSettings,
) -> None:
    # (1) a REAL SubagentStop hook envelope → the REAL parser → RawActivity (importance None).
    record = {
        "hook_event_name": "SubagentStop",
        "session_id": _OWNER_SESSION,
        "agent_type": _AGENT_TYPE,
        "last_assistant_message": "The capital of France is Paris",
    }
    activity = ClaudeCodeParserV1().parse(record=record, event_id="evt-sub-1")

    ident = resolve_subagent_identity(
        workspace_id=isolated_settings.default_workspace,
        owner_principal_id=_OWNER,
        parent_session_id=_OWNER_SESSION,
        agent_type=_AGENT_TYPE,
    )
    agent_session = subagent_partition_session(_OWNER_SESSION, ident.agent_principal_id)

    async with daemonless_host(isolated_settings) as host:
        # partitions default ON — the daemon wiring value.
        await InProcessLocalIngest(host, user=_OWNER).ingest(activity)

        # DIRECT STORE READ: a Redis STM key exists UNDER the agent partition (proves it is a real
        # η partition, not just a text prefix). The key is Namespace.to_prefix()-prefixed
        # (redis_stm.py / RedisMapper.memory_key), so it carries the agent-scoped session verbatim.
        redis: Redis = Redis.from_url(isolated_settings.storage.cache.url, decode_responses=False)
        try:
            keys = [
                k async for k in redis.scan_iter(match=f"*{ident.agent_principal_id}*".encode())
            ]
        finally:
            await redis.aclose()
        print(f"AGENT-PARTITION REDIS KEYS: {[k.decode() for k in keys]}")  # noqa: T201
        assert keys, "no STM key under the agent partition — subagent memory did not partition"
        assert any(b".sub." in k for k in keys), "key is not under the agent-scoped session"

        # And session-scoped STM recall confirms the split: the AGENT session has it, the plain
        # OWNER (top-level) session does NOT — the memory is genuinely in a different partition.
        in_agent = await host.recall(
            "capital of France", user=_OWNER, session=agent_session, tier=MemoryTier.STM
        )
        in_owner_top = await host.recall(
            "capital of France", user=_OWNER, session=_OWNER_SESSION, tier=MemoryTier.STM
        )
        print(  # noqa: T201
            "AGENT-SESSION STM: "
            + "; ".join(f"{it.memory_id}|{it.content}" for it in in_agent.items)
        )
        assert any("Paris" in it.content for it in in_agent.items), "agent partition missing memory"
        assert any(
            "[subagent:researcher]" in it.content for it in in_agent.items
        ), "provenance prefix lost"
        assert not any(
            "Paris" in it.content for it in in_owner_top.items
        ), "subagent memory leaked into the top-level owner-session partition"


async def test_owner_recalls_subagent_memory_but_other_user_does_not(
    isolated_settings: ClientSettings,
) -> None:
    owner = "owner_bob"
    owner_session = "sess_bob"
    agent_type = "analyst"
    fact = "The deploy target is staging-eu"

    # (1) WRITE — a high-salience subagent finding (promotes STM→MTM) via the public surface, in
    #     its own agent partition. importance_score >= importance_promote (0.6) so it reaches MTM,
    #     the tier the owner's federate-live recall spans.
    async with daemonless_host(isolated_settings) as host:
        write = await host.add(
            fact, user=owner, session=owner_session, importance_score=0.9, agent_type=agent_type
        )
    print(  # noqa: T201 — evidence: the receipt namespace IS the agent partition.
        f"STORED namespace={write.namespace} promoted={write.promoted} "
        f"tiers={write.tiers_written}"
    )
    assert ".sub.agt_" in write.namespace, "write did not land in an agent-scoped partition"
    assert write.promoted and "mtm" in write.tiers_written, "subagent finding did not reach MTM"

    # (2) READ — a FRESH host. The OWNER (parent) session recalls across its subagent's memory via
    #     the EXISTING federate-live recall (user-prefix arm spans every session under η.user).
    async with daemonless_host(isolated_settings) as host:
        parent = await _eventually(
            lambda: host.recall("what is the deploy target?", user=owner, session=owner_session)
        )
        print(  # noqa: T201
            "OWNER RECALL: "
            + "; ".join(f"{it.tier}/{it.channel}|{it.content}" for it in parent.items)
        )
        assert any(
            "staging-eu" in it.content for it in parent.items
        ), "owner did NOT recall its subagent's memory (federation broken)"

        # (3) ISOLATION — a DIFFERENT user recalls the SAME query/session and sees NOTHING of it.
        other = await host.recall(
            "what is the deploy target?", user=_OTHER_USER, session=owner_session
        )
        print(  # noqa: T201
            "OTHER-USER RECALL: "
            + "; ".join(f"{it.content}" for it in other.items)
        )
        assert not any(
            "staging-eu" in it.content for it in other.items
        ), "cross-user isolation BROKEN — another user recalled the subagent's memory"
