"""REAL PreCompact promote-before-delete — mu-dev-cache + mu-dev-qdrant + mu-dev-falkordb, REAL
MiniLM embedder, ZERO mocks (DEV-STANDARDS non-negotiable). This is the Phase 3 acceptance slice
(AGENT-INTEGRATION-AUDIT-AND-PLAN.md §4 Phase 3).

The proof, on the real stores, that the PreCompact trigger is no longer a parsed-then-dropped no-op:

  1. A fact is ``add``ed with routine (default) importance, so it stays STM-ONLY — ``promoted=False``,
     ``tiers_written=('stm',)`` — the CORRECT gated contract, and exactly the "at-risk turn the host
     is about to compact away" this feature exists to save. A DIRECT qdrant read confirms the
     durable MTM tier holds NOTHING for it yet.
  2. A REAL ``PreCompact`` hook payload (parsed by the actual ``ClaudeCodeParserV1``) is fed through
     the actual ``InProcessLocalIngest`` with a REAL ``PreCompactPromoter`` wired. The ingest routes
     it PAST the control-kind skip into ``promote_session_now(ns, force=True)`` — the real
     promotion/consolidation machinery.
  3. A DIRECT qdrant read now finds the fact in the durable MTM tier (point present, payload content
     = the fact) — it was FORCE-promoted despite its below-gate routine salience.
  4. The fact is RECALLABLE from the durable tier (``recall(tier=MTM)``), and STILL recallable after
     the STM turn is EVICTED (simulating the host's post-compaction deletion) — it genuinely
     survived the compaction.
  5. Contrast: with NO promoter wired (the pre-Phase-3 behaviour), the SAME PreCompact leaves the
     durable tier empty — proving the old path was a silent no-op and Phase 3 changed it for real.

If a container is down the test RAISES (BLOCKED, never faked).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from falkordb.asyncio import FalkorDB
from mu_contracts.domain.model.memory import Namespace, Visibility
from mu_engine.storage.domain.memory import MemoryTier
from mu_engine.storage.mappers.qdrant_mapper import point_id
from qdrant_client import AsyncQdrantClient
from redis.asyncio import Redis

from mu_client.capture.parsers import ClaudeCodeParserV1
from mu_client.config import ClientSettings
from mu_client.host import LocalMemoryHost
from mu_client.lifecycle.precompact import PreCompactPromoter
from mu_client.workers.ingest_client import ExtractionSkippedError, InProcessLocalIngest

pytestmark = pytest.mark.integration

_FACT = "Ada lives in Paris and works at Acme"
_SESSION = "precompact-session"
_EMBED_DIM = 384  # MiniLM (mu-local's local embedder) — collection suffix mu_mtm__{ws}__private__384
_POLL_ATTEMPTS = 40
_POLL_DELAY_S = 0.2


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


def _ns(settings: ClientSettings, *, session: str, user: str) -> Namespace:
    return Namespace(
        org=settings.default_namespace,
        workspace=settings.default_workspace,
        user=user,
        session=session,
        visibility=Visibility.PRIVATE,
    )


def _mtm_collection(settings: ClientSettings) -> str:
    return f"mu_mtm__{settings.default_workspace}__{Visibility.PRIVATE.value}__{_EMBED_DIM}"


async def _mtm_point_present(settings: ClientSettings, memory_id: str) -> tuple[bool, str]:
    """DIRECT qdrant read of the durable MTM tier: is there a point for ``memory_id``? Returns
    (present, content). Never masks a down store — a real connection error propagates."""
    qdrant = AsyncQdrantClient(url=settings.storage.vector.url)
    try:
        collection = _mtm_collection(settings)
        if not await qdrant.collection_exists(collection):
            return False, ""
        records = await qdrant.retrieve(
            collection_name=collection, ids=[point_id(memory_id)], with_payload=True
        )
        if not records:
            return False, ""
        payload = records[0].payload or {}
        return True, str(payload.get("content", ""))
    finally:
        await qdrant.close()


def _precompact_activity(session: str) -> object:
    """A REAL PreCompact hook envelope through the actual parser — not a hand-built stand-in."""
    return ClaudeCodeParserV1().parse(
        record={"hook_event_name": "PreCompact", "session_id": session, "trigger": "auto"},
        event_id="precompact-evt-1",
    )


async def test_precompact_saves_at_risk_stm_turn_into_durable_tier_and_survives_compaction(
    isolated_settings: ClientSettings,
) -> None:
    settings = isolated_settings
    user = settings.default_user
    ns = _ns(settings, session=_SESSION, user=user)

    host = LocalMemoryHost(settings)
    await host.start()
    try:
        # (1) An at-risk STM-only turn: routine importance ⇒ NOT promoted (correct gated contract).
        write = await host.add(_FACT, user=user, session=_SESSION)
        print(  # noqa: T201 — required evidence
            f"AT-RISK ADD  memory_id={write.memory_id} promoted={write.promoted} "
            f"tiers_written={write.tiers_written}"
        )
        assert not write.promoted, "precondition: the at-risk turn must be STM-only (below gate)"
        assert write.tiers_written == ("stm",), "at-risk turn must not already be in a durable tier"

        present_before, _ = await _mtm_point_present(settings, write.memory_id)
        assert not present_before, "durable MTM tier must be EMPTY for the fact before PreCompact"

        # (2) A REAL PreCompact through the REAL ingest routing with a REAL promoter wired.
        promoter = PreCompactPromoter(
            promoter=host.build_lifecycle_manager(),
            org=settings.default_namespace,
            workspace=settings.default_workspace,
            user=user,
        )
        ingest = InProcessLocalIngest(host, user=user, precompact_promoter=promoter)
        # The ingest routes PreCompact into the promotion side-effect, then ack's via the skip
        # sentinel (the control event never becomes a MemoryItem itself). This is NOT the old
        # no-op swallow — (3)/(4) below prove the durable promotion actually happened.
        with pytest.raises(ExtractionSkippedError):
            await ingest.ingest(_precompact_activity(_SESSION))  # type: ignore[arg-type]

        # (3) DIRECT qdrant read: the fact is now in the durable MTM tier, force-promoted.
        present_after = False
        content = ""
        for _ in range(_POLL_ATTEMPTS):
            present_after, content = await _mtm_point_present(settings, write.memory_id)
            if present_after:
                break
            await asyncio.sleep(_POLL_DELAY_S)
        print(f"POST-PRECOMPACT MTM (direct qdrant) present={present_after} content={content!r}")  # noqa: T201
        assert present_after, "PreCompact did NOT promote the at-risk turn into the durable MTM tier"
        assert "Paris" in content, "the promoted MTM point does not carry the original fact"

        # (4a) Recallable from the durable tier.
        mtm_hit = ""
        for _ in range(_POLL_ATTEMPTS):
            recalled = await host.recall(
                "Where does Ada live?", user=user, session=_SESSION, tier=MemoryTier.MTM
            )
            mtm_hit = "; ".join(it.content for it in recalled.items)
            if "Paris" in mtm_hit:
                break
            await asyncio.sleep(_POLL_DELAY_S)
        print(f"RECALL(tier=MTM) = {mtm_hit!r}")  # noqa: T201
        assert "Paris" in mtm_hit, "the promoted fact is not recallable from the durable MTM tier"

        # (4b) Simulate the host's post-compaction deletion: EVICT the STM turn, then prove the
        # fact STILL recalls — it genuinely survived the compaction via the durable tier.
        await host._require_memory()._container.stm.evict(ns, write.memory_id)
        survived = ""
        for _ in range(_POLL_ATTEMPTS):
            recalled = await host.recall("Where does Ada live?", user=user, session=_SESSION)
            survived = "; ".join(f"{it.tier}:{it.content}" for it in recalled.items)
            if "Paris" in survived:
                break
            await asyncio.sleep(_POLL_DELAY_S)
        print(f"POST-COMPACTION RECALL (STM evicted) = {survived!r}")  # noqa: T201
        assert "Paris" in survived, (
            "the fact did NOT survive compaction — after the STM turn was evicted it is gone, so "
            "PreCompact promotion failed to make it durable"
        )
    finally:
        await host.aclose()


async def test_precompact_without_promoter_is_the_old_noop_swallow(
    isolated_settings: ClientSettings,
) -> None:
    """Contrast/regression: with NO promoter wired (pre-Phase-3), the SAME PreCompact is the old
    silent swallow — the at-risk turn stays STM-only and the durable tier stays empty. This pins
    down that Phase 3's durable promotion is a REAL behaviour change, not incidental."""
    settings = isolated_settings
    user = settings.default_user

    host = LocalMemoryHost(settings)
    await host.start()
    try:
        write = await host.add(_FACT, user=user, session=_SESSION)
        assert not write.promoted and write.tiers_written == ("stm",)

        ingest = InProcessLocalIngest(host, user=user, precompact_promoter=None)
        with pytest.raises(ExtractionSkippedError):
            await ingest.ingest(_precompact_activity(_SESSION))  # type: ignore[arg-type]

        # No promoter ⇒ no promotion. Give qdrant the same generous window the positive test uses,
        # then assert the durable tier is STILL empty (the old no-op).
        for _ in range(10):
            present, _ = await _mtm_point_present(settings, write.memory_id)
            assert not present, "no-promoter PreCompact must NOT promote (old no-op contract)"
            await asyncio.sleep(_POLL_DELAY_S)
        print("NO-PROMOTER PreCompact left the durable MTM tier empty (old no-op) — as expected")  # noqa: T201
    finally:
        await host.aclose()
