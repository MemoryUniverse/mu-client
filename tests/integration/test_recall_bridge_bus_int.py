"""``RecallInjectBridge`` <-> ``InprocBus`` wiring — REAL ``mu-dev-cache``/``mu-dev-qdrant``, a
REAL ``LocalMemoryHost``, ZERO mocks (DEV-STANDARDS non-negotiable). Covers S3-02's acceptance
list (``.claude/team_analysis`` plan, task id S3-02):

- **Push refresh** — a REAL ``MemoryPromoted`` published by the REAL ingest pipeline
  (``host.add(..., promote=True)`` is the default, ``ingest.py:250``) refreshes this bridge's
  ``_last_rendered[session]`` entry on the SAME tick — the test never calls ``bridge.render()``
  again after the ``add()``; it only re-reads the cache state the bus handler itself wrote
  (acceptance clause 1's refresh half).
- **Push invalidate** — after the underlying content is GENUINELY deleted from BOTH real tiers it
  lives in (``StmTierRepository.evict``/``MtmTierRepository.remove`` — the SAME real ports
  ``LocalMemory`` itself uses, reached via the host's own container), publishing a REAL
  ``MemoryGarbageCollected`` onto the REAL bus makes this bridge re-render the affected session
  RIGHT NOW: the test never calls ``bridge.render()`` again after publishing the event, so the
  courtesy cache stops carrying the fact without a stale hit (acceptance clause 1's invalidate
  half + clause 4's second half).
- **Cross-session federation (acceptance clause 2 / clause 4 first half) — CONFIRMED BLOCKED
  upstream, real reproduction, xfail(strict).** ``LocalMemory.recall`` does build its
  ``RecallQuery`` with the default ``session_scope=None`` (S1-04, ``local_memory.py:181``), and
  the MTM query layer (``qdrant_mtm.py:_resolve_namespace_match``) really does relax to the
  truncated user-prefix match for a cross-session hit. But ``RecallService``'s own
  belt-and-suspenders authz re-check (``recall/service.py:134`` ->
  ``recall/authz.py:RecallAuthorizationFilter.assert_items`` -> ``platform/tenancy.py:
  DefaultTenancyGuard.assert_scope`` -> ``mu_contracts/domain/model/scope.py:
  ClientScope.assert_authorized``) UNCONDITIONALLY rejects any hit whose ``namespace.session``
  differs from the caller's own ``ClientScope.session_id`` — that assertion's own docstring reads
  "Reject a cross-org / cross-workspace / cross-session target," with NO carve-out for "same
  user, different session, PRIVATE" the way the MTM query layer already has. Reproduced directly
  against the real stack below (a real ``add`` in session "s-a", then a real ``recall`` scoped to
  session "s-b" for the SAME user, SAME namespace) — raises ``NamespaceIsolationError`` every
  time, not a flaky race. This is a genuine bug in `mu-contracts`/`mu-engine` shared,
  cross-cutting code (``scope.py``/``tenancy.py``/``authz.py`` — NOT this task's owned
  ``recall_bridge.py``, and not specific to this bridge: EVERY recall caller hits the same wall).
  Kept ``xfail(strict=True)`` (not skipped, not deleted) so this test starts FAILING loudly the
  moment a future fix lands — the signal a follow-up task needs to remove the marker.

If a required container is unreachable, these tests RAISE (BLOCKED, never faked) via the real
client's own connection error — no try/except swallows a real dependency being down.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from mu_contracts.domain.events import MemoryGarbageCollected
from mu_contracts.domain.model.memory import Namespace, State, Visibility
from qdrant_client import AsyncQdrantClient
from redis.asyncio import Redis

from mu_client.config import ClientSettings, InjectSettings
from mu_client.host import LocalMemoryHost
from mu_client.inject.recall_bridge import RecallInjectBridge

pytestmark = pytest.mark.integration

_FACT = "Ada lives in Paris and works at Acme"
_POLL_ATTEMPTS = 40
_POLL_DELAY_S = 0.2


@pytest_asyncio.fixture
async def isolated_settings(
    client_settings: ClientSettings, uid: str
) -> AsyncIterator[ClientSettings]:
    """A ``ClientSettings`` bound to a unique workspace/namespace so its η partition is isolated;
    teardown drops every qdrant collection / redis key the run created (mirrors
    ``test_daemonless_roundtrip_int.py``'s own ``isolated_settings`` fixture). ``default_user`` is
    left at its own config default (never a mismatched literal here) — ``RecallInjectBridge.
    render()`` always recalls under ``ClientSettings.default_user`` (it threads no explicit
    ``user=`` of its own, host.py:203-209), so every ``add()`` below deliberately uses THAT same
    default rather than a divergent per-test user id."""
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

    redis: Redis = Redis.from_url(settings.storage.cache.url, decode_responses=False)
    try:
        keys = [k async for k in redis.scan_iter(match=f"*{uid}*".encode())]
        if keys:
            await redis.delete(*keys)
    finally:
        await redis.aclose()


def _ns(settings: ClientSettings, *, session: str) -> Namespace:
    return Namespace(
        org=settings.default_namespace,
        workspace=settings.default_workspace,
        user=settings.default_user,
        session=session,
        visibility=Visibility.PRIVATE,
    )


@pytest.mark.xfail(
    strict=True,
    reason=(
        "CONFIRMED upstream bug (real reproduction, not a guess): ClientScope.assert_authorized "
        "(mu_contracts/domain/model/scope.py) unconditionally rejects a namespace.session that "
        "differs from the caller's own scope.session_id, with no 'same user, different session' "
        "carve-out — so a genuinely cross-session, same-user PRIVATE hit that the MTM query layer "
        "(session_scope=None, S1-04) DOES surface is then thrown away by RecallService's own "
        "belt-and-suspenders authz re-check (recall/service.py:134) as a NamespaceIsolationError. "
        "Not this task's (S3-02, recall_bridge.py) bug to fix — a cross-cutting mu-engine/"
        "mu-contracts gap every recall caller shares. Remove this xfail once that gap is closed."
    ),
)
async def test_cross_session_federation_default_session_scope_none(
    isolated_settings: ClientSettings,
) -> None:
    """S1-04's cross-session narrowing (``session_scope=None`` default) — a fact added in session
    "s-a" should surface when rendering a DIFFERENT session ("s-b") of the SAME user, purely
    through the pre-existing PULL path (no bus wired at all here) — acceptance clauses 2 and 4
    (first half). See module docstring for the confirmed upstream blocker this currently hits."""
    host = LocalMemoryHost(isolated_settings)
    await host.start()
    try:
        bridge = RecallInjectBridge(host, settings=InjectSettings())
        write = await host.add(_FACT, session="s-a")
        assert write.promoted, "STM->MTM deterministic promotion did not fire"

        body = ""
        for _ in range(_POLL_ATTEMPTS):
            rendered = await bridge.render("s-b", query="Where does Ada live?")
            body = rendered.body
            if "Paris" in body:
                break
            await asyncio.sleep(_POLL_DELAY_S)
        print(f"CROSS-SESSION RENDER (added=s-a, rendered=s-b) = {body!r}")  # noqa: T201
        assert "Paris" in body, (
            "session_scope=None federation did not surface session s-a's fact while "
            "rendering session s-b for the same user"
        )
    finally:
        await host.aclose()


async def test_memory_promoted_bus_event_refreshes_same_tick_no_explicit_pull(
    isolated_settings: ClientSettings,
) -> None:
    """A REAL ``MemoryPromoted`` published by the REAL ingest pipeline (``host.add``'s own
    ``promote=True`` path, ``ingest.py:250``) refreshes this bridge's ``_last_rendered[session]``
    entry on the SAME tick. The test NEVER calls ``bridge.render()`` again after the ``add()`` —
    it only re-reads the cache state the bus handler itself wrote (acceptance clause 1, refresh
    half)."""
    host = LocalMemoryHost(isolated_settings)
    await host.start()
    bridge = RecallInjectBridge(host, settings=InjectSettings(), bus=host.bus)
    try:
        session = "s-promo"
        assert session not in bridge._last_rendered  # cold baseline: nothing rendered/added yet

        write = await host.add(_FACT, session=session)
        assert write.promoted

        body = ""
        for _ in range(_POLL_ATTEMPTS):
            cached = bridge._last_rendered.get(session)
            body = cached.body if cached is not None else ""
            if "Paris" in body:
                break
            await asyncio.sleep(_POLL_DELAY_S)
        print(f"PUSH-REFRESHED CACHE (session={session}) = {body!r}")  # noqa: T201
        assert "Paris" in body, (
            "bridge._last_rendered was never refreshed by the real MemoryPromoted bus event "
            "published by host.add()'s own ingest pipeline"
        )
    finally:
        await bridge.aclose()
        await host.aclose()


async def test_memory_garbage_collected_bus_event_invalidates_same_tick_no_stale_hit(
    isolated_settings: ClientSettings,
) -> None:
    """After a fact is GENUINELY deleted from both real tiers it lives in
    (``StmTierRepository.evict``/``MtmTierRepository.remove`` — the SAME real ports
    ``LocalMemory`` itself uses, reached via the host's own container), publishing a REAL
    ``MemoryGarbageCollected`` onto the REAL bus makes this bridge re-render the affected session
    RIGHT NOW. The test never calls ``bridge.render()`` again after publishing the event, so a
    NEXT pull never returns the stale fact and the courtesy cache itself no longer carries it
    either (acceptance clause 1, invalidate half + clause 4, second half)."""
    host = LocalMemoryHost(isolated_settings)
    await host.start()
    bridge = RecallInjectBridge(host, settings=InjectSettings(), bus=host.bus)
    try:
        session = "s-gc"
        ns = _ns(isolated_settings, session=session)

        write = await host.add(_FACT, session=session)
        assert write.promoted

        # Wait for the real MemoryPromoted push-refresh (proved by the sibling test above) to
        # land the fact in the courtesy cache first — the precondition this test's invalidation
        # assertion needs to be meaningful.
        body = ""
        for _ in range(_POLL_ATTEMPTS):
            cached = bridge._last_rendered.get(session)
            body = cached.body if cached is not None else ""
            if "Paris" in body:
                break
            await asyncio.sleep(_POLL_DELAY_S)
        assert "Paris" in body, "setup precondition failed: fact never reached the courtesy cache"

        # REAL deletion from BOTH real tiers add() populated — the SAME container instance the
        # host itself uses (StmTierRepository.evict / MtmTierRepository.remove, storage/ports.py),
        # never a second, parallel store.
        container = host._require_memory()._container
        await container.stm.evict(ns, write.memory_id)
        await container.mtm.remove(ns, write.memory_id)

        # Real event, real bus — the exact shape DemotionService/RetentionService publish today
        # (demotion.py:276, retention.py:311) once a real tier-transition removes an item.
        await host.bus.publish(
            MemoryGarbageCollected(namespace=ns, id=write.memory_id, prior_state=State.ACTIVE)
        )

        body = "Paris"
        for _ in range(_POLL_ATTEMPTS):
            cached = bridge._last_rendered.get(session)
            body = cached.body if cached is not None else ""
            if "Paris" not in body:
                break
            await asyncio.sleep(_POLL_DELAY_S)
        print(f"POST-GC CACHE (session={session}) = {body!r}")  # noqa: T201
        assert (
            "Paris" not in body
        ), "bridge._last_rendered still serves the GC'd fact after a real MemoryGarbageCollected"
    finally:
        await bridge.aclose()
        await host.aclose()
