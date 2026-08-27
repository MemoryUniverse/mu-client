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
- **Cross-session federation (acceptance clause 2 / clause 4 first half) — FIXED upstream, real
  reproduction, no longer xfail.** ``LocalMemory.recall`` does build its ``RecallQuery`` with the
  default ``session_scope=None`` (S1-04, ``local_memory.py:181``), and the MTM query layer
  (``qdrant_mtm.py:_resolve_namespace_match``) relaxes to the truncated user-prefix match for a
  cross-session hit. ``RecallService``'s own belt-and-suspenders authz re-check
  (``recall/service.py:134`` -> ``recall/authz.py:RecallAuthorizationFilter.assert_items`` ->
  ``platform/tenancy.py:DefaultTenancyGuard.assert_scope`` -> ``mu_contracts/domain/model/
  scope.py:ClientScope.assert_authorized``) used to UNCONDITIONALLY reject any hit whose
  ``namespace.session`` differed from the caller's own ``ClientScope.session_id`` — with NO
  carve-out for "same user, different session, PRIVATE" the way the MTM query layer already had.
  Root-caused and fixed at the source: ``ClientScope.assert_authorized`` now only hard-walls
  ``session`` for a SHARED (room) target; a PRIVATE target's session is a filter/provenance stamp,
  never an isolation boundary (ADR 0030 "keep-and-scope") — the SAME-user invariant for PRIVATE
  stays fully enforced by ``DefaultTenancyGuard.assert_scope``'s independent user-slot check, which
  this fix does not touch. Proved below with a real ``add`` in session "s-a" then a real
  ``recall`` scoped to session "s-b" for the SAME user (federation — this test) and, in the sibling
  test right after it, the SAME shape but for a DIFFERENT user (isolation — still fully blocked).
  Not specific to this bridge: EVERY recall caller shares this fix (``scope.py``/``tenancy.py``/
  ``authz.py`` are cross-cutting ``mu-contracts``/``mu-engine`` code, not this task's owned
  ``recall_bridge.py``).

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
# ``LocalMemory.add`` correctly NO LONGER hardcodes ``promote=True`` (A6 fix): STM->MTM promotion
# is GATED on ``importance >= 0.6`` and add()'s default importance is 0.5, so a bare ``add(_FACT)``
# lands STM-only (``promoted=False``) — the CORRECT gated contract, not a bug. Every test below
# needs a REAL promotion (they turn on the ``MemoryPromoted`` push-refresh / MTM presence), so each
# add() supplies a salient importance to earn it, rather than weakening the ``promoted`` assertion.
_SALIENT_IMPORTANCE = 0.9
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


async def test_cross_session_federation_default_session_scope_none(
    isolated_settings: ClientSettings,
) -> None:
    """S1-04's cross-session narrowing (``session_scope=None`` default) — a fact added in session
    "s-a" should surface when rendering a DIFFERENT session ("s-b") of the SAME user, purely
    through the pre-existing PULL path (no bus wired at all here) — acceptance clauses 2 and 4
    (first half). See module docstring for the upstream fix (``ClientScope.assert_authorized``)
    that makes this pass; the sibling test right below proves isolation still holds for a
    different user."""
    host = LocalMemoryHost(isolated_settings)
    await host.start()
    try:
        bridge = RecallInjectBridge(host, settings=InjectSettings())
        write = await host.add(_FACT, session="s-a", importance_score=_SALIENT_IMPORTANCE)
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


async def test_cross_session_federation_does_not_leak_across_users(
    isolated_settings: ClientSettings,
) -> None:
    """The security half of S1-04's cross-session narrowing: fixing federation for the SAME user
    (the sibling test above) must NOT weaken isolation for a DIFFERENT user. A fact added by a
    different principal (``mallory``) in session "s-a" of the SAME org/workspace must never surface
    when rendering session "s-b" for THIS test's own (default) user — proving
    ``DefaultTenancyGuard.assert_scope``'s user-slot check still fully blocks a cross-user PRIVATE
    read even though ``ClientScope.assert_authorized`` no longer hard-walls PRIVATE on session. Real
    reproduction against the live stack, not a unit-level stand-in for it."""
    host = LocalMemoryHost(isolated_settings)
    await host.start()
    try:
        bridge = RecallInjectBridge(host, settings=InjectSettings())
        other_user = f"mallory-{isolated_settings.default_workspace}"
        other_write = await host.add(
            _FACT, session="s-a", user=other_user, importance_score=_SALIENT_IMPORTANCE
        )
        assert other_write.promoted, "STM->MTM deterministic promotion did not fire (other user)"

        # Give the other user's write every chance to have landed and be findable, then confirm
        # it does NOT leak into THIS test's own (default) user's cross-session federated render.
        body = ""
        for _ in range(_POLL_ATTEMPTS):
            rendered = await bridge.render("s-b", query="Where does Ada live?")
            body = rendered.body
            await asyncio.sleep(_POLL_DELAY_S)
        print(f"CROSS-USER RENDER (added by {other_user}, rendered as default) = {body!r}")  # noqa: T201
        assert "Paris" not in body, (
            "cross-user isolation breach: a fact written by a DIFFERENT user leaked into this "
            "user's cross-session federated recall"
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

        write = await host.add(_FACT, session=session, importance_score=_SALIENT_IMPORTANCE)
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

        write = await host.add(_FACT, session=session, importance_score=_SALIENT_IMPORTANCE)
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


async def test_a_sibling_session_stops_serving_a_gc_d_fact_real_stores(
    isolated_settings: ClientSettings,
) -> None:
    """The S3-02 review's headline blocker, against the REAL stores that make it real.

    The two properties already proved separately in this file collide: the ``recalled`` zone is
    SOURCED across every session of the user (``test_cross_session_federation_default_session_
    scope_none`` above — real Qdrant, real ``session_scope=None`` user-prefix match), while the
    push invalidation was keyed on the single ``Namespace`` the event carries. So a fact written
    in "s-a" was federated into "s-b"'s rendered body, and a ``MemoryGarbageCollected`` naming
    "s-a" dropped only "s-a" — leaving "s-b" serving a fact that no longer exists in ANY tier,
    with nothing that would ever remove it.

    Here the fact is genuinely deleted from both real tiers it lives in (the SAME
    ``StmTierRepository.evict``/``MtmTierRepository.remove`` ports ``LocalMemory`` itself uses) and
    the assertion is on the SIBLING session's warm body."""
    host = LocalMemoryHost(isolated_settings)
    await host.start()
    bridge = RecallInjectBridge(host, settings=InjectSettings(), bus=host.bus)
    try:
        origin, sibling = "s-fed-a", "s-fed-b"
        ns_origin = _ns(isolated_settings, session=origin)

        write = await host.add(_FACT, session=origin, importance_score=_SALIENT_IMPORTANCE)
        assert write.promoted

        # Warm BOTH sessions through the real PULL path. The sibling's body carries the fact only
        # because federation genuinely returns it — that is the precondition, asserted not assumed.
        await bridge.render(origin, query="Where does Ada live?")
        await bridge.render(sibling, query="Where does Ada live?")
        sibling_body = bridge.last_rendered_for(_ns(isolated_settings, session=sibling)) or ""
        assert "Paris" in sibling_body, (
            "precondition failed: the sibling session never federated the fact in, so this test "
            "would prove nothing"
        )

        container = host._require_memory()._container
        await container.stm.evict(ns_origin, write.memory_id)
        await container.mtm.remove(ns_origin, write.memory_id)
        await host.bus.publish(
            MemoryGarbageCollected(
                namespace=ns_origin, id=write.memory_id, prior_state=State.ACTIVE
            )
        )
        await bridge.drain_refreshes()

        body = bridge.last_rendered_for(_ns(isolated_settings, session=sibling))
        print(f"POST-GC SIBLING CACHE (session={sibling}) = {body!r}")  # noqa: T201
        assert "Paris" not in (body or ""), (
            "the SIBLING session still serves the GC'd fact — the warm cache is sourced federated "
            "but was invalidated per-session"
        )
    finally:
        await bridge.aclose()
        await host.aclose()
