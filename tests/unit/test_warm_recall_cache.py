"""``RecallInjectBridge`` in its ``WarmRecallCacheService`` role (S3-02) — the three properties the
courtesy-cache shape did NOT have, plus the sync contract the manager reads it through.

Isolated logic (the ``unit`` marker permits mocks): the HOST is mocked, but the BUS is the real
``mu_engine.platform.adapters.bus_inproc.InprocBus`` — the exact object the daemon wires
(``daemon/app.py:146`` ``bus=self._host.bus``). Its dispatch semantics (inline ``await`` of every
handler, re-raise into the publisher, ``bus_inproc.py:50-60``) are the whole reason this bridge
splits synchronous invalidation from background refresh, so mocking the bus would mock away the
thing under test. The real-store end of the same behaviour lives in
``tests/integration/test_recall_bridge_bus_int.py``.
"""

from __future__ import annotations

import asyncio
import inspect
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock

import pytest
from mu_contracts.contracts.recall import RecallItemView
from mu_contracts.domain.errors import StoreUnavailableError
from mu_contracts.domain.events import (
    MemoryCaptured,
    MemoryDemoted,
    MemoryPinned,
    MemoryPromoted,
    MemoryQuarantined,
    MemorySuperseded,
    MemoryUnpinned,
)
from mu_contracts.domain.model.memory import Namespace, Tier, Visibility
from mu_engine.lifecycle.manager import MemoryLifecycleManager, WarmRecallCacheServicePort
from mu_engine.platform.adapters.bus_inproc import InprocBus
from mu_engine.platform.clock import FrozenClock
from mu_local.views import MemoryListView

from mu_client.config import CaptureSettings, ClientSettings, InjectSettings, OutboxSettings
from mu_client.host import LocalMemoryHost
from mu_client.inject.recall_bridge import RecallInjectBridge

pytestmark = pytest.mark.unit

_SESSION = "shared-session-id"


def _listing(*contents: str) -> MemoryListView:
    return MemoryListView(
        items=[
            RecallItemView(
                memory_id=f"m{i}", content=c, tier=Tier.STM, channel="stm", fused_score=1.0
            )
            for i, c in enumerate(contents)
        ]
    )


@pytest.fixture
def client_config() -> ClientSettings:
    return ClientSettings()


@pytest.fixture
async def started_host(
    monkeypatch: pytest.MonkeyPatch, client_config: ClientSettings
) -> LocalMemoryHost:
    fake_memory = AsyncMock()
    monkeypatch.setattr("mu_client.host.LocalMemory", lambda *a, **kw: fake_memory)
    host = LocalMemoryHost(client_config)
    await host.start()
    return host


def _ns(config: ClientSettings, *, user: str, session: str = _SESSION) -> Namespace:
    return Namespace(
        org=config.default_namespace,
        workspace=config.default_workspace,
        user=user,
        session=session,
        visibility=Visibility.PRIVATE,
    )


# ====================================================================== 1. TENANCY (constraint 4)
async def test_two_principals_sharing_one_session_id_never_read_each_others_body(
    started_host: LocalMemoryHost, client_config: ClientSettings
) -> None:
    """The cache key is the FULL η, not the host-supplied session id. Two principals whose hosts
    hand out the SAME session id must get two independent entries, and the session-only port read
    must REFUSE (cold) rather than pick one — CLAUDE.md rule 4 / CANONICAL §1 rule 5."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    bridge = RecallInjectBridge(started_host, settings=InjectSettings())

    recall.return_value = _listing("alice deploys to staging-eu")
    await bridge.render(_SESSION, user="alice")
    recall.return_value = _listing("bob deploys to prod-us")
    await bridge.render(_SESSION, user="bob")

    alice_ns = _ns(client_config, user="alice")
    bob_ns = _ns(client_config, user="bob")

    # Both entries survive independently — neither overwrote the other.
    assert bridge.last_rendered_for(alice_ns) is not None
    assert bridge.last_rendered_for(bob_ns) is not None
    assert "staging-eu" in (bridge.last_rendered_for(alice_ns) or "")
    assert "prod-us" in (bridge.last_rendered_for(bob_ns) or "")
    # ...and neither leaked into the other.
    assert "prod-us" not in (bridge.last_rendered_for(alice_ns) or "")
    assert "staging-eu" not in (bridge.last_rendered_for(bob_ns) or "")

    # The port's session-only read cannot disambiguate, so it serves NOTHING rather than guess.
    assert bridge.last_rendered(_SESSION) is None


async def test_session_only_read_is_served_when_exactly_one_namespace_holds_it(
    started_host: LocalMemoryHost,
) -> None:
    """The refusal above is scoped to genuine ambiguity — the ordinary single-principal daemon case
    still gets its warm body through the port's session-only signature."""
    started_host._memory.recall.return_value = _listing("Ada lives in Paris")  # type: ignore[union-attr]
    bridge = RecallInjectBridge(started_host, settings=InjectSettings())
    await bridge.render(_SESSION)
    assert "Ada lives in Paris" in (bridge.last_rendered(_SESSION) or "")


async def test_invalidate_drops_only_the_named_namespace(
    started_host: LocalMemoryHost, client_config: ClientSettings
) -> None:
    """Invalidation is η-precise. Evicting by session id would take out every co-named tenant —
    a correct-looking cache that quietly blanks a bystander."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    bridge = RecallInjectBridge(started_host, settings=InjectSettings())
    recall.return_value = _listing("alice fact")
    await bridge.render(_SESSION, user="alice")
    recall.return_value = _listing("bob fact")
    await bridge.render(_SESSION, user="bob")

    bridge.invalidate(_ns(client_config, user="alice"))

    assert bridge.last_rendered_for(_ns(client_config, user="alice")) is None
    assert "bob fact" in (bridge.last_rendered_for(_ns(client_config, user="bob")) or "")


# =================================================================== 2. INVALIDATION (constraint 2)
async def test_tier_event_drops_the_stale_body_even_when_the_refresh_cannot_run(
    started_host: LocalMemoryHost, client_config: ClientSettings
) -> None:
    """THE hazard this task names: "a stale count presented as current is no better than a false 0
    — arguably worse, because it looks alive". A real ``MemoryDemoted`` arrives while the stores
    are down, so the re-render CANNOT tell us the new truth. The superseded body must still be gone
    — invalidation is synchronous and unconditional, never contingent on a successful refresh."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    bus = InprocBus()
    bridge = RecallInjectBridge(started_host, settings=InjectSettings(), bus=bus)
    ns = _ns(client_config, user=client_config.default_user)

    recall.return_value = _listing("the demoted fact")
    await bridge.render(_SESSION)
    assert "the demoted fact" in (bridge.last_rendered_for(ns) or "")

    recall.side_effect = StoreUnavailableError("qdrant down")
    await bus.publish(
        MemoryDemoted(namespace=ns, id="m0", tier=Tier.MTM, to_tier=Tier.STM, retention=0.1)
    )

    # Immediately after publish — before any background refresh could have completed.
    assert bridge.last_rendered_for(ns) is None, "a superseded body survived a real tier transition"
    await bridge.drain_refreshes()
    assert bridge.last_rendered_for(ns) is None
    await bridge.aclose()


async def test_tier_event_refreshes_under_the_events_own_principal(
    started_host: LocalMemoryHost, client_config: ClientSettings
) -> None:
    """The push refresh re-renders the EVENT's namespace, not the daemon's ``default_user``.
    Rendering the default principal's partition in response to another principal's transition
    would warm the wrong tenant's entry and waste a real recall."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    bus = InprocBus()
    bridge = RecallInjectBridge(started_host, settings=InjectSettings(), bus=bus)
    ns = _ns(client_config, user="carol")

    recall.return_value = _listing("carol's promoted fact")
    await bus.publish(
        MemoryPromoted(namespace=ns, id="m0", frm=Tier.STM, to=Tier.MTM, reason="salient")
    )
    await bridge.drain_refreshes()

    assert recall.await_args is not None
    assert recall.await_args.kwargs["user"] == "carol"
    assert "carol's promoted fact" in (bridge.last_rendered_for(ns) or "")
    await bridge.aclose()


async def test_tier_event_never_blocks_the_publisher_on_store_io(
    started_host: LocalMemoryHost, client_config: ClientSettings
) -> None:
    """``InprocBus.publish`` awaits each handler INLINE on the publisher's own stack
    (``bus_inproc.py:59-60``), and ``MemoryPromoted``'s publisher is the capture path
    (``ingest.py:414``). So the handler must not await the recall: ``publish()`` has to return
    while the re-render is still outstanding, or a slow store stalls ingest."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    released = asyncio.Event()

    async def _slow_recall(*_a: object, **_kw: object) -> MemoryListView:
        await released.wait()
        return _listing("eventually")

    recall.side_effect = _slow_recall
    bus = InprocBus()
    bridge = RecallInjectBridge(started_host, settings=InjectSettings(), bus=bus)
    ns = _ns(client_config, user=client_config.default_user)

    # Bounded, so a handler that DOES await the recall inline fails fast here instead of
    # deadlocking the suite (the recall is only released on the line after this one).
    await asyncio.wait_for(
        bus.publish(
            MemoryPromoted(namespace=ns, id="m0", frm=Tier.STM, to=Tier.MTM, reason="salient")
        ),
        timeout=5.0,
    )
    # publish() returned even though the recall has not been released.
    assert bridge._refresh_tasks, "the refresh was awaited inline instead of scheduled"
    released.set()
    await bridge.drain_refreshes()
    assert "eventually" in (bridge.last_rendered_for(ns) or "")
    await bridge.aclose()


async def test_aclose_unsubscribes_so_a_later_event_touches_nothing(
    started_host: LocalMemoryHost, client_config: ClientSettings
) -> None:
    """``aclose()`` is what the daemon's ordered shutdown calls (``daemon/app.py``); after it, the
    bridge must be off the bus entirely — a handler still reaching into store adapters the host is
    tearing down is exactly the shutdown race the ordering exists to prevent."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    bus = InprocBus()
    bridge = RecallInjectBridge(started_host, settings=InjectSettings(), bus=bus)
    ns = _ns(client_config, user=client_config.default_user)
    recall.return_value = _listing("still here")
    await bridge.render(_SESSION)

    await bridge.aclose()
    await bus.publish(
        MemoryDemoted(namespace=ns, id="m0", tier=Tier.MTM, to_tier=Tier.STM, retention=0.1)
    )

    assert "still here" in (
        bridge.last_rendered_for(ns) or ""
    ), "a post-aclose event still reached the handler — the subscriptions were not released"


async def test_foreign_partition_event_is_invalidated_but_never_re_rendered(
    started_host: LocalMemoryHost,
) -> None:
    """A namespace this host cannot render (another org) must not schedule a doomed recall. The
    invalidate half still runs; only the re-warm is skipped."""
    bus = InprocBus()
    bridge = RecallInjectBridge(started_host, settings=InjectSettings(), bus=bus)
    foreign = Namespace(
        org="some-other-org",
        workspace="some-other-ws",
        user="mallory",
        session=_SESSION,
        visibility=Visibility.PRIVATE,
    )
    await bus.publish(
        MemoryPromoted(namespace=foreign, id="m0", frm=Tier.STM, to=Tier.MTM, reason="salient")
    )
    assert not bridge._refresh_tasks
    started_host._memory.recall.assert_not_awaited()  # type: ignore[union-attr]
    await bridge.aclose()


# ==================================================================== 3. BOUNDEDNESS (constraint 6)
async def test_cache_is_lru_bounded_and_evicts_the_least_recently_used(
    started_host: LocalMemoryHost, client_config: ClientSettings
) -> None:
    """The daemon is long-lived and renders one entry per DISTINCT namespace forever. Without a
    bound that dict only grows — a real leak of bodies up to ``body_budget_chars`` each."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    bridge = RecallInjectBridge(started_host, settings=InjectSettings(warm_cache_max_entries=2))
    for name in ("s1", "s2", "s3"):
        recall.return_value = _listing(f"fact for {name}")
        await bridge.render(name)

    assert len(bridge._last_rendered) == 2, "the warm cache grew past its configured bound"
    user = client_config.default_user
    assert bridge.last_rendered_for(_ns(client_config, user=user, session="s1")) is None
    assert bridge.last_rendered_for(_ns(client_config, user=user, session="s2")) is not None
    assert bridge.last_rendered_for(_ns(client_config, user=user, session="s3")) is not None


async def test_reading_an_entry_makes_it_the_most_recently_used(
    started_host: LocalMemoryHost, client_config: ClientSettings
) -> None:
    """LRU, not FIFO: a session that is still being read must not be evicted ahead of an idle one,
    or the cache throws away exactly the entries that are in use."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    bridge = RecallInjectBridge(started_host, settings=InjectSettings(warm_cache_max_entries=2))
    user = client_config.default_user
    for name in ("s1", "s2"):
        recall.return_value = _listing(f"fact for {name}")
        await bridge.render(name)

    bridge.last_rendered_for(_ns(client_config, user=user, session="s1"))  # touch s1
    recall.return_value = _listing("fact for s3")
    await bridge.render("s3")

    assert bridge.last_rendered_for(_ns(client_config, user=user, session="s1")) is not None
    assert bridge.last_rendered_for(_ns(client_config, user=user, session="s2")) is None


async def test_over_budget_spill_paths_do_not_collide_across_principals(
    started_host: LocalMemoryHost, tmp_path_factory: pytest.TempPathFactory
) -> None:
    """The F4 spill file holds real MEMORY CONTENT on disk. Naming it after the bare session id let
    two principals sharing a session id overwrite — and read — each other's spilled bodies at a
    predictable path."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    recall_dir = tmp_path_factory.mktemp("recall")
    bridge = RecallInjectBridge(
        started_host,
        settings=InjectSettings(body_budget_chars=400),
        recall_dir=recall_dir,
    )
    recall.return_value = _listing(*(f"alice secret {i} " + "x" * 120 for i in range(20)))
    await bridge.render(_SESSION, user="alice")
    recall.return_value = _listing(*(f"bob secret {i} " + "y" * 120 for i in range(20)))
    await bridge.render(_SESSION, user="bob")

    spills = sorted(recall_dir.glob("*.txt"))
    assert len(spills) == 2, "two principals' spills collapsed onto one path"
    bodies = [p.read_text(encoding="utf-8") for p in spills]
    assert any("alice secret" in b for b in bodies)
    assert any("bob secret" in b for b in bodies)
    for body in bodies:
        assert not ("alice secret" in body and "bob secret" in body)


# ================================= 4. THE SYNC WARM-READ CONTRACT (constraint 1, spec §5)
def test_the_warm_read_seam_is_synchronous_end_to_end() -> None:
    """spec §5 types the manager's warm reads as SYNCHRONOUS "instant" reads that never enqueue and
    never await a job — which is precisely why the tier counts are a warm cache's job and not an
    ``await`` inside ``get_state``. That contract holds only if every method on the chain is a plain
    ``def``: the moment one becomes a coroutine function, ``get_state``/``ready_context`` either
    have to await (blocking the loop, DEV-STANDARDS rule 1) or silently return a coroutine object.

    The two mu-core ends are asserted, not owned: if a future change makes ``get_state`` or
    ``ready_context`` async, this goes red here rather than in production."""
    for owner, name in (
        (MemoryLifecycleManager, "get_state"),
        (MemoryLifecycleManager, "ready_context"),
        (RecallInjectBridge, "invalidate"),
        (RecallInjectBridge, "last_rendered"),
        (RecallInjectBridge, "last_rendered_for"),
    ):
        method = getattr(owner, name)
        assert not inspect.iscoroutinefunction(method), f"{owner.__name__}.{name} became async"


def test_warm_read_returns_a_real_value_with_no_event_loop_running(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The strong form of the same contract, executed rather than introspected: the port's two
    methods must work with NO running event loop at all. A coroutine function would return an
    un-awaited coroutine here (and never touch the cache), so this fails for the right reason."""
    fake_memory = AsyncMock()
    monkeypatch.setattr("mu_client.host.LocalMemory", lambda *a, **kw: fake_memory)
    config = ClientSettings()
    host = LocalMemoryHost(config)
    fake_memory.recall.return_value = _listing("Ada lives in Paris")
    bridge = RecallInjectBridge(host, settings=InjectSettings())

    asyncio.run(_warm_one(host, bridge))  # the only awaited part: the render itself

    ns = _ns(config, user=config.default_user)
    assert bridge.last_rendered(_SESSION) == bridge.last_rendered_for(ns)
    assert "Ada lives in Paris" in (bridge.last_rendered(_SESSION) or "")
    bridge.invalidate(ns)
    assert bridge.last_rendered(_SESSION) is None


async def _warm_one(host: LocalMemoryHost, bridge: RecallInjectBridge) -> None:
    await host.start()
    await bridge.render(_SESSION)


def test_bridge_satisfies_the_warm_recall_cache_service_port() -> None:
    """Structural (PEP 544) conformance to the seam ``daemon/app.py`` threads through as
    ``build_lifecycle_manager(warm_cache=bridge)`` — the class object itself, so a signature drift
    on either side is caught without needing a live host."""
    assert issubclass(RecallInjectBridge, WarmRecallCacheServicePort)


# ============================== 5. THE WIRING — a cache nothing releases is a subscription leak
async def test_daemon_shutdown_closes_the_bridge_before_it_closes_the_engine_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``RecallInjectBridge.aclose()`` documents itself as "the daemon shutdown calls this", and
    until this pass nothing did: ``start()`` held the bridge in a bare local, so its four bus
    subscriptions outlived every ``shutdown()``. ``shutdown()`` guards every collaborator with
    ``is not None``, so this drives the real method with only the bridge wired — and asserts the
    ORDER, because a refresh still in flight is holding a real recall against store adapters
    ``host.aclose()`` is about to close."""
    fake_memory = AsyncMock()
    monkeypatch.setattr("mu_client.host.LocalMemory", lambda *a, **kw: fake_memory)
    from mu_client.daemon.app import LocalDaemon

    order: list[str] = []
    host = LocalMemoryHost(ClientSettings())
    await host.start()
    bridge = RecallInjectBridge(host, settings=InjectSettings(), bus=InprocBus())
    assert bridge._subscriptions, "precondition: the bridge is on the bus"

    real_bridge_aclose = bridge.aclose
    real_host_aclose = host.aclose

    async def _spy_bridge_aclose() -> None:
        order.append("bridge")
        await real_bridge_aclose()

    async def _spy_host_aclose() -> None:
        order.append("host")
        await real_host_aclose()

    monkeypatch.setattr(bridge, "aclose", _spy_bridge_aclose)
    monkeypatch.setattr(host, "aclose", _spy_host_aclose)

    daemon = LocalDaemon(ClientSettings())
    daemon._bridge = bridge
    daemon._host = host
    await daemon.shutdown()

    assert order == ["bridge", "host"], f"shutdown order was {order}"
    assert not bridge._subscriptions, "daemon shutdown left the bridge subscribed to the bus"


# =========================================== 6. THE REVIEW ROUND — what the first build got wrong
#
# Each test below pins ONE defect three review lenses found in the first S3-02 build. They are
# grouped here rather than merged into the sections above because each one names a specific,
# reproduced failure mode, and the docstring is where the reproduction lives.


async def test_a_sibling_session_stops_serving_a_fact_the_event_named_another_session(
    started_host: LocalMemoryHost, client_config: ClientSettings
) -> None:
    """THE collision between "source federated" and "key on the full η".

    ``recall`` federates: PRIVATE + ``session_scope=None`` relaxes the match to the truncated USER
    prefix (``qdrant_mtm.py:104-120`` ``_resolve_namespace_match``), so a memory written in ``s1``
    is genuinely returned by a recall issued from ``s2``. The first build keyed BOTH the read and
    the drop on the six-slot prefix, so ``MemoryDemoted(namespace=ns(s1))`` popped exactly one
    entry and left ``s2`` serving the identical federated fact forever — nothing else would ever
    remove it (no TTL, LRU-64 only).

    Reads stay per-key (the tenancy property below still holds); DROPS follow the sourcing grain."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    bus = InprocBus()
    bridge = RecallInjectBridge(started_host, settings=InjectSettings(), bus=bus)
    federated = "Grace is the on-call engineer"

    recall.return_value = _listing(federated)
    await bridge.render("s1")
    await bridge.render("s2")  # the SAME federated fact, sourced across the user's sessions
    # ...and a different principal, who must NOT be touched by any of this.
    await bridge.render("s1", user="mallory")

    s1 = _ns(client_config, user=client_config.default_user, session="s1")
    s2 = _ns(client_config, user=client_config.default_user, session="s2")
    other = _ns(client_config, user="mallory", session="s1")
    assert federated in (bridge.last_rendered_for(s2) or "")  # precondition

    recall.return_value = _listing()  # the fact is gone from the tiers now
    await bus.publish(
        MemoryDemoted(namespace=s1, id="m0", tier=Tier.MTM, to_tier=Tier.STM, retention=0.1)
    )

    assert bridge.last_rendered_for(s1) is None
    assert bridge.last_rendered_for(s2) is None, (
        "a SIBLING session of the same user kept serving the demoted fact — the cache was "
        "sourced federated but invalidated per-session"
    )
    # Tenancy is NOT the price of that: another principal's body is untouched.
    assert federated in (bridge.last_rendered_for(other) or "")
    await bridge.aclose()


async def test_a_warm_body_is_marked_stale_by_age_and_evicted_past_the_hard_ttl(
    started_host: LocalMemoryHost, client_config: ClientSettings
) -> None:
    """``stale_after_s``/``hot_session_ttl_s`` were declared in ``InjectSettings`` and read by
    NOTHING, so an entry rendered once was served as ``fresh`` for the daemon's whole lifetime.
    Time is the ONLY bound on the mutation paths that publish no event at all: STM rows leave by
    Valkey TTL (``redis_stm.py:110``), ``facade.delete``/``facade.update`` publish nothing, and
    another process's write never reaches this process's ``InprocBus``.
    recall-service-design.md §8 calls these TTLs "a security parameter, not merely a freshness
    knob"."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    clock = FrozenClock(datetime(2026, 8, 24, 12, 0, tzinfo=UTC))
    settings = InjectSettings(stale_after_s=120, hot_session_ttl_s=1800)
    bridge = RecallInjectBridge(started_host, settings=settings, clock=clock)
    ns = _ns(client_config, user=client_config.default_user)

    recall.return_value = _listing("the deploy target is staging-eu")
    await bridge.render(_SESSION)
    fresh = bridge.last_rendered(_SESSION)
    # Containment, not equality: `4b2d2c2` wrapped the render in §4's named sections
    # (`<memory_context><recalled_memory>`), and this test is about STALENESS, not about the
    # section markup. The two assertions below it already read this way; this line did not, so a
    # deliberate render change failed a cache test. It still bites — the body must be there, and
    # a body inside `stale_after_s` must carry no age notice.
    assert fresh is not None and "the deploy target is staging-eu" in fresh
    assert "may be" not in fresh, "a body inside stale_after_s was served with an age notice"

    clock.advance(timedelta(seconds=121))
    aged = bridge.last_rendered(_SESSION)
    assert aged is not None and "the deploy target is staging-eu" in aged
    assert "may be 121 s old" in aged, "a body past stale_after_s was served as if it were current"

    clock.advance(timedelta(seconds=1800))
    assert (
        bridge.last_rendered(_SESSION) is None
    ), "an entry past hot_session_ttl_s was still served"
    assert bridge.last_rendered_for(ns) is None
    assert len(bridge._last_rendered) == 0, "the expired entry was served-around but never dropped"


async def test_a_render_in_flight_across_an_invalidation_never_lands_in_the_cache(
    started_host: LocalMemoryHost, client_config: ClientSettings
) -> None:
    """Write-after-invalidate. Backgrounding the refresh made two writers concurrent, and
    ``render`` used to ``put`` unconditionally whenever it completed — so a render already awaiting
    ``_host.recall`` when a transition landed wrote its PRE-transition snapshot back afterwards and
    labelled it ``fresh``. Reproduced by parking the recall on an ``Event``.

    The epoch fence refuses that write. The immediate caller still gets its body — a read that
    overlaps a write is inherently one transition behind for that one turn — but it is marked
    ``stale``, and the CACHE, which would be behind forever, does not take it."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    released = asyncio.Event()

    async def _parked_recall(*_a: object, **_kw: object) -> MemoryListView:
        await released.wait()
        return _listing("SECRET: db password is hunter2")

    recall.side_effect = _parked_recall
    bridge = RecallInjectBridge(started_host, settings=InjectSettings())
    ns = _ns(client_config, user=client_config.default_user)

    pull = asyncio.create_task(bridge.render(_SESSION))
    await asyncio.sleep(0)  # let the render reach the parked recall
    bridge.invalidate(ns)  # the real transition lands mid-render
    released.set()
    rendered = await pull

    assert bridge.last_rendered_for(ns) is None, (
        "an in-flight render resurrected its pre-transition body into the cache after the "
        "invalidation"
    )
    assert rendered.staleness == "stale", "a body that raced a transition was handed back as fresh"


async def test_one_sweep_of_many_events_costs_one_re_warm_not_one_per_event(
    started_host: LocalMemoryHost, client_config: ClientSettings
) -> None:
    """Every lifecycle publisher fires PER ITEM inside a sweep loop (``promotion.py:428``,
    ``demotion.py:302``, ``retention.py:346``, ``distill.py:993``). Uncoalesced, a 50-item sweep
    became 50 simultaneous three-arm recalls — measured as a 22x regression on the capture ack,
    because those recalls embed on the SAME default ThreadPoolExecutor the capture path's own
    embedder uses. This is the regression commit 0f5de74 already fixed once for consolidation;
    the ``_inflight``/``coalesced_count`` shape is reused from ``lifecycle/session_save.py``."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    bus = InprocBus()
    bridge = RecallInjectBridge(started_host, settings=InjectSettings(), bus=bus)
    ns = _ns(client_config, user=client_config.default_user)
    recall.return_value = _listing("a fact")

    for i in range(50):
        await bus.publish(
            MemoryPromoted(namespace=ns, id=f"m{i}", frm=Tier.STM, to=Tier.MTM, reason="salient")
        )

    assert len(bridge._refresh_tasks) == 1, (
        f"the sweep fanned out to {len(bridge._refresh_tasks)} concurrent re-warms "
        "(one full three-arm recall per event)"
    )
    assert bridge.coalesced_count == 49
    await bridge.drain_refreshes()
    assert recall.await_count <= 2, (
        f"{recall.await_count} real recalls for one sweep — coalescing collapses a burst to the "
        "one in flight plus at most one pending re-run"
    )
    await bridge.aclose()


async def test_every_memory_mutating_event_on_this_bus_drops_the_body(
    started_host: LocalMemoryHost, client_config: ClientSettings
) -> None:
    """The first build subscribed to 4 of the 9 memory-mutating event types, so four real
    publishers left the warm body in place. Each is a live path, not a hypothetical:

    * ``MemorySuperseded`` — distill's SELF_EXPIRE arm (``distill.py:765-783``) publishes this and
      NOTHING else, so a conflict-resolution loser kept being injected.
    * ``MemoryQuarantined`` — a quarantined item must stop being injected at once.
    * ``MemoryPinned``/``MemoryUnpinned`` — ``pin/service.py:141,156``; pinning changes ranking.
    * ``MemoryCaptured`` — the COMMON case: an ordinary captured turn below
      ``importance_promote`` returns from the promote stage with no event at all
      (``ingest.py:391-394``), while the STM recency floor it just changed is a large part of the
      body."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    ns = _ns(client_config, user=client_config.default_user)
    at = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)
    events = [
        MemorySuperseded(namespace=ns, loser_id="m0", winner_id="m9", valid_at=at),
        MemoryQuarantined(namespace=ns, id="m0", reason="conflicting", confidence=0.9),
        MemoryPinned(namespace=ns, id="m0", by="alice"),
        MemoryUnpinned(namespace=ns, id="m0", by="alice"),
        MemoryCaptured(namespace=ns, ids=["m0"], tier=Tier.STM),
    ]
    for event in events:
        bus = InprocBus()
        bridge = RecallInjectBridge(started_host, settings=InjectSettings(), bus=bus)
        recall.return_value = _listing("Standup is at 9am")
        await bridge.render(_SESSION)
        assert bridge.last_rendered_for(ns) is not None  # precondition

        await bus.publish(event)

        assert bridge.last_rendered_for(ns) is None, (
            f"{type(event).__name__} left the warm body in place — it is published on this very "
            "bus and it changes what a correct render contains"
        )
        await bridge.aclose()


async def test_an_ordinary_capture_invalidates_without_paying_for_a_re_warm(
    started_host: LocalMemoryHost, client_config: ClientSettings
) -> None:
    """``MemoryCaptured`` fires on EVERY captured turn. Invalidating is a dict pop and must happen;
    re-warming is a full three-arm recall and must not, because the very next ``UserPromptSubmit``
    pull re-renders that session anyway — with the user's REAL prompt as the query, which a push
    refresh cannot know. Cold is correct here; a body ranked for a stale prompt is not."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    bus = InprocBus()
    bridge = RecallInjectBridge(started_host, settings=InjectSettings(), bus=bus)
    ns = _ns(client_config, user=client_config.default_user)
    recall.return_value = _listing("Standup is at 9am")
    await bridge.render(_SESSION)
    recall.reset_mock()

    await bus.publish(MemoryCaptured(namespace=ns, ids=["m0"], tier=Tier.STM))

    assert bridge.last_rendered_for(ns) is None
    assert not bridge._refresh_tasks, "a captured turn scheduled a full re-render"
    recall.assert_not_awaited()
    await bridge.aclose()


async def test_the_push_re_warm_re_ranks_against_the_last_real_prompt(
    started_host: LocalMemoryHost, client_config: ClientSettings
) -> None:
    """The cached body is QUERY-CONDITIONED (live-session-context-design.md §5.3: "the block is
    assembled *for this prompt*, not a static dump"). The first build's ``_refresh`` re-rendered
    with ``query=None``, which fell through to ``query or session_id`` — the literal session id as
    the search string — and overwrote a body ranked for the user's real prompt with one ranked for
    a meaningless token. The query now rides the entry and the re-warm reuses it."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    bus = InprocBus()
    bridge = RecallInjectBridge(started_host, settings=InjectSettings(), bus=bus)
    ns = _ns(client_config, user=client_config.default_user)
    prompt = "Where does Ada live?"

    recall.return_value = _listing("Ada lives in Paris")
    await bridge.render(_SESSION, query=prompt)

    await bus.publish(
        MemoryPromoted(namespace=ns, id="m0", frm=Tier.STM, to=Tier.MTM, reason="salient")
    )
    await bridge.drain_refreshes()

    assert recall.await_args is not None
    assert recall.await_args.args[0] == prompt, (
        f"the re-warm re-ranked against {recall.await_args.args[0]!r} instead of the last real "
        "prompt — the session id is not a search string"
    )
    await bridge.aclose()


def test_the_sync_warm_read_does_not_emit_a_log_record_per_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``last_rendered`` is what ``MemoryLifecycleManager.ready_context`` calls on the
    ``/ready-context`` request path — a spec §5 "instant" read that must never block the loop. The
    first build emitted a ``HostInjectionSkipped`` on EVERY ambiguous lookup: a pydantic construct
    + ``model_dump(mode="json")`` + a structlog emit (``observability/events.py:56-57``), i.e. on a
    real file/stdout sink a blocking ``write()`` on the loop thread, caller-triggerable and
    unbounded. Throttled, the operator still sees the condition — once per interval."""
    fake_memory = AsyncMock()
    monkeypatch.setattr("mu_client.host.LocalMemory", lambda *a, **kw: fake_memory)
    emitted: list[str] = []
    monkeypatch.setattr(
        "mu_client.inject.recall_bridge.log_host_injection_skipped",
        lambda **kw: emitted.append(str(kw["reason"])),
    )
    config = ClientSettings()
    host = LocalMemoryHost(config)
    fake_memory.recall.return_value = _listing("a fact")
    clock = FrozenClock(datetime(2026, 8, 24, 12, 0, tzinfo=UTC))
    bridge = RecallInjectBridge(
        host, settings=InjectSettings(warm_cache_notice_interval_s=60.0), clock=clock
    )

    async def _two_principals() -> None:
        await host.start()
        await bridge.render(_SESSION, user="alice")
        await bridge.render(_SESSION, user="bob")

    asyncio.run(_two_principals())
    emitted.clear()

    for _ in range(1000):
        assert bridge.last_rendered(_SESSION) is None  # still REFUSED, every single time
    assert len(emitted) == 1, f"{len(emitted)} log records for 1000 warm reads"

    clock.advance(timedelta(seconds=61))
    assert bridge.last_rendered(_SESSION) is None
    assert len(emitted) == 2, "the throttle silenced the condition instead of rate-limiting it"


async def test_daemon_start_retains_the_bridge_it_wired_onto_the_bus(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The shutdown half of the subscription-leak fix was covered; the ASSIGNMENT half — the line
    that IS the fix, ``self._bridge = bridge`` — was not, so deleting it left the suite green.
    ``start()`` is driven for real up to the step AFTER the bridge is built, which is where the
    lifecycle WAL runner is constructed; that construction is replaced with a sentinel raise so the
    test stops there instead of standing up a whole daemon (real stores, a bound socket, four
    supervised tasks) to assert one wiring property."""
    fake_memory = AsyncMock()
    # A REAL ``InprocBus`` behind the mocked engine: ``bus.subscribe`` is a plain ``def`` returning
    # a ``Subscription``, so leaving it as an ``AsyncMock`` attribute would hand the bridge
    # coroutine objects and prove nothing about the wiring.
    fake_memory.bus = InprocBus()
    monkeypatch.setattr("mu_client.host.LocalMemory", lambda *a, **kw: fake_memory)
    from mu_client.daemon import app as app_module

    class _StopAfterTheBridgeError(RuntimeError):
        pass

    def _stop(*_a: object, **_kw: object) -> None:
        raise _StopAfterTheBridgeError

    monkeypatch.setattr(app_module, "SqliteWalRunner", _stop)
    settings = ClientSettings(
        outbox=OutboxSettings(outbox_path=tmp_path / "outbox.sqlite"),
        capture=CaptureSettings(spool_dir=tmp_path / "spool"),
    )
    daemon = app_module.LocalDaemon(settings)

    with pytest.raises(_StopAfterTheBridgeError):
        await daemon.start()

    assert daemon._bridge is not None, (
        "start() built the bridge into a bare local — nothing holds it, so shutdown() can never "
        "release its bus subscriptions"
    )
    assert daemon._bridge._subscriptions, "the retained bridge was never wired onto the real bus"
    await daemon.shutdown()
    assert not daemon._bridge._subscriptions


def test_the_port_methods_match_the_ports_declared_signatures() -> None:
    """``issubclass`` against a ``@runtime_checkable`` Protocol checks member NAMES only — never
    parameter lists, never return types — so the conformance test above passes even if
    ``last_rendered`` grows a required second argument or starts returning an ``int``. This is the
    part that actually catches drift on either side of the seam."""
    assert issubclass(RecallInjectBridge, WarmRecallCacheServicePort)
    for name in ("invalidate", "last_rendered"):
        declared = inspect.signature(getattr(WarmRecallCacheServicePort, name))
        implemented = inspect.signature(getattr(RecallInjectBridge, name))
        assert str(implemented) == str(declared), (
            f"{name}{implemented} no longer matches the port's {name}{declared} — the manager "
            "calls this through the Protocol and would fail at runtime, not at import"
        )


async def test_the_mcp_stdio_server_wires_its_bridge_onto_its_own_bus(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The MCP stdio server is a SEPARATE PROCESS with its own ``LocalMemoryHost`` and its own
    bridge, and it exposes real mutating tools (``update``/``delete``/``pin``/``unpin``) against the
    SAME real stores. It built its bridge with no ``bus`` at all, so its OWN writes did not
    invalidate its OWN warm body.

    STATED, not implied: this closes the same-process half only. ``InprocBus`` is in-process, so a
    write here still never reaches the daemon process's bridge — that needs a real cross-process
    bus (reported as a design delta), and until it exists the bound on that window is TIME
    (``computed_at`` + ``stale_after_s``/``hot_session_ttl_s``, tested above), not events."""
    fake_memory = AsyncMock()
    fake_memory.bus = InprocBus()
    monkeypatch.setattr("mu_client.host.LocalMemory", lambda *a, **kw: fake_memory)
    from mu_client.mcp import build_server
    from mu_client.mcp import server as mcp_server_module

    built: list[RecallInjectBridge] = []
    real_bridge_cls: type[RecallInjectBridge] = mcp_server_module.RecallInjectBridge  # type: ignore[attr-defined]

    def _record(*args: object, **kwargs: object) -> RecallInjectBridge:
        bridge = real_bridge_cls(*args, **kwargs)  # type: ignore[arg-type]
        built.append(bridge)
        return bridge

    monkeypatch.setattr(mcp_server_module, "RecallInjectBridge", _record)
    server = build_server()

    async with server._mcp_server.lifespan(server._mcp_server):
        assert built, "the MCP lifespan built no bridge at all"
        assert built[0]._subscriptions, (
            "the MCP server's bridge is not on any bus — this server's own update/delete/pin "
            "tools mutate the tiers its warm body was rendered from"
        )


async def test_the_fence_survives_its_own_bound(
    started_host: LocalMemoryHost, client_config: ClientSettings
) -> None:
    """The epoch fence is itself bounded (it would otherwise be the leak the cache bound exists to
    prevent), and a naive bound reopens the hole it closes: pruning a cohort's epoch resets it to
    zero, which is exactly the number an already-in-flight render may be holding. A pruned epoch is
    therefore folded into a monotonically-rising floor that every unknown cohort reports, so the
    fence can never go backwards — driven here with ``warm_cache_max_entries=1``, which makes the
    prune happen on the very next invalidation."""
    recall = cast(AsyncMock, started_host._memory.recall)  # type: ignore[union-attr]
    released = asyncio.Event()

    async def _parked_recall(*_a: object, **_kw: object) -> MemoryListView:
        await released.wait()
        return _listing("alice's pre-transition fact")

    recall.side_effect = _parked_recall
    bridge = RecallInjectBridge(started_host, settings=InjectSettings(warm_cache_max_entries=1))
    alice = _ns(client_config, user="alice")

    pull = asyncio.create_task(bridge.render(_SESSION, user="alice"))
    await asyncio.sleep(0)
    bridge.invalidate(alice)  # alice's cohort epoch rises...
    bridge.invalidate(_ns(client_config, user="bob"))  # ...and is pruned by the bound
    released.set()
    await pull

    assert bridge.last_rendered_for(alice) is None, (
        "the epoch fence was reset to zero when its own LRU bound pruned the cohort, so the "
        "in-flight render resurrected a pre-transition body"
    )
