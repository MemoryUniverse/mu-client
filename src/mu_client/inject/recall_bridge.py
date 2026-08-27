"""``RecallInjectBridge`` — the pull-companion of capture (capture-spec.md §7.2), PROMOTED (S3-02,
spec §12 / CANONICAL §7.22) into the ``WarmRecallCacheService`` role: ONE owner, not a second,
competing service. Renders a :class:`RenderedContext` the hook client reads (``GET /recall/
{session}``) and emits verbatim as ``additionalContext`` (host-capture-integration-devdoc.md
§2.1/§5.3).

**PULL path.** Each ``GET /recall/{session}`` call renders directly against
``LocalMemoryHost.recall`` (real stores) — the ``staleness``/warm-cache contract (fresh/stale/
cold, F4 budget, never-blank-the-host) is honored in full. **Gap D (Phase 2):** :meth:`render` no
longer dumps the raw session STM verbatim; it runs the hits through
:func:`~mu_client.inject.distill.distill_items` — drop tool-capture/output noise (``Write:``/
``Bash:``…), dedupe by content, prefer promoted/salient over the query-insensitive STM recency
floor. The SAME distilled render backs the MCP silent resource (``memory-universe://silent/{…}``),
so both auto-inject surfaces emit identical distilled context.

**Cross-session sourcing of the ``recalled`` zone (S1-04, live-session-context-design.md §4).** The
PULL render sources the ``recalled`` zone across **every session of the asking user**, not just the
asking one. It does so through the ONE existing federation seam and adds no second path: the MTM
arm (``qdrant_mtm.py:_resolve_namespace_match``) and the LTM arm (``falkor_ltm.py:
_resolve_memory_namespace_filter``) both relax a PRIVATE match to the truncated, session-less USER
prefix whenever ``session_scope is None``, and ``None`` is the default every arm is called with
(``ranker.py:185`` ``self._mtm.semantic(...)``, ``ranker.py:292`` ``self._ltm.graph_recall(...)`` —
neither passes the kwarg). The STM arm stays session-scoped (``ranker.py:182``
``self._stm.recent(ns, ...)``), which is correct: the recency floor is per-session by definition.
Proven end-to-end (same user federates / different user is blocked) in
``tests/integration/test_recall_bridge_bus_int.py``.

**Federated in, federated out — the invalidation grain follows the SOURCING grain.** Because a
memory written in session ``s1`` is genuinely returned by a recall issued from ``s2`` (same user,
PRIVATE, ``session_scope=None``), a body cached under ``s2`` can carry a fact whose transition
event names ``s1``. Invalidating only the event's own six-slot key therefore leaves every sibling
session serving the very fact that just left the tier. :meth:`invalidate` consequently drops the
whole **user-prefix cohort** — ``mu/{org}/{workspace}/{visibility}/{user_slot}``, the SAME
truncated grain ``qdrant_mtm._user_prefix`` federates over, mirrored here (SHARED -> ``*`` user
slot) rather than re-invented. This is not a weakening of tenancy: entries stay KEYED on the full
six-slot ``to_prefix()`` (no read can ever cross a tenant), and the cohort is a strict subset of
one user's own partition — no other org, workspace, visibility or user is touched. Dropping is
always safe: the cache is a CQRS read model (live-session-context-design.md §0) and a dropped
sibling simply renders fresh on its next pull.

**PUSH path (spec §12 lines 397-404).** When a real ``EventBusPort`` (the LOCAL ``InprocBus``) is
threaded in via the optional ``bus=`` constructor kwarg, this bridge ALSO subscribes to the engine's
own memory-mutating events. ``bus=None`` (a daemonless/one-shot caller with no ``InprocBus`` wired)
degrades to PULL-only with NO new ``DegradeReason`` — the existing ``RECALL_CORE_DOWN``/
``STALE_INJECTION`` reasons already cover "the bus subscription itself is unavailable."

**The push handler SPLITS invalidate from refresh, and that split is the whole point.**
``InprocBus.publish`` fires handlers *inline* and awaits each one (``bus_inproc.py:59-60``), so the
handler runs on the publisher's own stack — i.e. inside ``IngestService``'s capture path
(``ingest.py:414`` publishes ``MemoryPromoted``) and inside ``DistillPipeline``
(``distill.py:993``/``:472``). Therefore:

1. **Invalidation is synchronous, unconditional, and cannot fail** — dict pops over the event's own
   user-prefix cohort. It happens before anything that can raise, so a tier transition can never
   leave a superseded body in the cache. This is the correctness-critical half: a stale body
   presented as ``fresh`` is worse than a cold miss, because it looks alive.
2. **Refresh is scheduled, not awaited** — a background task re-renders against the real stores.
   Awaiting a full three-arm recall (embed + Qdrant + FalkorDB) inline would put real store I/O on
   the capture ack path (the p99 budget ``daemon/app.py`` calls out) and make a slow store stall
   ingest. The refresh only re-warms; it is never what makes the cache correct.

**Backgrounding the refresh is what creates the write-after-invalidate hazard, so the writes are
FENCED.** Every publisher fires PER ITEM inside a sweep loop (``promotion.py:428``,
``demotion.py:302``, ``retention.py:346``, ``distill.py:993``), and a render already awaiting store
I/O when a later event lands would otherwise ``put`` its PRE-transition snapshot back into the
cache AFTER the invalidation and label it ``fresh`` — resurrecting exactly what was just removed.
Two mechanisms prevent it, both structural:

* **Epoch fence.** :class:`_ScopedRenderCache` keeps a monotonic epoch per user-prefix cohort,
  bumped by every drop. :meth:`render` reads the epoch BEFORE it issues the recall and hands it
  back to ``put``; a ``put`` whose epoch no longer matches is REFUSED, and the raced body is
  returned to the immediate caller marked ``stale`` (never ``fresh``) rather than cached.
* **Coalescing.** One in-flight refresh per namespace, with a single pending re-run flag — the
  ``_inflight``/``coalesced_count`` shape ``lifecycle/session_save.py:106-181`` already uses for
  the same reason (commit 0f5de74: consolidation must never block the capture path). A 200-event
  sweep over one namespace collapses to at most two renders instead of 200 concurrent three-arm
  recalls saturating the default executor the capture path's own embedder runs on.
  ``warm_cache_refresh_concurrency`` additionally caps how many DISTINCT namespaces may hold a
  real recall open at once.

**Time is a bound, not a decoration (recall-service-design.md §2.4/§8).** Events cover only the
transitions that publish one; STM rows leave by Valkey TTL with no event at all
(``redis_stm.py:110``), ``facade.delete``/``facade.update`` publish nothing (reported), and a write
made in ANOTHER process (the MCP stdio server) never reaches this process's ``InprocBus``. So the
warm entry carries ``computed_at`` and the two configured TTLs are enforced on every read:
``stale_after_s`` (120) is the age past which a body is served WITH the ``(memory may be N s old)``
marker and a named ``DegradedModeEntered(mode="serve_stale_with_marker", reason=STALE_INJECTION)``;
``hot_session_ttl_s`` (1800) is the hard ceiling at which the entry is evicted and the read goes
cold. recall-service-design.md §8 calls these TTLs "a security parameter, not merely a freshness
knob" — the bounded read-after-revoke window — which is precisely why they may not sit unread in
``InjectSettings``.

**Tenancy: the cache is keyed on the FULL namespace, never on the bare session id.** See
:class:`_ScopedRenderCache`. ``WarmRecallCacheServicePort.last_rendered(session_id)`` (mu-core, see
below) can only pass a session id, so a session-only lookup is served ONLY when it resolves to
exactly one namespace and REFUSED (cold, named event) when it is ambiguous — never guessed.

**``WarmRecallCacheServicePort`` conformance (structural, PEP 544).** ``mu_engine.lifecycle.
manager.MemoryLifecycleManager`` carries an optional ``warm_cache:
WarmRecallCacheServicePort | None`` seam (``manager.py:226-240``) whose two required methods —
``invalidate(ns) -> None`` and ``last_rendered(session_id) -> str | None`` — :meth:`invalidate` and
:meth:`last_rendered` satisfy below, both plain ``def`` because ``ready_context``/``get_state`` are
SYNC "instant warm read" methods by spec §5 and must never await store I/O. That contract is taken
literally here: the sync path emits at most one structlog record per notice key per
``warm_cache_notice_interval_s`` (:class:`_NoticeThrottle`), because a pydantic construct +
``model_dump`` + a ``write()`` to a real log sink on EVERY ``/ready-context`` request is itself
blocking I/O on the loop thread, at IPC rate. The daemon threads this same instance through at
``daemon/app.py:173-178`` (``warm_cache=bridge``) — one owner, CANONICAL §7.22.

**What this port CANNOT do, stated plainly rather than implied.** ``WarmRecallCacheServicePort``
has no count-bearing method and ``MemoryLifecycleManager.get_state`` never consults
``self._warm_cache`` at all, so wiring this bridge in fixed ``ready_context`` and did NOT — and
structurally cannot — have anything to do with ``get_state``'s tier counts. This bridge caches
rendered BODIES keyed by session; a tier count is a per-user-prefix cardinality. Different key,
different shape, different lifetime.

**Correction (ARCHITECTURE-DELTAS AD-24, and the reason this paragraph is worth reading).** An
earlier version of this text went on to assert that ``get_state`` "returns literal
``stm_count=0``/``mtm_count=0``/``ltm_count=0``" and that closing it would need three specific
mu-core changes (a ``count(ns)`` primitive on the tier ports, an async seed seam on
``LocalContainer``, and ``get_state`` reading the result). **Both claims are now stale, and the
second was never the design that landed.** The counts are no longer ``0``: mu-core grew
``mu_engine.lifecycle.counts.TierCountCache``, an in-memory per-``UserPrefix`` cache fed by the
plane's own ``InprocBus`` and read synchronously by ``get_state``, wired once in
``LocalContainer``. No tier-port count primitive and no async seed seam were involved. Read
``mu_engine/lifecycle/counts.py`` for what the numbers mean (an observed DELTA, never a store
cardinality) — writing a confident wrong reason into a docstring for the next agent to believe is
precisely the failure mode AD-24 exists to remove, so it is corrected here rather than deleted.

**mu-client itself needs no change for any of this** and deliberately gets none: the counts arrive
through the same ``LifecycleStateView`` this daemon already serves.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import NamedTuple

import structlog
from mu_contracts.domain.errors import MemoryUniverseError
from mu_contracts.domain.events import (
    ConsolidationCompleted,
    DegradeReason,
    MemoryCaptured,
    MemoryDemoted,
    MemoryGarbageCollected,
    MemoryPinned,
    MemoryPromoted,
    MemoryQuarantined,
    MemorySuperseded,
    MemoryUnpinned,
)
from mu_contracts.domain.model.memory import Namespace, Visibility
from mu_contracts.ports.bus import EventBusPort, Subscription
from mu_contracts.ports.time import Clock
from mu_engine.platform.clock import SystemClock
from pydantic import BaseModel, ConfigDict

from mu_client.config import InjectSettings
from mu_client.host import LocalMemoryHost
from mu_client.inject.distill import distill_items
from mu_client.observability.events import log_degraded, log_host_injection_skipped

__all__ = ["RecallInjectBridge", "RenderedContext"]

_log = structlog.get_logger("mu.client.inject")

# EVERY memory-mutating event this bridge can see on the LOCAL bus, not the four tier-transition
# ones alone. Each one changes what a correct render would contain, and each carries a structural
# ``namespace: Namespace`` — the ONLY field the handler reads.
#
#   * ``MemoryPromoted`` / ``MemoryDemoted`` / ``MemoryGarbageCollected`` /
#     ``ConsolidationCompleted`` — the MLM tier transitions (spec §12 lines 397-404).
#   * ``MemorySuperseded`` — conflict resolution. distill's SELF_EXPIRE arm (``distill.py:765-783``)
#     publishes this and NOTHING else, so without it a superseded fact keeps being injected until
#     the whole sweep's closing ``ConsolidationCompleted``, if one comes at all.
#   * ``MemoryQuarantined`` — a quarantined item must stop being injected immediately.
#   * ``MemoryPinned`` / ``MemoryUnpinned`` — pinning changes ranking and lifecycle eligibility,
#     i.e. what a correct render contains. (``PinService`` is not wired into either mu-client
#     composition root yet — both pass ``pin=None`` — so this subscription is what makes the warm
#     cache correct on the day it is, rather than a second thing to remember then.)
#   * ``MemoryCaptured`` — the COMMON case, and the one nothing else covers: an ordinary captured
#     turn whose importance is below ``importance_promote`` returns from the promote stage with no
#     event at all (``ingest.py:391-394``), while the STM recency floor it just changed is a large
#     part of the rendered body.
_MutationEvent = (
    MemoryPromoted
    | MemoryDemoted
    | MemoryGarbageCollected
    | ConsolidationCompleted
    | MemorySuperseded
    | MemoryQuarantined
    | MemoryPinned
    | MemoryUnpinned
    | MemoryCaptured
)

# The subset that also schedules a background re-warm. ``MemoryCaptured`` is deliberately
# INVALIDATE-ONLY: it fires on every captured turn, the very next ``UserPromptSubmit`` pull
# re-renders that session against the real stores anyway (with the REAL prompt as the query, which
# a push refresh cannot know), and a full three-arm recall per captured turn would put that load on
# the daemon for a body nobody may read. Cold is correct; stale is not.
_REFRESH_EVENTS: tuple[type, ...] = (
    MemoryPromoted,
    MemoryDemoted,
    MemoryGarbageCollected,
    ConsolidationCompleted,
    MemorySuperseded,
    MemoryQuarantined,
    MemoryPinned,
    MemoryUnpinned,
)

# Length of the namespace-prefix digest appended to an F4 spill filename (below). Long enough that
# two real η prefixes will not collide; short enough to keep the path readable.
_SPILL_DIGEST_CHARS = 12


def _user_prefix(ns: Namespace) -> str:
    """The federation grain — ``mu/{org}/{workspace}/{visibility}/{user_slot}``, i.e.
    ``Namespace.to_prefix()`` with the session slot dropped. A MIRROR of
    ``mu_engine.storage.adapters.qdrant_mtm._user_prefix`` (``qdrant_mtm.py:95-101``), SHARED's
    ``*`` user slot included, because the set of entries a transition can affect must be exactly
    the set of entries the recall could have sourced it into. mu-core exposes it as a private
    adapter helper (never a public ``Namespace`` method), so it is reproduced here with its source
    cited rather than imported across a package boundary — reported as a DRY delta."""
    user_slot = "*" if ns.visibility is Visibility.SHARED else ns.user
    return "/".join(("mu", ns.org, ns.workspace, ns.visibility.value, user_slot))


class _NoticeThrottle:
    """At most one notice per key per ``interval_s``, over a bounded key set.

    Exists because the SYNCHRONOUS warm read is on the ``ready_context`` path (spec §5: "instant",
    never blocks) and is caller-triggerable at IPC rate. Every notice this module emits from that
    path costs a pydantic construct + ``model_dump(mode="json")`` + a structlog emit
    (``observability/events.py:56-57``) — on a real file/stdout sink, a blocking ``write()`` on the
    loop thread. Throttling keeps the signal (an operator still sees the condition, once a minute)
    and removes the unbounded per-read cost. Bounded like the cache itself: the LRU key set can
    never grow past ``max_keys``."""

    def __init__(self, *, interval_s: float, max_keys: int) -> None:
        self._interval_s = interval_s
        self._max_keys = max_keys
        self._last: OrderedDict[str, datetime] = OrderedDict()

    def allow(self, key: str, now: datetime) -> bool:
        last = self._last.get(key)
        if last is not None and (now - last).total_seconds() < self._interval_s:
            self._last.move_to_end(key)
            return False
        self._last[key] = now
        self._last.move_to_end(key)
        while len(self._last) > self._max_keys:
            self._last.popitem(last=False)
        return True


class RenderedContext(BaseModel, frozen=True):
    """capture-spec.md §7.2 / recall-service-design.md §2.2 shape, verbatim — including
    ``computed_at``, without which nothing downstream can tell a body rendered a second ago from
    one rendered when the daemon booted."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    session_id: str
    body: str
    etag: str
    computed_at: datetime
    staleness: str  # "fresh" | "stale" | "cold"


class _CacheEntry(NamedTuple):
    """One warm entry. ``query`` is the search string the body was RANKED against — the body is
    query-conditioned (live-session-context-design.md §5.3: "the block is assembled *for this
    prompt*, not a static dump"), so a background re-render must reuse it. Without it ``_refresh``
    re-ranked against ``query or session_id``, i.e. the literal session id as the search string,
    and overwrote a body ranked for the user's real prompt with one ranked for a meaningless
    token."""

    rendered: RenderedContext
    query: str | None


class _ScopedRenderCache:
    """The warm cache's storage. Four properties this type exists to make STRUCTURAL rather than
    hoped-for, because none of them were:

    **1. Namespace-keyed (CLAUDE.md rule 4 / CANONICAL §1 rule 5 / live-session-context-design.md
    §0: "scoped by the same ``Namespace.to_prefix()`` as everything else").** Entries are keyed on
    the FULL six-slot ``mu/{org}/{workspace}/{visibility}/{user}/{session}`` prefix, never on the
    bare ``session_id``. A session id is host-supplied and carries no tenancy: two principals whose
    hosts hand out the same session id (short ids in tests; a re-used default session name; the
    same human on two orgs) collided into ONE entry under session-only keying, so one tenant's
    rendered memory body was served to another. Keying on ``to_prefix()`` makes that
    unrepresentable — there is no key two namespaces can share.

    **2. Cohort-invalidated at the FEDERATION grain.** Reads are per-key; drops are per
    ``_user_prefix`` cohort (module docstring). The two grains differ because sourcing and
    invalidation must agree, and sourcing federates.

    **3. Bounded — in space AND in time.** ``max_entries`` (``InjectSettings
    .warm_cache_max_entries``) caps the dict (LRU); ``hot_session_ttl_s`` caps how long any entry
    may live at all, so an entry whose content left the tiers through a path that publishes no
    event (Valkey STM TTL expiry; ``facade.delete``; another process's write) is retired by age
    instead of being served forever. Dropping is always safe — this is a CQRS read model holding
    nothing that is not recoverable from the tiers (live-session-context-design.md §0).

    **4. Epoch-fenced.** Each cohort carries a monotonic epoch bumped by every drop, so a render
    that was already in flight cannot ``put`` its pre-transition snapshot back afterwards.

    **The session-only lookup, and why it refuses instead of guessing.**
    ``WarmRecallCacheServicePort.last_rendered(session_id)`` (mu-core, ``manager.py:239``) can only
    pass a session id — ``MemoryLifecycleManager.ready_context(session_id)`` has the same
    session-only signature and cannot disambiguate either. So :meth:`get` accepts EITHER a full
    prefix or a bare session id (unambiguous by construction: ``Namespace`` forbids ``/`` in every
    component, ``memory.py:96``, so a session id can never look like a prefix). A bare session id
    is served only when it maps to exactly ONE live namespace; when two or more match it is
    REFUSED — cold, with a named (throttled) ``HostInjectionSkipped``, never a coin-flip between
    tenants. Widening the port to carry ``ns`` is the real fix and is reported as a mu-core delta.
    """

    def __init__(
        self,
        *,
        max_entries: int,
        clock: Clock,
        hot_session_ttl_s: int,
        notices: _NoticeThrottle,
    ) -> None:
        if max_entries < 1:
            raise ValueError(f"warm_cache_max_entries must be >= 1, got {max_entries}")
        self._max_entries = max_entries
        self._clock = clock
        self._hot_session_ttl_s = hot_session_ttl_s
        self._notices = notices
        self._entries: OrderedDict[str, _CacheEntry] = OrderedDict()
        # Three indexes kept in lockstep with ``_entries`` by put/pop/eviction, so the ambiguity
        # check and the cohort drop are exact and no code path has to re-parse the prefix string
        # to recover a session (``to_prefix()``'s layout is mu-core's to change, not ours to
        # depend on).
        self._by_session: dict[str, set[str]] = {}
        self._by_user: dict[str, set[str]] = {}
        self._key_session: dict[str, str] = {}
        self._key_user: dict[str, str] = {}
        # Monotonic per-cohort fence. A cohort's epoch keeps rising even while it holds no entries:
        # a render in flight across an invalidation of a cohort that went empty must NOT see the
        # same number on both sides of the drop. Bounded by the same LRU discipline as the entries
        # — and because pruning an entry would otherwise reset that cohort to 0 (a number an
        # in-flight render may already be holding), a pruned epoch is folded into ``_floor``, the
        # value every UNKNOWN cohort reports. The fence therefore never goes backwards, which is
        # the only property it has to have.
        self._epochs: OrderedDict[str, int] = OrderedDict()
        self._floor = 0

    def __len__(self) -> int:
        return len(self._entries)

    def __contains__(self, key: str) -> bool:
        return self._resolve(key) is not None

    @property
    def max_entries(self) -> int:
        return self._max_entries

    # ------------------------------------------------------------------------------ the fence
    def epoch(self, ns: Namespace) -> int:
        """The cohort's current epoch — read by :meth:`RecallInjectBridge.render` BEFORE it issues
        a recall, and handed back to :meth:`put`."""
        return self._epoch_of(_user_prefix(ns))

    def _epoch_of(self, cohort: str) -> int:
        return max(self._epochs.get(cohort, self._floor), self._floor)

    def _bump(self, cohort: str) -> None:
        self._epochs[cohort] = self._epoch_of(cohort) + 1
        self._epochs.move_to_end(cohort)
        while len(self._epochs) > self._max_entries:
            _, pruned = self._epochs.popitem(last=False)
            self._floor = max(self._floor, pruned)

    # ------------------------------------------------------------------------------ the reads
    def _resolve(self, key: str) -> str | None:
        """``key`` -> the full-prefix key holding its entry, or ``None``. A full prefix resolves to
        itself; a bare session id resolves only when EXACTLY one namespace holds an entry for it."""
        if key in self._entries:
            return key
        keys = self._by_session.get(key)
        if keys is None or len(keys) != 1:
            return None
        return next(iter(keys))

    def entry(self, key: str) -> _CacheEntry | None:
        """The warm entry for ``key``, or ``None`` (cold / ambiguous / past
        ``hot_session_ttl_s``). Enforcing the hard TTL HERE, on the read, is deliberate: there is
        no sweeper thread in a sync warm-read design, so expiry must be a property of reading."""
        resolved = self._resolve(key)
        if resolved is None:
            holders = self._by_session.get(key)
            if holders is not None and len(holders) > 1:
                # Ambiguous session id across namespaces: serve COLD rather than another tenant's
                # body. Named + content-free (the session id is a routing key, never memory
                # content) — the closed ``DegradeReason`` union has no warm-cache-scope member yet
                # (reported), and ``HostInjectionSkipped`` is the event that already means exactly
                # "nothing was injected, and here is why". Throttled: see ``_NoticeThrottle``.
                now = self._clock.now()
                if self._notices.allow(f"ambiguous:{key}", now):
                    log_host_injection_skipped(session_id=key, reason="warm_cache_scope_ambiguous")
            return None
        found = self._entries[resolved]
        if self.age_s(found) > self._hot_session_ttl_s:
            # HARD ceiling (recall-service-design.md §8: the TTL is the read-after-revoke bound).
            # Past it the entry is not "stale", it is GONE — an event-free removal upstream (STM
            # TTL expiry, ``facade.delete``, another process) can never be outlived.
            self._drop(resolved)
            _log.info(
                "inject.warm_cache.evicted",
                reason="hot_session_ttl_expired",
                hot_session_ttl_s=self._hot_session_ttl_s,
                size=len(self._entries),
            )
            return None
        self._entries.move_to_end(resolved)
        return found

    def age_s(self, entry: _CacheEntry) -> float:
        return (self._clock.now() - entry.rendered.computed_at).total_seconds()

    def get(self, key: str) -> RenderedContext | None:
        entry = self.entry(key)
        return entry.rendered if entry is not None else None

    # ----------------------------------------------------------------------------- the writes
    def put(
        self, ns: Namespace, rendered: RenderedContext, *, query: str | None, epoch: int
    ) -> bool:
        """Store ``rendered`` — UNLESS the cohort's epoch moved while the render was in flight, in
        which case the write is REFUSED (``False``) because it would resurrect a pre-transition
        body. Returns whether the entry was stored."""
        cohort = _user_prefix(ns)
        if self._epoch_of(cohort) != epoch:
            return False
        key = ns.to_prefix()
        self._entries[key] = _CacheEntry(rendered=rendered, query=query)
        self._entries.move_to_end(key)
        self._by_session.setdefault(ns.session, set()).add(key)
        self._by_user.setdefault(cohort, set()).add(key)
        self._key_session[key] = ns.session
        self._key_user[key] = cohort
        while len(self._entries) > self._max_entries:
            evicted_key, _ = self._entries.popitem(last=False)
            self._unindex(evicted_key)
            # Operator-only, content-free: the key itself is η routing metadata and the body is
            # never logged. No ``DegradeReason`` is emitted — an LRU drop is the designed steady
            # state of a bounded cache, not a degrade (the next pull renders fresh).
            _log.info(
                "inject.warm_cache.evicted",
                reason="lru_bound",
                max_entries=self._max_entries,
                size=len(self._entries),
            )
        return True

    def pop(self, ns: Namespace) -> bool:
        """Drop exactly ONE namespace's entry (the cold-render path). Bumps the cohort epoch, so a
        concurrent render cannot put a body across the drop. Never falls back to a session-only
        match: an invalidation that guessed which tenant to evict would be as wrong as a read that
        did."""
        self._bump(_user_prefix(ns))
        return self._drop(ns.to_prefix())

    def pop_cohort(self, ns: Namespace) -> int:
        """Drop EVERY entry of ``ns``'s user-prefix cohort — the federation grain (module
        docstring). Returns how many entries went. One epoch bump covers the whole cohort."""
        cohort = _user_prefix(ns)
        self._bump(cohort)
        dropped = 0
        for key in list(self._by_user.get(cohort, ())):
            if self._drop(key):
                dropped += 1
        return dropped

    def _drop(self, key: str) -> bool:
        if self._entries.pop(key, None) is None:
            return False
        self._unindex(key)
        return True

    def _unindex(self, key: str) -> None:
        session = self._key_session.pop(key, None)
        if session is not None:
            holders = self._by_session.get(session)
            if holders is not None:
                holders.discard(key)
                if not holders:
                    del self._by_session[session]
        cohort = self._key_user.pop(key, None)
        if cohort is not None:
            members = self._by_user.get(cohort)
            if members is not None:
                members.discard(key)
                if not members:
                    del self._by_user[cohort]


class RecallInjectBridge:
    def __init__(
        self,
        host: LocalMemoryHost,
        *,
        settings: InjectSettings,
        recall_dir: Path | None = None,
        bus: EventBusPort | None = None,
        clock: Clock | None = None,
    ) -> None:
        self._host = host
        self._settings = settings
        # DEV-STANDARDS §1.1: no bare literal default — falls back to InjectSettings.recall_dir
        # (env: MU_INJECT__RECALL_DIR) when a caller (tests, a future override) doesn't pass one.
        self._recall_dir = (recall_dir or settings.recall_dir).expanduser()
        self._clock: Clock = clock or SystemClock()
        self._notices = _NoticeThrottle(
            interval_s=settings.warm_cache_notice_interval_s,
            max_keys=settings.warm_cache_max_entries,
        )
        # The warm cache proper — namespace-keyed, cohort-invalidated, LRU- and TTL-bounded,
        # epoch-fenced (see ``_ScopedRenderCache``). Every bound is configured, never a literal.
        self._last_rendered = _ScopedRenderCache(
            max_entries=settings.warm_cache_max_entries,
            clock=self._clock,
            hot_session_ttl_s=settings.hot_session_ttl_s,
            notices=self._notices,
        )
        self._subscriptions: list[Subscription] = []
        # Strong refs to in-flight push refreshes. Without the set, ``asyncio`` only weakly
        # references a bare ``create_task`` result and the loop may collect it mid-render
        # (ruff RUF006); it is also what :meth:`aclose`/:meth:`drain_refreshes` act on.
        self._refresh_tasks: set[asyncio.Task[None]] = set()
        # Coalescing state, mirroring ``lifecycle/session_save.py:106-181``: at most ONE in-flight
        # refresh per namespace + at most one pending re-run, so a per-item sweep cannot fan out.
        self._inflight: set[str] = set()
        self._pending: set[str] = set()
        self._refresh_query: dict[str, str | None] = {}
        #: Observability for the coalescing, so a test/operator can assert it rather than infer it.
        self.coalesced_count = 0
        # ...and a hard cap on how many DISTINCT namespaces may hold a real three-arm recall open
        # at once. The refreshes run on the SAME default ThreadPoolExecutor mu-engine's embedder
        # uses (``providers/embedding.py:50`` ``asyncio.to_thread``), so an uncapped burst steals
        # the capture path's own ack latency.
        self._refresh_slots = asyncio.Semaphore(settings.warm_cache_refresh_concurrency)
        if bus is not None:
            self._subscribe(bus)

    # ------------------------------------------------------------------------- bus subscription
    def _subscribe(self, bus: EventBusPort) -> None:
        """Wires this bridge into the SAME real ``InprocBus`` the engine's ingest/promotion/
        demotion/retention/distill/pin paths publish onto (``LocalMemoryHost.bus`` ->
        ``LocalMemory.bus`` -> ``LocalContainer.bus`` — the identical accessor ``MaintenanceLoop``
        subscribes, ``host.py:155-160``), never a second, dark bus. Mirrors
        ``MaintenanceLoop._subscribe``'s idempotent-handle-list shape (``maintenance.py:184-190``).

        Subscribes to EVERY memory-mutating event type on this bus (``_MutationEvent``), not the
        four tier ones: an event type that mutates memory and is not subscribed is, by
        construction, a body this cache will keep serving after it stopped being true."""
        self._subscriptions = [
            bus.subscribe(MemoryPromoted, self._on_mutation),
            bus.subscribe(MemoryDemoted, self._on_mutation),
            bus.subscribe(MemoryGarbageCollected, self._on_mutation),
            bus.subscribe(ConsolidationCompleted, self._on_mutation),
            bus.subscribe(MemorySuperseded, self._on_mutation),
            bus.subscribe(MemoryQuarantined, self._on_mutation),
            bus.subscribe(MemoryPinned, self._on_mutation),
            bus.subscribe(MemoryUnpinned, self._on_mutation),
            bus.subscribe(MemoryCaptured, self._on_mutation),
        ]

    async def aclose(self) -> None:
        """Unsubscribes from the bus, then cancels + collects any in-flight push refresh. Called by
        the daemon's ordered shutdown (``daemon/app.py``) BEFORE ``host.aclose()``, so no refresh is
        still reaching into store adapters that are being torn down. Unsubscribe happens FIRST so
        no new refresh can be scheduled while we are draining. A no-op when no ``bus`` was ever
        wired (``self._subscriptions`` stays empty, PULL-only degrade, acceptance §3)."""
        for sub in self._subscriptions:
            await sub.unsubscribe()
        self._subscriptions = []
        self._pending.clear()
        tasks = list(self._refresh_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def drain_refreshes(self) -> None:
        """Await every in-flight push refresh to completion (never cancels). The deterministic
        counterpart to :meth:`aclose` for a caller that wants the cache fully warm before reading
        it — a graceful flush, and the seam a test uses instead of sleeping."""
        while self._refresh_tasks:
            await asyncio.gather(*list(self._refresh_tasks), return_exceptions=True)

    async def _on_mutation(self, event: _MutationEvent) -> None:
        """Every memory-mutating event -> invalidate the affected cohort NOW; re-warm in the
        background (spec §12). Split deliberately (module docstring):

        1. **Invalidate SYNCHRONOUSLY and unconditionally.** :meth:`invalidate` is a set of dict
           pops over the event's own user-prefix cohort, runs before anything that can raise, and
           cannot fail — so a real mutation can never leave a superseded body behind, whatever
           happens to the refresh. This is the half that makes the cache correct.
        2. **Refresh in the BACKGROUND, coalesced.** ``InprocBus.publish`` awaits each handler
           inline (``bus_inproc.py:59-60``), i.e. on the publisher's own stack — which for
           ``MemoryPromoted`` is ``IngestService``'s capture path (``ingest.py:414``). Awaiting a
           full three-arm recall there would put embed + Qdrant + FalkorDB latency on the capture
           ack and let a slow store stall ingest. The scheduled task only re-warms, and one
           namespace never holds more than one of them.

        NEVER propagates: ``InprocBus.publish`` re-raises a handler's exception to ITS caller
        ("fail-loud, no silent swallow"), i.e. straight back into whatever real engine service just
        published — an unhandled exception here would break a REAL promotion, not merely this
        bridge. Nothing in this method's own body raises; the refresh body catches broadly and
        degrades via the existing ``RECALL_CORE_DOWN`` reason (acceptance §3: no new
        ``DegradeReason``), never a bare swallow (DEV-STANDARDS rule 8)."""
        ns = event.namespace
        # The query the affected body was ranked against, read BEFORE the drop takes it away, so
        # the re-warm re-ranks for the user's real prompt rather than for the session id.
        entry = self._last_rendered.entry(ns.to_prefix())
        query = entry.query if entry is not None else None
        self.invalidate(ns)
        if not isinstance(event, _REFRESH_EVENTS):
            return  # MemoryCaptured: invalidate-only by design (see ``_REFRESH_EVENTS``).
        if not self._renderable(ns):
            # A namespace this host cannot render (a foreign org/workspace, or a SHARED room —
            # ``LocalMemory`` is private-plane-only by construction, local_memory.py:12). The
            # invalidate above still ran, which is the part that matters; there is simply nothing
            # to re-warm. Content-free: a reason, no η values, no body.
            _log.info("inject.warm_cache.refresh_skipped", reason="not_this_hosts_partition")
            return
        self._schedule_refresh(ns, query=query)

    def _schedule_refresh(self, ns: Namespace, *, query: str | None) -> bool:
        """Start a background re-warm for ``ns``, or fold this request into the one already
        running. Returns whether a NEW task was created (``False`` = coalesced) — the same
        contract, and the same reason, as ``SessionSaveTrigger.trigger``
        (``lifecycle/session_save.py:167-181``): the publisher does not await the work, so the
        only way to bound it is to refuse to start a second one."""
        key = ns.to_prefix()
        self._refresh_query[key] = query
        if key in self._inflight:
            self._pending.add(key)
            self.coalesced_count += 1
            # Content-free: a count and a reason; no η values, no body, no query text.
            _log.info("inject.warm_cache.refresh_coalesced", coalesced_count=self.coalesced_count)
            return False
        self._inflight.add(key)
        task = asyncio.create_task(self._refresh_loop(ns), name="mu.inject.warm_cache.refresh")
        self._refresh_tasks.add(task)
        task.add_done_callback(self._refresh_tasks.discard)
        return True

    async def _refresh_loop(self, ns: Namespace) -> None:
        """One refresh, then ONE more if events arrived while it ran — never one per event. The
        re-check and the ``_inflight`` release below contain no ``await`` between them, so an event
        arriving after the last check always sees ``_inflight`` clear and schedules a fresh task:
        no lost wakeup, and no unbounded loop either."""
        key = ns.to_prefix()
        try:
            while True:
                await self._refresh(ns, query=self._refresh_query.get(key))
                if key not in self._pending:
                    return
                self._pending.discard(key)
        finally:
            self._inflight.discard(key)
            self._refresh_query.pop(key, None)

    async def _refresh(self, ns: Namespace, *, query: str | None) -> None:
        """Re-render ``ns`` against the REAL host so the cache reflects the just-applied mutation.
        One reused code path (:meth:`render`) serves both "refresh" and "invalidate": a genuine
        ``LocalMemoryHost.recall()`` reflects whichever way the transition went (a promoted fact
        starts appearing; a demoted/GC'd one stops), so there is no second rendering implementation
        to keep in sync. ``user=ns.user`` — the EVENT's principal, never this daemon's
        ``default_user``: rendering the default user's partition in response to another principal's
        transition would be both wrong and wasted work."""
        try:
            async with self._refresh_slots:
                await self.render(ns.session, query=query, user=ns.user)
        except asyncio.CancelledError:
            raise  # DEV-STANDARDS rule 1: cancellation is never swallowed as a failure
        except Exception as exc:
            log_degraded(
                component="inject",
                mode="bus_refresh_failed",
                reason=DegradeReason.RECALL_CORE_DOWN,
                detail=f"{type(exc).__name__}: {exc}",
            )

    def _renderable(self, ns: Namespace) -> bool:
        """Can THIS host render ``ns``? ``LocalMemoryHost`` is bound to one org/workspace at
        construction (``host.py:105-112`` -> ``LocalMemory(workspace=…, namespace=…)``) and
        ``LocalMemory`` is PRIVATE-plane-only, so anything else is not ours to warm."""
        client = self._host.settings
        return (
            ns.visibility is Visibility.PRIVATE
            and ns.org == client.default_namespace
            and ns.workspace == client.default_workspace
        )

    def _namespace(self, session_id: str, user: str | None) -> Namespace:
        """The η this bridge renders under — the SAME six slots ``LocalMemory._ns``
        (``local_memory.py:516-524``) builds for the recall this render is about to run, from the
        SAME ``ClientSettings`` the host was constructed from (``host.py:105-112``: ``namespace``
        fills ``η.org``, ``workspace`` fills ``η.workspace``). Reconstructed here rather than read
        back off the result because the cache key has to exist BEFORE the recall (the stale/cold
        fallback needs it when the recall raises). mu-core exposes no public η builder on the host
        to reuse — reported as a DRY delta, not silently forked."""
        client = self._host.settings
        return Namespace(
            org=client.default_namespace,
            workspace=client.default_workspace,
            user=user or client.default_user,
            session=session_id,
            visibility=Visibility.PRIVATE,
        )

    # ------------------------------------------------------- WarmRecallCacheServicePort (sync)
    def invalidate(self, ns: Namespace) -> None:
        """``WarmRecallCacheServicePort`` required method 1 (``manager.py:237``). Drops every warm
        body that could have SOURCED a memory from ``ns`` — i.e. ``ns``'s whole user-prefix cohort,
        because the ``recalled`` zone federates across all of one user's sessions (module
        docstring). Plain ``def`` and O(entries-in-cohort), bounded by ``warm_cache_max_entries``:
        the manager's ``get_state``/``ready_context`` are synchronous "instant warm read" methods
        by spec §5 and nothing on this seam may await store I/O."""
        self._last_rendered.pop_cohort(ns)

    def last_rendered(self, session_id: str) -> str | None:
        """``WarmRecallCacheServicePort`` required method 2 (``manager.py:239``) —
        :meth:`~mu_engine.lifecycle.manager.MemoryLifecycleManager.ready_context`'s real
        synchronous read. Returns the warm body for ``session_id``, or ``None`` when nothing has
        been rendered for it yet (cold), when the entry aged past ``hot_session_ttl_s``, **or when
        the session id maps to more than one namespace** — see :class:`_ScopedRenderCache`:
        ambiguity is refused, never guessed. A body older than ``stale_after_s`` is returned WITH
        the named stale marker rather than silently as current. Never triggers a render itself:
        :meth:`render` (PULL) and :meth:`_refresh` (PUSH) are the only writers."""
        return self._warm_body(session_id)

    def last_rendered_for(self, ns: Namespace) -> str | None:
        """The unambiguous, namespace-addressed warm read — what
        ``WarmRecallCacheServicePort.last_rendered`` SHOULD take (reported as a mu-core delta:
        widening the port and ``ready_context`` to carry ``ns`` removes the ambiguity case
        entirely). Every in-repo caller that HAS an ``η`` should prefer this over
        :meth:`last_rendered`."""
        return self._warm_body(ns.to_prefix())

    def _warm_body(self, key: str) -> str | None:
        """The ONE synchronous warm read both port methods go through — including the age check,
        so no caller can accidentally read around it. recall-service-design.md §2.4: ``fresh``/
        ``stale`` -> body (``stale`` adds the one-line marker + a named
        ``DegradedModeEntered(mode="serve_stale_with_marker", reason=STALE_INJECTION)``); past the
        hard TTL the entry is already gone (``_ScopedRenderCache.entry``) and the read is cold.
        The degrade emit is throttled — this runs on the ``/ready-context`` request path."""
        entry = self._last_rendered.entry(key)
        if entry is None:
            return None
        age_s = self._last_rendered.age_s(entry)
        if age_s <= self._settings.stale_after_s:
            return entry.rendered.body
        if self._notices.allow(f"stale:{key}", self._clock.now()):
            log_degraded(
                component="inject",
                mode="serve_stale_with_marker",
                reason=DegradeReason.STALE_INJECTION,
                detail=f"age_s={int(age_s)} stale_after_s={self._settings.stale_after_s}",
            )
        return f"{entry.rendered.body}\n(memory may be {int(age_s)} s old)"

    # --------------------------------------------------------------------------------- render
    async def render(
        self, session_id: str, *, query: str | None = None, user: str | None = None
    ) -> RenderedContext:
        """``fresh``/``stale`` -> body (+ a stale marker); ``cold`` -> empty body +
        :func:`log_host_injection_skipped` — NEVER hangs/blanks the host turn on a genuine failure.
        The ``recalled`` zone is sourced across every session of ``user`` via the existing
        ``session_scope=None`` federation (module docstring); ``user`` defaults to the host's
        configured principal and is set explicitly only by the push path, for the EVENT's own
        principal.

        ``query`` is what the hits are RANKED against, and it is remembered with the entry: a
        query-less caller (the MCP silent resource; a re-warm with no recorded prompt) re-uses the
        last real prompt for that namespace before falling back to the session id."""
        ns = self._namespace(session_id, user)
        key = ns.to_prefix()
        cached = self._last_rendered.entry(key)
        effective_query = query or (cached.query if cached is not None else None) or session_id
        # Read the fence BEFORE the I/O: everything after this point is racing the bus.
        epoch = self._last_rendered.epoch(ns)
        try:
            listing = await self._host.recall(
                effective_query, user=ns.user, session=session_id, limit=self._settings.top_k
            )
        except MemoryUniverseError as exc:
            return self._fallback_or_cold(ns, reason=str(exc))
        # Gap D (AGENT-INTEGRATION-AUDIT-AND-PLAN §4 Phase 2): render DISTILLED context, not the raw
        # session STM dump — drop tool-capture/output noise (``Write:``/``Bash:``…), dedupe by
        # content, and sink the query-insensitive STM recency-floor below the ranked hits. One
        # deterministic pass (:func:`~mu_client.inject.distill.distill_items`) shared by BOTH this
        # hook-inject path and the MCP silent resource, so both surfaces emit one distilled view.
        items = distill_items(listing.items)
        body = "\n".join(f"- {item.content}" for item in items)
        now = self._clock.now()
        if not body:
            log_host_injection_skipped(session_id=session_id, reason="cold_cache")
            self._last_rendered.pop(ns)
            return RenderedContext(
                session_id=session_id,
                body="",
                etag=_etag(""),
                computed_at=now,
                staleness="cold",
            )
        rendered = self._budget(ns, body, now=now)
        if self._last_rendered.put(ns, rendered, query=effective_query, epoch=epoch):
            return rendered
        # THE FENCE FIRED: a mutation landed while this render was awaiting store I/O, so this body
        # predates it. It is NOT cached (that would resurrect what the event removed) and it is NOT
        # handed back as ``fresh`` — the caller gets the honest named degrade instead. A read that
        # overlaps a write is at most one transition behind for that one turn; a CACHE that does is
        # behind forever.
        log_degraded(
            component="inject",
            mode="render_raced_invalidation",
            reason=DegradeReason.STALE_INJECTION,
        )
        return rendered.model_copy(
            update={"body": rendered.body + _RACED_NOTE, "staleness": "stale"}
        )

    def _fallback_or_cold(self, ns: Namespace, *, reason: str) -> RenderedContext:
        cached = self._last_rendered.get(ns.to_prefix())
        log_degraded(
            component="inject",
            mode="recall_core_down",
            reason=DegradeReason.RECALL_CORE_DOWN,
            detail=reason,
        )
        if cached is None:
            log_host_injection_skipped(session_id=ns.session, reason="cold_cache")
            return RenderedContext(
                session_id=ns.session,
                body="",
                etag=_etag(""),
                computed_at=self._clock.now(),
                staleness="cold",
            )
        log_degraded(
            component="inject",
            mode="stale_snapshot_served",
            reason=DegradeReason.STALE_INJECTION,
        )
        stale_note = "\n(memory may be stale — engine unreachable)"
        # ``computed_at`` is deliberately NOT refreshed: this body was computed when it was
        # computed, and re-stamping it would hide its age from every downstream age check.
        return cached.model_copy(update={"body": cached.body + stale_note, "staleness": "stale"})

    def _budget(self, ns: Namespace, body: str, *, now: datetime) -> RenderedContext:
        session_id = ns.session
        budget = self._settings.body_budget_chars
        if len(body) <= budget:
            return RenderedContext(
                session_id=session_id,
                body=body,
                etag=_etag(body),
                computed_at=now,
                staleness="fresh",
            )
        # F4: over-budget spills to a file + preview — named degrade, NEVER a silent truncate.
        # The filename carries an η digest, not the bare session id: the spill file holds real
        # MEMORY CONTENT, and a session-only name let two principals sharing a session id
        # overwrite — and read — each other's spilled bodies at a predictable path (same tenancy
        # defect as the cache key, with a worse blast radius because it is on disk).
        self._recall_dir.mkdir(parents=True, exist_ok=True)
        scope_digest = _etag(ns.to_prefix())[:_SPILL_DIGEST_CHARS]
        spill_path = self._recall_dir / f"{session_id}-{scope_digest}.txt"
        spill_path.write_text(body, encoding="utf-8")
        note = f"\n… (full context spilled to {spill_path})"
        preview = body[: budget - len(note)] + note
        # No dedicated "inject body over F4 budget" reason exists yet in the closed DegradeReason
        # union (capture-spec.md §7.2 names the degrade but not a reason id) — ARTIFACT_HYDRATION_
        # BUDGET is the nearest existing budget-family reason; a proper reason addition routes
        # through the Apply phase per the specs' own "Contract changes" convention.
        log_degraded(
            component="inject",
            mode="body_over_budget_file_spill",
            reason=DegradeReason.ARTIFACT_HYDRATION_BUDGET,
            detail=f"chars={len(body)} budget={budget} spill_path={spill_path}",
        )
        return RenderedContext(
            session_id=session_id,
            body=preview,
            etag=_etag(body),
            computed_at=now,
            staleness="fresh",
        )


_RACED_NOTE = "\n(memory may be stale — a lifecycle transition landed during this render)"


def _etag(body: str) -> str:
    return hashlib.sha256(body.encode("utf-8")).hexdigest()
