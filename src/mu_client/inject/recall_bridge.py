"""``RecallInjectBridge`` — the pull-companion of capture (capture-spec.md §7.2), PROMOTED (S3-02,
spec §12 / CANONICAL §7.22) into the ``WarmRecallCacheService`` role: ONE owner, not a second,
competing service. Renders a :class:`RenderedContext` the hook client reads (``GET /recall/
{session}``) and emits verbatim as ``additionalContext`` (host-capture-integration-devdoc.md
§2.1/§5.3).

**PULL path (unchanged).** Each ``GET /recall/{session}`` call renders directly against
``LocalMemoryHost.recall`` (real stores) — the ``staleness``/courtesy-cache contract (fresh/stale/
cold, F4 budget, never-blank-the-host) is honored in full, exactly as before this task.

**PUSH path (this task, spec §12 lines 397-404).** When a real ``EventBusPort`` (the LOCAL
``InprocBus``) is threaded in via the optional ``bus=`` constructor kwarg, this bridge ALSO
subscribes to the MLM's own tier-transition events — ``MemoryPromoted``/``ConsolidationCompleted``
(refresh: "recall writes the recalled zone ahead of the prompt" / "roll the running summary
forward") and ``MemoryDemoted``/``MemoryGarbageCollected`` (invalidate: "a demoted/GC'd fact stops
being injected") — and re-renders the affected session's ``_last_rendered`` entry on the SAME tick
a real transition happens, never waiting for the next pull. One code path (:meth:`render` itself,
reused verbatim) satisfies both "refresh" and "invalidate": a genuine ``LocalMemoryHost.recall()``
against the real stores reflects whichever way the transition went. ``bus=None`` (e.g. a
daemonless/one-shot caller with no ``InprocBus`` wired) degrades to the pre-existing PULL-only
behavior with NO new ``DegradeReason`` — the existing ``RECALL_CORE_DOWN``/``STALE_INJECTION``
reasons already cover "the bus subscription itself is unavailable."

**``WarmRecallCacheServicePort`` conformance (structural, additive).** ``mu_engine.lifecycle.
manager.MemoryLifecycleManager`` already carries an optional ``warm_cache:
WarmRecallCacheServicePort | None`` seam (``manager.py:224-228``, single required method
``invalidate(ns) -> None``) that no constructor call site threads a concrete instance through yet
(module docstring: "``ready_context`` stays a documented stub until S3-02 lands").
:meth:`invalidate` below satisfies that Protocol structurally (PEP 544 — no import of
``manager.py`` needed here) so a future integrate-phase wiring can pass ``warm_cache=bridge`` into
``MemoryLifecycleManager`` instead of building a second warm-cache service — this task's canonical
rule (CANONICAL §7.22 ONE owner).
That wiring (and ``ready_context`` reading real ``_last_rendered`` state) is NOT this task's owned
file (``manager.py`` is S1-03's) — flagged for the integrate phase, see this module's own report.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import structlog
from mu_contracts.domain.errors import MemoryUniverseError
from mu_contracts.domain.events import (
    ConsolidationCompleted,
    DegradeReason,
    MemoryDemoted,
    MemoryGarbageCollected,
    MemoryPromoted,
)
from mu_contracts.domain.model.memory import Namespace
from mu_contracts.ports.bus import EventBusPort, Subscription
from pydantic import BaseModel, ConfigDict

from mu_client.config import InjectSettings
from mu_client.host import LocalMemoryHost
from mu_client.observability.events import log_degraded, log_host_injection_skipped

__all__ = ["RecallInjectBridge", "RenderedContext"]

_log = structlog.get_logger("mu.client.inject")

# The MLM tier-transition events this bridge reacts to (spec §12 lines 397-404). All four carry a
# structural ``namespace: Namespace`` field — the ONLY field this bridge's handler reads (no
# ``isinstance`` narrowing needed beyond what ``EventBusPort.subscribe``'s per-type dispatch
# already guarantees at each individual ``subscribe()`` call site below).
_TierEvent = MemoryPromoted | MemoryDemoted | MemoryGarbageCollected | ConsolidationCompleted


class RenderedContext(BaseModel, frozen=True):
    """capture-spec.md §7.2 shape, verbatim."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    session_id: str
    body: str
    etag: str
    staleness: str  # "fresh" | "stale" | "cold"


class RecallInjectBridge:
    def __init__(
        self,
        host: LocalMemoryHost,
        *,
        settings: InjectSettings,
        recall_dir: Path | None = None,
        bus: EventBusPort | None = None,
    ) -> None:
        self._host = host
        self._settings = settings
        # DEV-STANDARDS §1.1: no bare literal default — falls back to InjectSettings.recall_dir
        # (env: MU_INJECT__RECALL_DIR) when a caller (tests, a future override) doesn't pass one.
        self._recall_dir = (recall_dir or settings.recall_dir).expanduser()
        self._last_rendered: dict[str, RenderedContext] = {}  # courtesy cache for stale fallback
        self._subscriptions: list[Subscription] = []
        if bus is not None:
            self._subscribe(bus)

    # ------------------------------------------------------------------------- bus subscription
    def _subscribe(self, bus: EventBusPort) -> None:
        """Wires this bridge into the SAME real ``InprocBus`` MLM's promotion/demotion/distill
        publish onto (``LocalMemoryHost.bus`` -> ``LocalMemory.bus`` -> ``LocalContainer.bus`` —
        the identical accessor ``MaintenanceLoop`` subscribes, ``host.py:161-167``), never a
        second, dark bus. Mirrors ``MaintenanceLoop._subscribe``'s idempotent-handle-list shape
        (``maintenance.py:184-190``)."""
        self._subscriptions = [
            bus.subscribe(MemoryPromoted, self._on_tier_event),
            bus.subscribe(MemoryDemoted, self._on_tier_event),
            bus.subscribe(MemoryGarbageCollected, self._on_tier_event),
            bus.subscribe(ConsolidationCompleted, self._on_tier_event),
        ]

    async def aclose(self) -> None:
        """Unsubscribes from the bus — the integrate-phase daemon shutdown calls this (mirrors
        ``MaintenanceLoop._unsubscribe``, ``maintenance.py:192-197``). A no-op when no ``bus`` was
        ever wired (``self._subscriptions`` stays empty, PULL-only degrade, acceptance §3)."""
        for sub in self._subscriptions:
            await sub.unsubscribe()
        self._subscriptions = []

    async def _on_tier_event(self, event: _TierEvent) -> None:
        """``MemoryPromoted``/``ConsolidationCompleted`` -> refresh; ``MemoryDemoted``/
        ``MemoryGarbageCollected`` -> invalidate (spec §12). Both resolve to the exact SAME
        action — re-render the affected session against the REAL host on this same tick — because
        a genuine ``LocalMemoryHost.recall()`` already reflects whichever way the just-applied
        tier transition went (a promoted fact starts appearing; a demoted/GC'd one stops), so one
        reused code path (:meth:`render`) satisfies "refresh" and "invalidate" identically; no
        second rendering implementation to keep in sync.

        NEVER propagates: ``InprocBus.publish`` fires handlers synchronously and re-raises a
        handler's own exception to ITS caller (``bus_inproc.py:59-60`` — "fail-loud, no silent
        swallow"), i.e. straight back into whatever real MLM service just published the tier
        event. An unhandled exception here would therefore break a REAL promotion/demotion/
        consolidate call, not merely this bridge — caught broadly and degraded via the existing
        ``RECALL_CORE_DOWN`` reason (acceptance §3: no new ``DegradeReason``), never a bare
        swallow (DEV-STANDARDS rule 8)."""
        session_id = event.namespace.session
        try:
            await self.render(session_id)
        except Exception as exc:  # see docstring: must never reach the publisher (fail-loud bus)
            log_degraded(
                component="inject",
                mode="bus_refresh_failed",
                reason=DegradeReason.RECALL_CORE_DOWN,
                detail=f"{type(exc).__name__}: {exc}",
            )

    def invalidate(self, ns: Namespace) -> None:
        """Structural ``WarmRecallCacheServicePort`` conformance (``manager.py:224-228`` — one of
        the two required methods, PEP 544, no import needed here) — the synchronous seam
        integrate-phase wiring passes this bridge into ``MemoryLifecycleManager(warm_cache=...)``
        through, rather than building a second, competing warm-cache service (this task's own
        canonical rule, CANONICAL §7.22). Pops the affected session's courtesy-cache entry so the
        NEXT render is forced fresh against the real host; the bus subscription above is this
        bridge's own primary, tested push-refresh path when a real ``bus`` is wired."""
        self._last_rendered.pop(ns.session, None)

    def last_rendered(self, session_id: str) -> str | None:
        """The other ``WarmRecallCacheServicePort`` required method (Stage-3 integrate) —
        :meth:`~mu_engine.lifecycle.manager.MemoryLifecycleManager.ready_context`'s real synchronous
        read. Returns this bridge's own courtesy-cache body for ``session_id``, or ``None`` if
        nothing has been rendered yet (cold — no ``GET /recall`` pull and no bus-driven refresh has
        touched this session). Never triggers a render itself: :meth:`render` (the PULL path) and
        :meth:`_on_tier_event` (the PUSH path) are the only writers of ``_last_rendered``."""
        cached = self._last_rendered.get(session_id)
        return cached.body if cached is not None else None

    async def render(self, session_id: str, *, query: str | None = None) -> RenderedContext:
        """``fresh``/``stale`` -> body (+ ``(memory may be N s old)`` marker on stale);
        ``cold`` -> empty body + :func:`log_host_injection_skipped` — NEVER hangs/blanks the host
        turn on a genuine failure."""
        try:
            listing = await self._host.recall(
                query or session_id, session=session_id, limit=self._settings.top_k
            )
        except MemoryUniverseError as exc:
            return self._fallback_or_cold(session_id, reason=str(exc))
        body = "\n".join(f"- {item.content}" for item in listing.items)
        if not body:
            log_host_injection_skipped(session_id=session_id, reason="cold_cache")
            rendered = RenderedContext(
                session_id=session_id, body="", etag=_etag(""), staleness="cold"
            )
            self._last_rendered.pop(session_id, None)
            return rendered
        rendered = self._budget(session_id, body)
        self._last_rendered[session_id] = rendered
        return rendered

    def _fallback_or_cold(self, session_id: str, *, reason: str) -> RenderedContext:
        cached = self._last_rendered.get(session_id)
        log_degraded(
            component="inject",
            mode="recall_core_down",
            reason=DegradeReason.RECALL_CORE_DOWN,
            detail=reason,
        )
        if cached is None:
            log_host_injection_skipped(session_id=session_id, reason="cold_cache")
            return RenderedContext(session_id=session_id, body="", etag=_etag(""), staleness="cold")
        log_degraded(
            component="inject",
            mode="stale_snapshot_served",
            reason=DegradeReason.STALE_INJECTION,
        )
        stale_note = "\n(memory may be stale — engine unreachable)"
        return cached.model_copy(update={"body": cached.body + stale_note, "staleness": "stale"})

    def _budget(self, session_id: str, body: str) -> RenderedContext:
        budget = self._settings.body_budget_chars
        if len(body) <= budget:
            return RenderedContext(
                session_id=session_id, body=body, etag=_etag(body), staleness="fresh"
            )
        # F4: over-budget spills to a file + preview — named degrade, NEVER a silent truncate.
        self._recall_dir.mkdir(parents=True, exist_ok=True)
        spill_path = self._recall_dir / f"{session_id}.txt"
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
            session_id=session_id, body=preview, etag=_etag(body), staleness="fresh"
        )


def _etag(body: str) -> str:
    return hashlib.sha256(body.encode("utf-8")).hexdigest()
