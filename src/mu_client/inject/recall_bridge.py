"""``RecallInjectBridge`` — the pull-companion of capture (capture-spec.md §7.2). Renders a
:class:`RenderedContext` the hook client reads (``GET /recall/{session}``) and emits verbatim as
``additionalContext`` (host-capture-integration-devdoc.md §2.1/§5.3).

**Deviation (recorded).** capture-spec.md's ``WarmRecallCacheService`` PRE-renders on every
``MemoryCaptured`` (push, ahead of the next prompt) via a live LOCAL event-bus subscription — this
stage has no concrete ``EventBusPort`` adapter (see ``observability/events.py``'s docstring), so
this bridge renders PULL, on each ``GET /recall/{session}`` call, directly against
``LocalMemoryHost.recall`` (real stores, no staleness from a missed bus event). The
``staleness``/courtesy-cache contract (fresh/stale/cold, F4 budget, never-blank-the-host) is
still honored in full — only the "ahead of the prompt" latency-hiding optimization is deferred.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import structlog
from mu_contracts.domain.errors import MemoryUniverseError
from mu_contracts.domain.events import DegradeReason
from pydantic import BaseModel, ConfigDict

from mu_client.config import InjectSettings
from mu_client.host import LocalMemoryHost
from mu_client.observability.events import log_degraded, log_host_injection_skipped

__all__ = ["RecallInjectBridge", "RenderedContext"]

_log = structlog.get_logger("mu.client.inject")


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
    ) -> None:
        self._host = host
        self._settings = settings
        # DEV-STANDARDS §1.1: no bare literal default — falls back to InjectSettings.recall_dir
        # (env: MU_INJECT__RECALL_DIR) when a caller (tests, a future override) doesn't pass one.
        self._recall_dir = (recall_dir or settings.recall_dir).expanduser()
        self._last_rendered: dict[str, RenderedContext] = {}  # courtesy cache for stale fallback

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
