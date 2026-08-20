"""``SessionSaveTrigger`` — the two MISSING lifecycle triggers that consolidate STM upward.

**The problem this closes.** MU had exactly one routine path out of STM: the periodic
``MaintenanceLoop`` sweep, gated on salience (``PromotionService.promote_session``,
``S(m) >= promote_stm_mtm``). That gate is unreachable for an auto-captured turn. The implemented
salience is ``S = 0.5·recency + 0.2·usage + 0.3·importance``, so a freshly captured turn at the
default importance scores, at BEST::

    S = 0.5(1.0) + 0.2(0.0) + 0.3(0.5) = 0.650   against   promote_stm_mtm = 0.700

``0.650`` is the CEILING, not a typical value — recency is already 1.0 at capture and usage is 0 by
definition, and recency only decays from there. So auto-captured memory could never promote, never
reach the MTM->LTM distill, and stayed in session-partitioned STM forever. Live-reproduced: Claude
states a fact in session A through the real hooks; a brand-new Claude session asks for it and
answers "UNKNOWN".

**Why a score is the wrong gate here — the reference-repo evidence.** Of nine OSS memory systems in
``other_repos`` (mem0, letta/MemGPT, graphiti, MemoryBank, MemOS, A-mem, Memori, LightRAG,
HippoRAG), NOT ONE gates durability on a computed score. Where a score exists it governs
*forgetting* or *ranking*, never *admission*: mem0's extractor IS the filter ("Hi." -> ``{"facts":
[]}``, ``mem0/configs/prompts.py``) and anything extracted is durable immediately; letta promotes
via an explicit ``archival_memory_insert`` tool call; MemoryBank stores everything and reinforces
``memory_strength += 1`` on recall. MU is the only one using a number as a gate at the door.

**What this adds — event-driven consolidation, the way a buffer actually drains.** STM is a
BUFFER. A buffer consolidates on PRESSURE and on CLOSE, not only on a slow timer:

1. :meth:`on_session_end` — the session is over, so there is no "later" to be salient in. Same
   semantics as ``PreCompact``: save what is in the buffer before it is gone.
2. :meth:`on_capture` — capture PRESSURE. After ``consolidate_every_n`` captured turns in one
   session, drain that session's buffer upward. This is the "STM limit full" trigger, counted
   IN-PROCESS (never a per-ingest store round-trip).

Both reuse ``MemoryLifecycleManager.promote_session_now(ns, force=True)`` — the EXACT machinery
``PreCompactPromoter`` already drives, which force-promotes STM->MTM and hands MTM->LTM to
``DistillPipeline``. ``force=True`` bypasses the routine salience gate for the same reason
PreCompact does: a turn that is about to age out of a closed session has no second chance to become
salient. This is NOT a parallel promotion path (DEV-STANDARDS rule 6, DRY) — no new verb, no second
gate, no new store call.

The salience gate is deliberately LEFT ALONE. It still governs the routine periodic sweep, which is
the right place for a score: deciding what is worth *keeping*, not what is allowed *in*.

**Context limits are already handled** and need nothing here: ``ModelRouter.complete`` detects
``LongTextChunker.needs_chunking(...)`` and transparently runs ``map_reduce`` over a small-context
model (``providers/model_router.py``), so the extractor a consolidation drives inherits chunking for
free regardless of how large the drained window is.
"""

from __future__ import annotations

import structlog
from mu_contracts.domain.model.memory import Namespace, Visibility

from mu_client.capture.model import ActivityKind, RawActivity
from mu_client.lifecycle.precompact import SessionPromoterPort

__all__ = ["SessionSaveTrigger"]

_log = structlog.get_logger(__name__)

#: Captured turns in ONE session before its STM buffer is drained upward. Named, not a literal
#: (DEV-STANDARDS rule 3), and deliberately modest: consolidation is idempotent (distill NOOPs an
#: identical active fact) so an extra drain costs a bounded extraction, while too high a value
#: leaves memory session-trapped for longer.
DEFAULT_CONSOLIDATE_EVERY_N = 20


class SessionSaveTrigger:
    """Owns the session-end and capture-pressure consolidation triggers.

    Constructed once at the daemon composition root with the SAME org/workspace/user the capture
    path writes under, so the η it resolves matches the partition that session's STM turns actually
    live in (identical rule to ``PreCompactPromoter``).
    """

    def __init__(
        self,
        *,
        promoter: SessionPromoterPort,
        org: str,
        workspace: str,
        user: str,
        consolidate_every_n: int = DEFAULT_CONSOLIDATE_EVERY_N,
    ) -> None:
        if consolidate_every_n < 1:
            raise ValueError("consolidate_every_n must be >= 1")
        self._promoter = promoter
        self._org = org
        self._workspace = workspace
        self._user = user
        self._every_n = consolidate_every_n
        #: Per-session capture counter. In-process by design: the alternative is a store COUNT on
        #: every single ingest, which would put a network round-trip on the capture hot path to
        #: answer a question that a local integer answers exactly as well.
        self._counts: dict[str, int] = {}

    def _ns(self, session_id: str) -> Namespace:
        return Namespace(
            org=self._org,
            workspace=self._workspace,
            user=self._user,
            session=session_id,
            visibility=Visibility.PRIVATE,
        )

    async def on_session_end(self, activity: RawActivity) -> None:
        """Drain this session's STM buffer upward because the session is CLOSING.

        Raises on a misrouted activity — a caller bug, never silently ignored (mirrors
        ``PreCompactPromoter.on_precompact``). A store failure inside ``promote_session_now``
        PROPAGATES so the outbox worker's retry/dead-letter path sees it; this never swallows a real
        failure into a fake success (DEV-STANDARDS rule 8).
        """
        if activity.kind is not ActivityKind.SESSION_END:
            raise ValueError(
                f"SessionSaveTrigger.on_session_end received a non-SessionEnd activity "
                f"(kind={activity.kind.value}) — only ActivityKind.SESSION_END is routed here"
            )
        self._counts.pop(activity.session_id, None)  # the session is over; drop its counter
        _log.info("session_save.session_end_consolidate", session=activity.session_id)
        await self._promoter.promote_session_now(self._ns(activity.session_id), force=True)

    async def on_capture(self, session_id: str) -> bool:
        """Count one captured turn; drain the buffer when this session reaches the threshold.

        Returns whether a consolidation actually ran, so a caller/test can assert the cadence
        rather than infer it. The counter resets on drain, so pressure is measured per drain cycle.
        """
        count = self._counts.get(session_id, 0) + 1
        if count < self._every_n:
            self._counts[session_id] = count
            return False
        self._counts[session_id] = 0
        _log.info(
            "session_save.capture_pressure_consolidate",
            session=session_id,
            captured_turns=count,
        )
        await self._promoter.promote_session_now(self._ns(session_id), force=True)
        return True
