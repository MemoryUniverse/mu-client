"""``PreCompactPromoter`` — the promote-before-delete owner for the ``PreCompact`` control event
(AGENT-INTEGRATION-AUDIT-AND-PLAN.md §4 Phase 3; the owner ``mu_engine/lifecycle/promotion.py`` and
the audit table 2B deferred as "S1-07's job").

**The problem this closes.** A Claude Code ``PreCompact`` hook fires when the host is about to
compact/delete the session's context. MU parses it (``capture/parsers.py`` -> ``ActivityKind.
PRE_COMPACT``) but, being a ``CONTROL_KIND``, it was then DROPPED at ingest (``workers/
ingest_client.py`` raised ``ExtractionSkippedError`` and the worker just ack'd it) — so the
vision's "promote surviving turns before the host compacts/deletes them" was sold-as-done but was a
no-op. Every at-risk STM turn silently died with the compaction.

**What it does now.** On a ``PreCompact`` activity, this promoter builds the session's write η and
drives the REAL promotion/consolidation machinery — ``MemoryLifecycleManager.promote_session_now(
ns, force=True)`` -> ``PromotionService.promote_session(force=True)`` — to FORCE-promote the
session's at-risk STM turns STM->MTM (and distill MTM->LTM) so they reach a durable tier before the
host drops them. ``force=True`` deliberately BYPASSES the routine STM->MTM salience gate
(``promote_stm_mtm``): the whole point is to SAVE the at-risk context regardless of whether each
turn was individually salient enough for the periodic backstop — a turn about to be deleted has no
second chance. This is NOT a parallel promoter: it reuses the identical ``promote_session`` /
distill path every other lifecycle trigger uses (DEV-STANDARDS rule 6, DRY).

**Scope discipline.** This promoter owns ONLY the ``PreCompact`` control event. Non-PreCompact
activities never reach it (``InProcessLocalIngest`` routes only ``ActivityKind.PRE_COMPACT`` here).
The η it builds mirrors exactly the partition captures land under in the daemon (``org`` =
``default_namespace``, ``workspace`` = ``default_workspace``, ``user`` = ``default_user``,
``session`` = the activity's session id, ``visibility`` = PRIVATE) — the SAME η
``OutboxWorker``/``LocalMemory.add`` write to, so ``promote_session`` self-fetches the correct STM
window.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

import structlog
from mu_contracts.domain.model.memory import Namespace, Visibility

from mu_client.capture.model import ActivityKind, RawActivity

__all__ = ["PreCompactPromoter", "SessionPromoterPort"]

_log = structlog.get_logger("mu.client.lifecycle.precompact")


@runtime_checkable
class SessionPromoterPort(Protocol):
    """The one method ``PreCompactPromoter`` needs from the engine's orchestrator —
    ``mu_engine.lifecycle.manager.MemoryLifecycleManager`` satisfies this structurally (PEP 544),
    so mu-client depends on this narrow seam, not the whole manager surface. Kept structural so a
    unit test can drive routing with a tiny in-process double while the real proof runs the real
    manager against real stores."""

    async def promote_session_now(self, ns: Namespace, *, force: bool = False) -> None: ...


class PreCompactPromoter:
    """Promote-before-delete owner for the ``PreCompact`` control event (Phase 3).

    Constructed once at the daemon composition root with the daemon's real
    ``MemoryLifecycleManager`` and the SAME org/workspace/user the capture path writes under, so the
    η it resolves for a session matches the partition that session's STM turns actually live in.
    """

    def __init__(
        self,
        *,
        promoter: SessionPromoterPort,
        org: str,
        workspace: str,
        user: str,
    ) -> None:
        self._promoter = promoter
        self._org = org
        self._workspace = workspace
        self._user = user

    async def on_precompact(self, activity: RawActivity) -> None:
        """Force-promote the activity's session's at-risk STM turns into a durable tier before the
        host compacts/deletes them. Raises on a misrouted (non-PreCompact) activity — a caller bug,
        never silently ignored. Any store failure inside ``promote_session_now`` PROPAGATES (the
        outbox worker's retry/dead-letter path handles it) — this method never swallows a real
        failure into a fake success (DEV-STANDARDS rule 8)."""
        if activity.kind is not ActivityKind.PRE_COMPACT:
            raise ValueError(
                f"PreCompactPromoter.on_precompact received a non-PreCompact activity "
                f"(kind={activity.kind.value}) — only ActivityKind.PRE_COMPACT is routed here"
            )
        ns = Namespace(
            org=self._org,
            workspace=self._workspace,
            user=self._user,
            session=activity.session_id,
            visibility=Visibility.PRIVATE,
        )
        _log.info(
            "precompact.promote_before_delete",
            session=activity.session_id,
            trigger=activity.payload.get("trigger"),
        )
        # force=True: SAVE every at-risk STM turn regardless of routine salience — the host is
        # about to drop them. Reuses PromotionService.promote_session's real STM->MTM promote +
        # MTM->LTM distill machinery; the manager publishes its own MemoryPromoted events.
        await self._promoter.promote_session_now(ns, force=True)
