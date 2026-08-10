"""``IngestClientPort`` — outbox record -> ``LocalMemory.add`` (capture-spec.md §8.5). Only
``InProcessLocalIngest`` this stage (the daemon co-hosts ``LocalMemoryHost``); ``Http``/``Durable``
variants are a later, multi-process/SHARED-plane stage (out of scope, daemon-app-skeleton-spec.md
§9's ``ingest_mode`` literal is a forward-declared seam, not read yet).

**Subagent attribution (Phase 0 cheap wire, AGENT-INTEGRATION-AUDIT-AND-PLAN.md §6A "Concrete
design, cut 1").** ``ActivityKind.SUBAGENT_RUN`` carries ``payload["agent_type"]`` (which subagent
produced this turn — capture/parsers.py's ``_map_event``), but until this wire it was read only by
the outbox row, never by ``ingest()`` — the identity was captured then silently dropped before
reaching ``.add()``, so a subagent's memory was indistinguishable from a top-level turn once
stored. This is deliberately the CHEAP cut, not the real one: no new namespace, no new
``ClientScope``/``agent_principal_id`` partition (that is "Phase 1.5" per §6A/§6D, a genuine new
identity model) — just free-text provenance, a ``[subagent:{agent_type}]`` prefix on the stored
text, so "which subagent said this" is queryable in the SAME partition it already lands in today.
Zero new contracts, zero widened verb surface."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from mu_client.capture.claude_tailer import THINKING_SOURCE as _THINKING_SOURCE
from mu_client.capture.model import CONTROL_KINDS, ActivityKind, RawActivity
from mu_client.host import LocalMemoryHost

__all__ = ["ExtractionSkippedError", "InProcessLocalIngest", "IngestClientPort"]


class ExtractionSkippedError(Exception):
    """Not an error — a sentinel the worker checks for to distinguish "this activity carries no
    memory-worthy text" (control kind, dropped slash-command, empty tool outcome) from a genuine
    ingest failure. Raised internally by :class:`InProcessLocalIngest`, always caught by the
    worker (:mod:`mu_client.workers.pool`), never surfaced past it."""


@runtime_checkable
class IngestClientPort(Protocol):
    async def ingest(self, activity: RawActivity) -> None: ...


class InProcessLocalIngest:
    """capture-spec.md §7.1 kind-gate, enforced HERE (not at capture time — the outbox keeps every
    row, including control kinds, for provenance/triggers; §5.5): an activity with ``kind`` in
    :data:`~mu_client.capture.model.CONTROL_KINDS` or ``text is None`` (the salient-slice filter
    already dropped it, e.g. a bare slash-command) never reaches :meth:`LocalMemoryHost.add` — it
    is durably outboxed + acked, but never becomes a ``MemoryItem``."""

    def __init__(self, host: LocalMemoryHost, *, user: str) -> None:
        self._host = host
        self._user = user

    async def ingest(self, activity: RawActivity) -> None:
        if activity.kind in CONTROL_KINDS or activity.text is None:
            raise ExtractionSkippedError(
                f"activity {activity.activity_id} kind={activity.kind.value} carries no "
                "memory-worthy text (control kind or filtered slice) — ack without remember()"
            )
        text = activity.text
        if activity.kind is ActivityKind.SUBAGENT_RUN:
            agent_type = activity.payload.get("agent_type")
            if isinstance(agent_type, str) and agent_type:
                text = f"[subagent:{agent_type}] {text}"
        # Phase 0B background-thinking attribution (capture-spec.md §6): a reasoning-tail memory is
        # tagged ``payload["source"]=="thinking"`` — prefix the stored text ``[thinking] `` (mirrors
        # the ``[subagent:...]`` provenance above) so a stored decision/finding is distinguishable
        # from a top-level turn in the SAME partition it already lands in. No new namespace/verb.
        if activity.payload.get("source") == _THINKING_SOURCE:
            text = f"[thinking] {text}"
        # Thread the tailer's per-candidate importance straight to the engine's promotion gate
        # (``LocalMemory.add(importance_score=...)`` → ``DeterministicPromoteStage``). ``None``
        # (every hook-parsed activity) leaves add()'s own default untouched — byte-for-byte the
        # prior behaviour; a set value (reasoning candidates) drives STM→MTM promotion. We REUSE
        # this one gate, we do not add a second (AGENT-INTEGRATION-AUDIT-AND-PLAN.md §6).
        await self._host.add(
            text,
            user=self._user,
            session=activity.session_id,
            importance_score=activity.importance,
        )
