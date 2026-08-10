"""``IngestClientPort`` — outbox record -> ``LocalMemory.add`` (capture-spec.md §8.5). Only
``InProcessLocalIngest`` this stage (the daemon co-hosts ``LocalMemoryHost``); ``Http``/``Durable``
variants are a later, multi-process/SHARED-plane stage (out of scope, daemon-app-skeleton-spec.md
§9's ``ingest_mode`` literal is a forward-declared seam, not read yet).

**Subagent attribution (Phase 1.5, AGENT-INTEGRATION-AUDIT-AND-PLAN.md §6A "cut 2 — agent-scoped
partition"; ``mu_contracts.domain.model.agent``).** ``ActivityKind.SUBAGENT_RUN`` carries
``payload["agent_type"]`` (which subagent produced this turn — capture/parsers.py's ``_map_event``).
Phase 0 was the CHEAP cut: a ``[subagent:{agent_type}]`` text prefix only, SAME partition as a
top-level turn. Phase 1.5 lands the REAL one: the ``agent_type`` is now ALSO threaded to
``host.add(agent_type=...)`` → ``LocalMemory.add``, which resolves a stable, deterministic
``agent_principal_id`` and writes the memory into a DISTINCT agent-scoped ``η`` partition (a real
``ClientScope`` with ``AgentKind.SUBAGENT`` — the identity model's first live call site), while
keeping ``η.user`` = the owner so the parent's federate-live recall still surfaces it and cross-user
isolation holds (agent.py federation choice). The text prefix stays as human-readable provenance;
the partition is the genuine isolation. Gated by ``subagent_partitions`` (default ON) for backward
compatibility. DEFERRED (flagged, not faked): the ``AgentRegistryPort``/``AgentRegistryService``
write-side, ``AgentVisibilityGrant`` own⊕granted arms, and the ``AgentEnrolled``/``SubagentSpawned``
lifecycle events (§6D) — none required for the partition + owner-federation this stage proves."""

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

    def __init__(
        self, host: LocalMemoryHost, *, user: str, subagent_partitions: bool = True
    ) -> None:
        self._host = host
        self._user = user
        # Phase 1.5 gate (AGENT-INTEGRATION-AUDIT-AND-PLAN.md §6). ON by default: a SUBAGENT_RUN
        # capture writes to a distinct agent-scoped partition (agent.py). OFF ⇒ Phase-0 behaviour
        # (the ``[subagent:...]`` text prefix only, SAME partition as a top-level turn) — the
        # backward-compatible escape hatch. Human/top-level captures are byte-for-byte unchanged
        # either way; the flag only affects subagent-attributed activities.
        self._subagent_partitions = subagent_partitions

    async def ingest(self, activity: RawActivity) -> None:
        if activity.kind in CONTROL_KINDS or activity.text is None:
            raise ExtractionSkippedError(
                f"activity {activity.activity_id} kind={activity.kind.value} carries no "
                "memory-worthy text (control kind or filtered slice) — ack without remember()"
            )
        text = activity.text
        # A SUBAGENT_RUN's ``agent_type`` (capture/parsers.py ``_map_event``) drives BOTH: the
        # human-readable ``[subagent:...]`` provenance prefix on the stored text (Phase 0, kept),
        # AND — when partitions are enabled (Phase 1.5) — a real agent-scoped namespace partition
        # threaded to ``host.add(agent_type=...)`` (LocalMemory resolves the deterministic
        # ``agent_principal_id`` + partition, agent.py). The text prefix is provenance; the
        # partition is the genuine isolation the audit's cut 2 called for.
        subagent_type: str | None = None
        if activity.kind is ActivityKind.SUBAGENT_RUN:
            agent_type = activity.payload.get("agent_type")
            if isinstance(agent_type, str) and agent_type:
                text = f"[subagent:{agent_type}] {text}"
                if self._subagent_partitions:
                    subagent_type = agent_type
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
            agent_type=subagent_type,
        )
