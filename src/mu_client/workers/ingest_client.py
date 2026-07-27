"""``IngestClientPort`` — outbox record -> ``LocalMemory.add`` (capture-spec.md §8.5). Only
``InProcessLocalIngest`` this stage (the daemon co-hosts ``LocalMemoryHost``); ``Http``/``Durable``
variants are a later, multi-process/SHARED-plane stage (out of scope, daemon-app-skeleton-spec.md
§9's ``ingest_mode`` literal is a forward-declared seam, not read yet)."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from mu_client.capture.model import CONTROL_KINDS, RawActivity
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
        await self._host.add(activity.text, user=self._user, session=activity.session_id)
