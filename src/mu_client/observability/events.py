"""Content-free capture/daemon event emission (CANONICAL-CONTRACTS §3, capture-spec.md §11).

**Deviation (recorded).** mu-core ships ``EventBusPort`` as a Protocol only (``mu_contracts.ports
.bus``) — no concrete LOCAL-plane adapter (Redis Streams) exists yet in this repo to construct one
against. Rather than hand-roll an unreviewed bus adapter out of this stage's scope, every event
this module logs is the SAME frozen, content-free ``DomainEvent`` subclass CANONICAL/capture-spec
pin (``ActivityCaptured``, ``MemoryCaptured``, ``DegradedModeEntered``, ``CaptureSourceHalted``,
``OutboxRecordDeadLettered``, ``HostInjectionSkipped``) — constructed for real, validated by
pydantic, then emitted via ``structlog`` (content-free by construction: ``DomainEvent`` itself
structurally forbids ``text``/``body``/``content`` fields). Promoting this to a live
``EventBusPort.publish`` later is a one-line swap at each call site below, not a rewrite.
"""

from __future__ import annotations

from typing import Any

import structlog
from mu_contracts.domain.events import (
    ActivityCaptured,
    CaptureSourceHalted,
    DegradedModeEntered,
    DegradeReason,
    DomainEvent,
    HostInjectionSkipped,
    MemoryCaptured,
    OutboxRecordDeadLettered,
)
from mu_contracts.domain.model.memory import Namespace

__all__ = [
    "EventLog",
    "log_activity_captured",
    "log_capture_source_halted",
    "log_degraded",
    "log_host_injection_skipped",
    "log_memory_captured",
    "log_outbox_record_dead_lettered",
]

_log = structlog.get_logger("mu.client.events")


class EventLog:
    """Thin instance wrapper so a call site (daemon/app.py, workers/pool.py) can hold ONE logger
    reference rather than importing the module-level functions piecemeal; both forms emit the
    identical validated ``DomainEvent`` payload."""

    def __init__(self, *, component: str) -> None:
        self._log = _log.bind(component=component)

    def emit(self, event: DomainEvent) -> None:
        _emit(event, self._log)


def _emit(event: DomainEvent, log: Any | None = None) -> None:
    (log or _log).info(type(event).__name__, **event.model_dump(mode="json"))


def log_activity_captured(*, activity_id: str, host: str, kind: str, session_id: str) -> None:
    _emit(ActivityCaptured(activity_id=activity_id, host=host, kind=kind, session_id=session_id))


def log_memory_captured(*, namespace: Namespace, ids: list[str]) -> None:
    _emit(MemoryCaptured(namespace=namespace, ids=ids))


def log_degraded(
    *, component: str, mode: str, reason: DegradeReason, detail: str | None = None
) -> None:
    _emit(DegradedModeEntered(component=component, mode=mode, reason=reason, detail=detail))


def log_capture_source_halted(*, host: str, schema_version: str, raw_sample_sha256: str) -> None:
    _emit(
        CaptureSourceHalted(
            host=host, schema_version=schema_version, raw_sample_sha256=raw_sample_sha256
        )
    )


def log_outbox_record_dead_lettered(*, seq: int, reason: str) -> None:
    _emit(OutboxRecordDeadLettered(seq=seq, reason=reason))


def log_host_injection_skipped(*, session_id: str, reason: str) -> None:
    _emit(HostInjectionSkipped(session_id=session_id, reason=reason))
