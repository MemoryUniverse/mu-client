"""Outbox record shapes (capture-spec.md §8.3, verbatim ``RecordState`` union)."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict

from mu_client.capture.model import RawActivity

__all__ = ["OutboxBatch", "OutboxRecord", "RecordState"]


class RecordState(StrEnum):
    PENDING = "pending"
    INFLIGHT = "inflight"
    ACKED = "acked"
    DEAD = "dead"


class OutboxRecord(BaseModel, frozen=True):
    model_config = ConfigDict(frozen=True, extra="forbid")

    seq: int
    activity: RawActivity
    state: RecordState
    attempts: int
    enqueued_at: datetime
    last_error: str | None = None


class OutboxBatch(BaseModel, frozen=True):
    model_config = ConfigDict(frozen=True, extra="forbid")

    records: list[OutboxRecord]
