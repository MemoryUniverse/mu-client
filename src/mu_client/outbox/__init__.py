"""The durability spine (capture-spec.md §8.3) — a REAL WAL-mode SQLite outbox. Every capture is
fsync'd here BEFORE the caller is acked; a crash mid-capture loses nothing (``UNIQUE(activity_id)``
makes any replay a no-op)."""

from __future__ import annotations
