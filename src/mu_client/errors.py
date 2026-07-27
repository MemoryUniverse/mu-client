"""``mu-client`` error hierarchy (DEV-STANDARDS rule 8: one typed hierarchy, fail-loud, no silent
fallback) + the CLI's global exception boundary.

Extends the frozen ``mu_contracts.domain.errors.MemoryUniverseError`` root (the shared wire
vocabulary every mu-core/mu-client/mu-server plane maps) — additive, no new envelope.
"""

from __future__ import annotations

import asyncio
import functools
import sys
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any, ParamSpec, TypeVar

from mu_contracts.domain.errors import MemoryUniverseError, SchemaDriftError

__all__ = [
    "CaptureSchemaDriftError",
    "ClientError",
    "ClientNotStartedError",
    "OutboxCorruptionError",
    "cli_error_boundary",
]

P = ParamSpec("P")
R = TypeVar("R")


class ClientError(MemoryUniverseError):
    """Root of mu-client's OWN error subtree — never raised directly, only its subclasses."""


class ClientNotStartedError(ClientError):
    """A verb was called on a :class:`~mu_client.host.LocalMemoryHost` before ``start()`` (or after
    ``aclose()``) — fail loud rather than silently constructing a second, unmanaged engine."""


class OutboxCorruptionError(ClientError):
    """The outbox WAL is unreadable at open — the daemon/CLI refuses to start (loud) rather than
    running healthy with a silently lost backlog (daemon-app-skeleton-spec.md §10)."""


class CaptureSchemaDriftError(SchemaDriftError):
    """A capture parser saw an unexpected schema (capture-spec.md §4.2). Subclasses mu-core's
    :class:`~mu_contracts.domain.errors.SchemaDriftError` root (so it is caught by any handler
    written against that type) but carries the structured, content-free fields the spec pins:
    ``host``, ``source_id``, ``detected_keys``, ``expected_schema``, ``raw_sample_sha256`` — never
    the raw record itself (only a hash of it may cross this boundary)."""

    def __init__(
        self,
        *,
        host: str,
        source_id: str,
        detected_keys: list[str],
        expected_schema: list[str],
        raw_sample_sha256: str,
    ) -> None:
        self.host = host
        self.source_id = source_id
        self.detected_keys = detected_keys
        self.expected_schema = expected_schema
        self.raw_sample_sha256 = raw_sample_sha256
        super().__init__(
            f"schema drift on host={host!r} source={source_id!r}: "
            f"detected_keys={sorted(detected_keys)} not matched by any of "
            f"expected_schema={expected_schema!r} (raw_sample_sha256={raw_sample_sha256})"
        )


def cli_error_boundary(
    func: Callable[P, Awaitable[int]],
) -> Callable[P, Coroutine[Any, Any, int]]:
    """The CLI's ONE global exception→exit-code mapping (DEV-STANDARDS rule 8).

    * ``asyncio.CancelledError`` / ``KeyboardInterrupt`` propagate/exit as SIGINT convention (130) —
      never counted as an application error.
    * A ``MemoryUniverseError`` (this hierarchy, or an engine/local one it wraps) prints a single
      content-free line — the error's ``type`` + a bounded, already-safe message (never a raw
      traceback of internal state) — to stderr and returns exit code 1.
    * Any other unexpected exception is NEVER swallowed: it re-raises after being logged, so a
      genuine bug surfaces loudly (never a silent wrong "success").
    """

    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> int:
        try:
            return await func(*args, **kwargs)
        except asyncio.CancelledError:
            raise
        except KeyboardInterrupt:
            return 130
        except MemoryUniverseError as exc:
            print(f"mu: {type(exc).__name__}: {exc}", file=sys.stderr)  # noqa: T201
            return 1

    return wrapper
