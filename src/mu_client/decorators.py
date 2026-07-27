"""Cross-cutting decorator for :class:`~mu_client.host.LocalMemoryHost` methods (DEV-STANDARDS
rule 7: a concern repeated across many functions becomes a decorator, not copy-paste).

Mirrors ``mu_engine.platform.decorators``'s ``timed``+``guard`` behaviour (content-free span +
latency observation + a failure counter that re-raises, never swallows — rule 8) but is shaped for
an INSTANCE method whose ``Tracer``/``MetricSink`` live on ``self`` (the host's own client-level
sinks, built once in ``LocalMemoryHost.__init__`` — distinct from the engine-internal sinks
``LocalContainer`` wires for ingest/recall/distill). Reusing the engine's decorators verbatim would
require the tracer/metrics to be known at DECORATION time (module import), which ``self`` is not —
so this is a deliberate, minimal re-expression of the same idiom, not a duplicated implementation.
"""

from __future__ import annotations

import asyncio
import functools
import time
from collections.abc import Awaitable, Callable, Coroutine
from typing import TYPE_CHECKING, Any, Concatenate, ParamSpec, TypeVar

__all__ = ["observed"]

P = ParamSpec("P")
R = TypeVar("R")

_LATENCY_METRIC = "mu_client_operation_latency_seconds"
_ERROR_METRIC = "mu_client_operation_errors_total"

if TYPE_CHECKING:
    from mu_client.host import LocalMemoryHost


def observed(
    operation: str,
) -> Callable[
    [Callable[Concatenate[LocalMemoryHost, P], Awaitable[R]]],
    Callable[Concatenate[LocalMemoryHost, P], Coroutine[Any, Any, R]],
]:
    """Open a content-free span for ``operation`` on ``self._tracer``, observe latency and a
    failure counter on ``self._metrics``. ``CancelledError`` propagates uncounted (rule 1)."""

    def decorator(
        func: Callable[Concatenate[LocalMemoryHost, P], Awaitable[R]],
    ) -> Callable[Concatenate[LocalMemoryHost, P], Coroutine[Any, Any, R]]:
        @functools.wraps(func)
        async def wrapper(self: LocalMemoryHost, *args: P.args, **kwargs: P.kwargs) -> R:
            start = time.perf_counter()
            with self._tracer.span(operation):
                try:
                    return await func(self, *args, **kwargs)
                except asyncio.CancelledError:
                    raise
                except BaseException:
                    self._metrics.inc(_ERROR_METRIC, labels={"operation": operation})
                    raise
                finally:
                    self._metrics.observe(
                        _LATENCY_METRIC,
                        time.perf_counter() - start,
                        labels={"operation": operation},
                    )

        return wrapper  # type: ignore[return-value]  # mypy/functools.wraps + Concatenate limitation

    return decorator
