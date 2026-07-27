"""``LocalMemoryHost`` — hosts mu-local's ``LocalMemory`` with a clean async lifecycle
(daemon-app-skeleton-spec.md §4 ``EngineHost``, restricted to THIS stage's scope: no capture/outbox/
IPC yet — those are the daemon stage. This is the thin client-side wrapper the CLI's daemonless
path and any programmatic caller share).

``EngineHost`` (the spec's daemon-owned composition) and this class both wrap mu-local's
``LocalContainer``/``LocalMemory`` and "add no engine logic" (composition.py's own docstring) — the
daemon's future ``EngineHost`` will reuse the identical construction shape below when that stage
lands; this class is written so that promotion is a straight lift, not a rewrite.

Cancellation-safe (DEV-STANDARDS rule 1): construction runs the embedder's real (blocking, CPU-bound
model-load) work off the event loop via ``asyncio.to_thread``; ``aclose()`` clears the owned
reference BEFORE awaiting teardown so a cancelled close never leaves the host thinking it is still
started.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from mu_contracts.config.settings import RuntimeMode, Settings
from mu_engine.platform.observability import build_metrics, build_tracer
from mu_engine.storage.domain.memory import MemoryTier
from mu_local.config import StorageSettings as LocalBackendSettings
from mu_local.local_memory import LocalMemory
from mu_local.views import MemoryListView, MemoryWriteResult

from mu_client.config import ClientSettings, get_client_settings
from mu_client.decorators import observed
from mu_client.errors import ClientNotStartedError

if TYPE_CHECKING:
    from mu_contracts.ports.observability import MetricSink, Tracer

__all__ = ["LocalMemoryHost", "daemonless_host"]


class LocalMemoryHost:
    """Constructs + owns ONE ``LocalMemory`` over real local stores (mu-core's composition /
    ``STORE_REGISTRY``, via ``LocalContainer`` — never hand-instantiates a vendor client itself,
    exactly mu-local's own extension-seam discipline). Idempotent ``start()``; safe double
    ``aclose()``. Use as an async context manager (:func:`daemonless_host`) for the one-shot path,
    or hold it open across multiple ops in a longer-lived programmatic caller.
    """

    def __init__(self, settings: ClientSettings | None = None) -> None:
        self._settings = settings or get_client_settings()
        self._memory: LocalMemory | None = None
        # Client-level sinks (DEV-STANDARDS rule 4) — distinct from the engine-internal sinks
        # LocalContainer wires for ingest/recall/distill; these observe the HOST's own lifecycle +
        # verb-proxy operations (see mu_client.decorators.observed).
        self._tracer: Tracer = build_tracer(enabled=True, service_name="mu-client")
        self._metrics: MetricSink = build_metrics(enabled=True)

    @property
    def settings(self) -> ClientSettings:
        return self._settings

    @property
    def is_started(self) -> bool:
        return self._memory is not None

    async def start(self) -> LocalMemory:
        """Idempotent: a second ``start()`` on an already-started host returns the SAME
        ``LocalMemory`` instance rather than silently constructing a second, unmanaged engine."""
        if self._memory is not None:
            return self._memory
        core_settings = self._core_settings()
        with self._tracer.span("host.start"):
            # LocalContainer.__init__ is synchronous and loads the REAL local embedder (a genuine
            # CPU-bound model load, not I/O) — run it off the event loop (DEV-STANDARDS async
            # sharpener: no blocking work in the loop).
            memory = await asyncio.to_thread(
                LocalMemory,
                LocalBackendSettings(),
                workspace=self._settings.default_workspace,
                namespace=self._settings.default_namespace,
                settings=core_settings,
            )
        self._memory = memory
        self._metrics.inc("mu_client_host_starts_total")
        return memory

    async def aclose(self) -> None:
        """Release every store connection the container opened. Cancellation-safe: the owned
        reference is cleared BEFORE the awaited teardown, so a cancelled ``aclose()`` never leaves
        the host reporting ``is_started`` while its engine is half-torn-down."""
        memory, self._memory = self._memory, None
        if memory is None:
            return
        with self._tracer.span("host.aclose"):
            await memory.aclose()

    async def __aenter__(self) -> LocalMemoryHost:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    # ------------------------------------------------------------------------------- verb proxies
    # Thin, observed delegations to the owned LocalMemory — the surface both the CLI (cli.py) and a
    # programmatic caller drive. Kept explicit (not a dynamic __getattr__ proxy) so mypy --strict
    # sees a fully-typed public API.
    @observed("client.add")
    async def add(
        self,
        content: str,
        *,
        user: str | None = None,
        session: str | None = None,
    ) -> MemoryWriteResult:
        memory = self._require_memory()
        return await memory.add(content, user=user or self._settings.default_user, session=session)

    @observed("client.recall")
    async def recall(
        self,
        query: str,
        *,
        user: str | None = None,
        session: str | None = None,
        tier: MemoryTier | None = None,
        limit: int = 10,
    ) -> MemoryListView:
        memory = self._require_memory()
        return await memory.recall(
            query,
            user=user or self._settings.default_user,
            session=session,
            tier=tier,
            limit=limit,
        )

    @observed("client.search")
    async def search(
        self,
        query: str,
        *,
        user: str | None = None,
        session: str | None = None,
        tier: MemoryTier | None = None,
        limit: int = 10,
    ) -> MemoryListView:
        """mem0 muscle-memory alias for :meth:`recall` (mirrors ``LocalMemory.search``)."""
        return await self.recall(query, user=user, session=session, tier=tier, limit=limit)

    # ----------------------------------------------------------------------------------- internals
    def _require_memory(self) -> LocalMemory:
        if self._memory is None:
            raise ClientNotStartedError(
                "LocalMemoryHost.start() was not called (or aclose() already ran) — use "
                "'async with LocalMemoryHost(...) as host' or 'await host.start()' first"
            )
        return self._memory

    def _core_settings(self) -> Settings:
        """Build the ``mu_contracts`` central ``Settings`` (endpoint host/ports) that
        ``LocalContainer`` falls back to when a ``BackendChoice.config`` is empty — this is the
        exact seam ``mu_local``'s own integration test drives (``tests/conftest.py``:
        ``Settings()``), just sourced from THIS client's own env boundary instead of a bare
        ``Settings()`` construction, so a client-side env override (``MU_STORAGE__...``) reaches
        the same place mu-core's own tests read it from."""
        return Settings(runtime_mode=RuntimeMode.LOCAL, storage=self._settings.storage)


@contextlib.asynccontextmanager
async def daemonless_host(
    settings: ClientSettings | None = None,
) -> AsyncIterator[LocalMemoryHost]:
    """The ONE daemonless one-shot path (daemon-app-skeleton-spec.md §6, Path A): construct the
    host, hand it to the caller, tear it down on exit — NO daemon, NO socket, NO listener. Both the
    CLI (``mu add|recall|search``) and any programmatic embedder use this exact helper."""
    host = LocalMemoryHost(settings)
    await host.start()
    try:
        yield host
    finally:
        await host.aclose()
