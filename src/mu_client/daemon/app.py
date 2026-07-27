"""``LocalDaemon`` — the composition root + lifecycle (daemon-app-skeleton-spec.md §3), scoped to
THIS build stage: **capture + outbox + recall/inject + IPC front door**, one ``TaskGroup``,
ordered shutdown. Device-sync (§8), cross-plane Centrifugo listeners (§8/§9), and bound-agent
supervision are SHARED-plane / multi-device features the ALL-CLIENT-CRITERIA brief scoped OUT of
this stage (they need a running mu-server + Centrifugo this repo never talks to — ``client-has-
no-server``); the daemon's shutdown order and TaskGroup-per-source discipline below are still the
REAL, load-bearing subset for a single-device LOCAL daemon.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

from mu_client.capture.hook import replay_spool
from mu_client.capture.parsers import ClaudeCodeParserV1, ParserRegistry
from mu_client.config import ClientSettings, get_client_settings
from mu_client.daemon.ipc import IpcServer
from mu_client.host import LocalMemoryHost
from mu_client.inject.recall_bridge import RecallInjectBridge
from mu_client.outbox.sqlite_outbox import SqliteOutbox
from mu_client.workers.ingest_client import InProcessLocalIngest
from mu_client.workers.pool import OutboxWorker, WorkerPool

__all__ = ["LocalDaemon"]


class LocalDaemon:
    """The ONE wiring point (APP-scoped singleton, daemon-app-skeleton-spec.md §3.1). Coded
    against :class:`~mu_client.config.ClientSettings` — never a bare ``os.environ`` read anywhere
    downstream of construction."""

    def __init__(self, settings: ClientSettings | None = None) -> None:
        self._settings = settings or get_client_settings()
        self._host: LocalMemoryHost | None = None
        self._outbox: SqliteOutbox | None = None
        self._pool: WorkerPool | None = None
        self._ipc: IpcServer | None = None
        self._tg: asyncio.TaskGroup | None = None
        self._run_task: asyncio.Task[None] | None = None

    @property
    def outbox(self) -> SqliteOutbox:
        if self._outbox is None:
            raise RuntimeError("LocalDaemon.start() was not called")
        return self._outbox

    @property
    def host(self) -> LocalMemoryHost:
        if self._host is None:
            raise RuntimeError("LocalDaemon.start() was not called")
        return self._host

    @contextlib.asynccontextmanager
    async def lifespan(self) -> AsyncIterator[LocalDaemon]:
        await self.start()
        try:
            yield self
        finally:
            await self.shutdown()

    async def start(self) -> None:
        # 1) ON-DEVICE ENGINE HOST — real mu-local, in-process (daemon-app-skeleton-spec.md §4).
        self._host = LocalMemoryHost(self._settings)
        await self._host.start()

        # 2) DURABILITY SPINE — the SQLite-WAL outbox (capture-spec.md §8.3). replay of any
        #    INFLIGHT->PENDING crash-recovery runs inside SqliteOutbox.open().
        self._outbox = SqliteOutbox(self._settings.outbox.outbox_path)
        await self._outbox.open()
        # Recover any hook-client spool (idempotent, UNIQUE(activity_id)).
        await replay_spool(self._settings, self._outbox)

        # 3) CAPTURE cluster — registry-selected parsers (Claude Code is the only host this
        #    stage wires; capture-spec.md §5.4 names Codex/Desktop as later-stage additions).
        registry = ParserRegistry()
        registry.register(
            ClaudeCodeParserV1(tool_outcome_max_chars=self._settings.capture.tool_outcome_max_chars)
        )

        # 4) WORKER POOL — drain -> LocalMemory.add -> ack (capture-spec.md §8.4).
        ingest = InProcessLocalIngest(self._host, user=self._settings.default_user)
        worker = OutboxWorker(
            self._outbox,
            ingest,
            settings=self._settings.outbox,
            org=self._settings.default_namespace,
            workspace=self._settings.default_workspace,
            user=self._settings.default_user,
        )
        self._pool = WorkerPool(worker, poll_interval_s=self._settings.outbox.poll_interval_s)

        # 5) INJECTION bridge (capture-spec.md §7.2).
        bridge = RecallInjectBridge(self._host, settings=self._settings.inject)

        # 6) FRONT DOOR. Bound HERE (synchronously, awaited) — a caller of start()/lifespan() must
        #    be able to connect to the real socket the instant start() returns, never race the
        #    background accept loop's own startup.
        self._ipc = IpcServer(
            self._settings.ipc, registry=registry, outbox=self._outbox, bridge=bridge
        )
        await self._ipc.bind()

        await self._supervise()

    async def _supervise(self) -> None:
        """One ``TaskGroup``, fixed task set (daemon-app-skeleton-spec.md §3.2): the pool and the
        IPC server are the two top-level supervised tasks THIS stage owns. Started as a
        background task (not awaited here) so ``start()`` returns once the daemon is live —
        ``lifespan()``'s caller awaits its own stop signal, then drives ``shutdown()``."""
        if self._pool is None or self._ipc is None:
            raise RuntimeError("_supervise() called before start() finished wiring")
        pool, ipc = self._pool, self._ipc

        async def _run() -> None:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(pool.run())
                tg.create_task(ipc.serve())  # bind() already ran; this just accepts + serve_forever
                self._tg = tg

        # The socket is already bound (ipc.bind() ran above, awaited); this task just runs the
        # worker-pool poll loop + the accept loop. Yield once so both tasks get a chance to reach
        # their first await point before start() returns.
        self._run_task = asyncio.create_task(_run())
        await asyncio.sleep(0)

    async def shutdown(self) -> None:
        """Ordered shutdown (daemon-app-skeleton-spec.md §3.3): stop new inbound (1), drain
        outbound (2 — this stage has no per-source checkpoint to persist, capture is stateless
        request/response over IPC, not a tailer), release the engine LAST. A crash before step 2
        completes is safe — every record was fsync'd at append (capture-spec.md §8.3)."""
        if self._ipc is not None:
            await self._ipc.stop_accepting()  # 1. stop new /capture handoffs
        if self._pool is not None:
            await self._pool.drain_and_stop()  # 2. drain in-flight outbox -> remember -> ack
        if self._run_task is not None:
            self._run_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._run_task
        if self._outbox is not None:
            await self._outbox.aclose()
        if self._host is not None:
            await self._host.aclose()  # 3. release engine adapters LAST
