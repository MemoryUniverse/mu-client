"""``IpcServer`` — the daemon's front door (daemon-app-skeleton-spec.md §5). Unix-socket,
``SO_PEERCRED``-checked (only THIS uid may connect) — the daemon holds PRIVATE memory, so this is
never an unauthenticated bind (CANONICAL §8-m1).

**Deviation (recorded):** the spec's route table is HTTP-shaped (``POST /capture`` etc.); this
stage serves the identical contract over a newline-delimited JSON protocol on the SAME unix
socket (no aiohttp/uvicorn dependency for an MVP front door) — one JSON object in, one JSON
object out, connection closed. Loopback-HTTP is a route-table-compatible follow-up (the handlers
below are already framework-agnostic), not a rewrite.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import socket
import struct
from typing import Any

import structlog

from mu_client.capture.model import HostKind
from mu_client.capture.parsers import ParserRegistry
from mu_client.config import DaemonIpcSettings
from mu_client.errors import CaptureSchemaDriftError
from mu_client.inject.recall_bridge import RecallInjectBridge
from mu_client.observability.events import log_activity_captured, log_capture_source_halted
from mu_client.outbox.sqlite_outbox import SqliteOutbox

__all__ = ["IpcServer"]

_log = structlog.get_logger("mu.client.ipc")


class IpcServer:
    def __init__(
        self,
        settings: DaemonIpcSettings,
        *,
        registry: ParserRegistry,
        outbox: SqliteOutbox,
        bridge: RecallInjectBridge,
    ) -> None:
        self._settings = settings
        self._registry = registry
        self._outbox = outbox
        self._bridge = bridge
        self._server: asyncio.AbstractServer | None = None
        self._accepting = True

    async def bind(self) -> None:
        """Create + listen on the real unix socket. Split from :meth:`serve` so the composition
        root (``daemon/app.py``) can ``await`` a guarantee the socket file EXISTS before
        ``start()`` returns — a caller must never race the daemon's own startup to find out
        whether it is listening yet."""
        if self._server is not None:
            return
        socket_path = self._settings.socket_path.expanduser()
        socket_path.parent.mkdir(parents=True, exist_ok=True)
        with contextlib.suppress(FileNotFoundError):
            socket_path.unlink()
        self._server = await asyncio.start_unix_server(self._handle, path=str(socket_path))
        socket_path.chmod(0o600)
        _log.info("ipc.listening", socket_path=str(socket_path))

    async def serve(self) -> None:
        """Run the accept loop until :meth:`stop_accepting` closes the server. Call :meth:`bind`
        first (idempotent if already bound)."""
        await self.bind()
        if self._server is None:  # pragma: no cover — bind() above always sets this
            raise RuntimeError("IpcServer.bind() did not set a server")
        async with self._server:
            await self._server.serve_forever()

    async def stop_accepting(self) -> None:
        """Ordered-shutdown step 1 (daemon-app-skeleton-spec.md §3.3): stop new host handoffs —
        no new ``/capture`` accepted after this returns."""
        self._accepting = False
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            if self._settings.socket_peer_check and not _peer_is_self(writer):
                await _respond(writer, {"status": 401, "error": "foreign_uid"})
                return
            if not self._accepting:
                await _respond(writer, {"status": 503, "error": "shutting_down"})
                return
            line = await reader.readline()
            if not line:
                return
            request = json.loads(line)
            response = await self._dispatch(request)
            await _respond(writer, response)
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _dispatch(self, request: dict[str, Any]) -> dict[str, Any]:
        route = request.get("route")
        if route == "capture":
            return await self._route_capture(request)
        if route == "recall":
            return await self._route_recall(request)
        if route == "healthz":
            return await self._route_healthz()
        return {"status": 404, "error": "unknown_route", "route": route}

    async def _route_capture(self, request: dict[str, Any]) -> dict[str, Any]:
        host = HostKind(request["host"])
        record = request["record"]
        event_id = str(request.get("event_id") or _sha256_of(record))
        try:
            parser = self._registry.select(host, record)
            activity = parser.parse(record=record, event_id=event_id)
        except CaptureSchemaDriftError as drift:
            await self._outbox.quarantine_raw(
                host, json.dumps(record).encode("utf-8"), reason="schema_drift"
            )
            log_capture_source_halted(
                host=host.value, schema_version="unknown", raw_sample_sha256=drift.raw_sample_sha256
            )
            return {"status": 422, "error": "schema_drift"}
        record_row = await self._outbox.append(activity)
        log_activity_captured(
            activity_id=activity.activity_id,
            host=activity.host.value,
            kind=activity.kind.value,
            session_id=activity.session_id,
        )
        return {"status": 200, "activity_id": activity.activity_id, "seq": record_row.seq}

    async def _route_recall(self, request: dict[str, Any]) -> dict[str, Any]:
        rendered = await self._bridge.render(str(request["session_id"]), query=request.get("query"))
        return {"status": 200, **rendered.model_dump()}

    async def _route_healthz(self) -> dict[str, Any]:
        return {
            "status": 200,
            "outbox_depth": await self._outbox.outbox_depth(),
            "dead_letter_count": await self._outbox.undelivered_count(),
        }


def _peer_is_self(writer: asyncio.StreamWriter) -> bool:
    sock: socket.socket | None = writer.get_extra_info("socket")
    if sock is None:
        return False
    creds = sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
    _pid, uid, _gid = struct.unpack("3i", creds)
    return bool(uid == os.getuid())


async def _respond(writer: asyncio.StreamWriter, payload: dict[str, Any]) -> None:
    writer.write((json.dumps(payload) + "\n").encode("utf-8"))
    await writer.drain()


def _sha256_of(record: dict[str, Any]) -> str:
    encoded = json.dumps(record, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
