"""``IpcServer`` — the daemon's front door (daemon-app-skeleton-spec.md §5). Unix-socket,
``SO_PEERCRED``-checked (only THIS uid may connect) — the daemon holds PRIVATE memory, so this is
never an unauthenticated bind (CANONICAL §8-m1).

**Deviation (recorded):** the spec's route table is HTTP-shaped (``POST /capture`` etc.); this
stage serves the identical contract over a newline-delimited JSON protocol on the SAME unix
socket (no aiohttp/uvicorn dependency for an MVP front door) — one JSON object in, one JSON
object out, connection closed. Loopback-HTTP is a route-table-compatible follow-up (the handlers
below are already framework-agnostic), not a rewrite.

**Framing is EXPLICIT (``DaemonIpcSettings.max_request_bytes`` / ``request_io_timeout_s``).**
Every failure mode of reading one request line is ANSWERED rather than closed silently: a record
past the limit gets ``413``, a peer that goes silent mid-line is reaped instead of pinning its
handler (and, through it, ``stop_accepting()``'s ``wait_closed()``). A close with no reply is
indistinguishable from success on the wire, and the client's fallback is the only thing standing
between a refused capture and a lost one — so this server never answers with silence.

**``/state`` + ``/ready-context`` (S3-03, ADR 0033 "always-accessible" leg 2).** memory-lifecycle-
manager-spec.md §5 (lines 247-252): "the existing loopback IPC socket serves ``/state``,
``/recall``, ``/ready-context`` as instant warm-cache reads that never touch the runner". Both new
routes dispatch to the SAME ``_handle``/``_dispatch`` newline-delimited-JSON pipe every other
route already uses — no second protocol, no new auth surface (the ``SO_PEERCRED`` check in
``_handle`` runs before ``_dispatch`` is ever reached, identically for every route). Both handlers
below call ``MemoryLifecycleManager.get_state``/``.ready_context`` directly — spec §5's own words,
"synchronous warm reads (never enqueue, never await a job)" — so a sweep/promote/demote job
in flight on the SAME ``LifecycleWorkflowRunnerPort`` never blocks either route: there is no
``await self._runner...`` anywhere on this call path, structurally (not just empirically) unlike
``sweep_user``/``promote``/``demote``, which are the ONLY methods on the manager that touch
``self._runner``. ``lifecycle_manager`` is optional (``None`` when a caller — e.g. today's
``daemon/app.py``, ahead of its own integrate-phase wiring — has not threaded one through yet);
both routes then answer a named, content-free 503 rather than raising, matching this file's
existing "never construct a service IpcServer itself owns" discipline for ``registry``/``outbox``/
``bridge``.

**``/health`` + ``/pin`` + ``/unpin`` (memory-health-pinning-spec.md §7.1 line 331 / §7.2 line
339).** The spec puts ``mu health`` / ``mu pin <id>`` / ``mu unpin <id>`` on exactly this loopback
socket so the LOCAL plane answers "how is my memory doing?" and "never forget this one" *with no
UI open and no server*. All three dispatch through the SAME newline-delimited-JSON pipe and the
SAME ``SO_PEERCRED`` check as every other route — no second protocol, no new auth surface (the
spec's "per-daemon token" belongs to the loopback-HTTP variant this daemon does not implement;
peer-uid is the stronger check on a unix socket).

``health``/``pin`` are optional exactly as ``lifecycle_manager`` is, and for a harder reason: both
``mu_engine`` services require a ``MemoryRepository`` façade **mu-core does not yet implement**
(see :mod:`mu_client.memory_health` for the citation). Until a composition root can hand one in,
these routes answer a named, content-free 503 — never a raise, never a fabricated view.

⚠ ``health`` is NOT ``healthz``. ``healthz`` is daemon liveness (outbox depth / dead letters);
``health`` is the user's memory-health lens. Exact-match dispatch keeps them apart functionally;
they are one character apart on the wire, which is a reading hazard flagged for the owner.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import socket
import struct
from typing import TYPE_CHECKING, Any

import structlog
from mu_contracts.domain.model.memory import Namespace

from mu_client.capture.model import HostKind
from mu_client.capture.parsers import ParserRegistry
from mu_client.config import DaemonIpcSettings
from mu_client.errors import CaptureSchemaDriftError
from mu_client.inject.recall_bridge import RecallInjectBridge
from mu_client.memory_health import (
    HEALTH_ROUTE,
    HEALTH_UNWIRED,
    PIN_ROUTE,
    PIN_SURFACE_ERRORS,
    PIN_UNWIRED,
    UNKNOWN_HEALTH_FLAG,
    UNPIN_ROUTE,
    local_scope,
    malformed_request_response,
    namespace_on_the_wire,
    parse_health_flags,
    pin_failure_response,
    pin_request_of,
    private_plane_refusal,
    unwired_response,
)
from mu_client.observability.events import log_activity_captured, log_capture_source_halted
from mu_client.outbox.sqlite_outbox import SqliteOutbox

if TYPE_CHECKING:
    # Type-only (mirrors host.py/app.py's identical guard): IpcServer never CONSTRUCTS a
    # MemoryLifecycleManager, only calls warm-read methods on one handed to it, so no runtime
    # import is needed on this socket-front-door module's own import surface.
    from mu_engine.lifecycle.manager import MemoryLifecycleManager
    from mu_engine.services.health import MemoryHealthService
    from mu_engine.services.pin import PinService

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
        lifecycle_manager: MemoryLifecycleManager | None = None,
        health: MemoryHealthService | None = None,
        pin: PinService | None = None,
    ) -> None:
        self._settings = settings
        self._registry = registry
        self._outbox = outbox
        self._bridge = bridge
        # Optional (module docstring): None until a composition root (integrate-phase daemon/
        # app.py wiring — S1-03's real MemoryLifecycleManager already exists there as
        # LocalDaemon._lifecycle_manager) threads one through. /state and /ready-context answer a
        # named 503 rather than raising when absent.
        self._lifecycle_manager = lifecycle_manager
        # Same optional-injection contract as ``lifecycle_manager`` above, for the memory-health +
        # pinning surface (spec §7). NEVER constructed here — this class does not own them.
        self._health = health
        self._pin = pin
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
        # ``limit=`` is NOT optional here: asyncio's default ``StreamReader`` limit is 64 KiB, and
        # an ordinary ``PostToolUse`` record (untruncated ``tool_response``) exceeds it — see
        # ``DaemonIpcSettings.max_request_bytes`` for the sizing, and ``_handle`` for the 413 a
        # record past even THIS bound now gets instead of a silent close.
        self._server = await asyncio.start_unix_server(
            self._handle, path=str(socket_path), limit=self._settings.max_request_bytes
        )
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
        timeout_s = self._settings.request_io_timeout_s
        try:
            if self._settings.socket_peer_check and not _peer_is_self(writer):
                await _respond(writer, {"status": 401, "error": "foreign_uid"}, timeout_s=timeout_s)
                return
            if not self._accepting:
                await _respond(
                    writer, {"status": 503, "error": "shutting_down"}, timeout_s=timeout_s
                )
                return
            try:
                line = await asyncio.wait_for(reader.readline(), timeout=timeout_s)
            except ValueError:
                # ``readline()`` overran ``limit=`` (``max_request_bytes``) — the record is bigger
                # than this daemon frames. ANSWER it: a silent close is indistinguishable from a
                # success at the client, and that is exactly how captures were being dropped. A
                # 413 makes the client spool instead (durability boundary BEFORE the host is
                # acked). Content-free: only the limit is logged, never the record.
                _log.info("ipc.request_too_large", limit_bytes=self._settings.max_request_bytes)
                await _respond(
                    writer, {"status": 413, "error": "request_too_large"}, timeout_s=timeout_s
                )
                return
            if not line:
                return
            request = json.loads(line)
            response = await self._dispatch(request)
            await _respond(writer, response, timeout_s=timeout_s)
        except (TimeoutError, ConnectionError) as exc:
            # A peer that went silent mid-line, or hung up before reading its reply. RELEASING the
            # handler is the point: ``wait_closed()`` (3.12: waits for live handlers) is step 1 of
            # the ordered shutdown, so one stuck handler otherwise hangs the whole daemon's exit.
            # Content-free — the exception TYPE only, never the request.
            _log.info("ipc.request_aborted", error=type(exc).__name__, timeout_s=timeout_s)
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
        if route == "state":
            return await self._route_state(request)
        if route == "ready-context":
            return await self._route_ready_context(request)
        if route == HEALTH_ROUTE:
            return await self._route_health(request)
        if route == PIN_ROUTE:
            return await self._route_pin(request)
        if route == UNPIN_ROUTE:
            return await self._route_unpin(request)
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
        # ``mode="json"``: ``RenderedContext`` carries a real ``datetime`` (``computed_at``, the
        # canonical §2.2 shape) and this dict is handed straight to ``json.dumps`` at
        # ``_write_line`` — which has no ``default=`` and would raise on a datetime.
        return {"status": 200, **rendered.model_dump(mode="json")}

    async def _route_healthz(self) -> dict[str, Any]:
        return {
            "status": 200,
            "outbox_depth": await self._outbox.outbox_depth(),
            "dead_letter_count": await self._outbox.undelivered_count(),
        }

    async def _route_state(self, request: dict[str, Any]) -> dict[str, Any]:
        """``/state`` -> ``MemoryLifecycleManager.get_state(ns)`` — instant warm read (spec §5).
        ``async def`` for dispatch-table uniformity only: the body below contains no ``await`` at
        all, so a running sweep on ``self._lifecycle_manager``'s own
        ``LifecycleWorkflowRunnerPort`` structurally cannot block this coroutine — there is nothing
        here to block ON.

        ``namespace`` on the wire is η's own 5-part codec (``Namespace.parts()``/``.from_parts()``,
        CANONICAL §7.3): ``[org, workspace, user, session, visibility]``."""
        if self._lifecycle_manager is None:
            return {"status": 503, "error": "lifecycle_manager_not_wired"}
        ns = Namespace.from_parts(tuple(request["namespace"]))
        view = self._lifecycle_manager.get_state(ns)
        return {"status": 200, **view.model_dump(mode="json")}

    async def _route_ready_context(self, request: dict[str, Any]) -> dict[str, Any]:
        """``/ready-context`` -> ``MemoryLifecycleManager.ready_context(session_id)`` — instant
        warm read (spec §5). Same no-``await``-in-body guarantee as :meth:`_route_state`."""
        if self._lifecycle_manager is None:
            return {"status": 503, "error": "lifecycle_manager_not_wired"}
        rendered = self._lifecycle_manager.ready_context(str(request["session_id"]))
        return {"status": 200, **rendered.model_dump(mode="json")}

    # ---- memory-health + pinning (spec §7.1 / §7.2) -------------------------------------------
    # η on the wire is the same 5-part codec every other namespaced route uses
    # (``Namespace.parts()``/``.from_parts()``, CANONICAL §7.3), and the ``ClientScope`` the engine
    # services require is derived FROM that authorized η — see ``memory_health.local_scope`` for
    # why that is sound on THIS plane and nowhere else.

    async def _route_health(self, request: dict[str, Any]) -> dict[str, Any]:
        """``/health`` -> ``MemoryHealthService.assess`` — ONE bounded page of the caller's own
        memory health (spec §5.1). Read-pure: the service holds no write port at all, so this
        route structurally cannot reinforce, demote or otherwise mutate what it reports on.

        Content-free by construction: ``MemoryHealthView`` carries ids, enums, numbers and
        timestamps only (mu-core did not build the spec's ``preview`` field), so the dump below
        can never contain memory text.
        """
        if self._health is None:
            return unwired_response(HEALTH_UNWIRED)
        try:
            ns = namespace_on_the_wire(request)
        except (KeyError, TypeError, ValueError):
            return malformed_request_response()
        refusal = private_plane_refusal(ns)
        if refusal is not None:
            return refusal
        try:
            flags = parse_health_flags(request.get("flags"))
        except (TypeError, ValueError):
            return {"status": 422, "error": UNKNOWN_HEALTH_FLAG}
        cursor = request.get("cursor")
        view = await self._health.assess(
            local_scope(ns),
            ns,
            filter_flags=flags,
            cursor=str(cursor) if cursor is not None else None,
        )
        return {"status": 200, **view.model_dump(mode="json")}

    async def _route_pin(self, request: dict[str, Any]) -> dict[str, Any]:
        """``/pin`` -> ``PinService.pin`` — set the lifecycle-override so the item is never
        demoted / GC'd / auto-superseded (spec §5.2).

        ``reason`` is the spec's short NAMED classification ("policy", "decision"), bounded at 200
        chars by ``PinRequest`` — it is persisted on the item and is never carried on the bus, so
        it is passed through untouched and never logged here.
        """
        if self._pin is None:
            return unwired_response(PIN_UNWIRED)
        try:
            ns = namespace_on_the_wire(request)
            req = pin_request_of(request)
        except (KeyError, TypeError, ValueError):
            # ``ValidationError`` (a ``ValueError``) covers an empty ``memory_id`` and a ``reason``
            # past the contract's 200-char bound — both user-reachable through ``mu pin``.
            return malformed_request_response()
        refusal = private_plane_refusal(ns)
        if refusal is not None:
            return refusal
        try:
            result = await self._pin.pin(local_scope(ns), ns, req)
        except PIN_SURFACE_ERRORS as exc:
            return pin_failure_response(exc)
        return {"status": 200, **result.model_dump(mode="json")}

    async def _route_unpin(self, request: dict[str, Any]) -> dict[str, Any]:
        """``/unpin`` -> ``PinService.unpin`` — release the override (spec §5.2). Symmetric to
        :meth:`_route_pin`; reachable in EVERY state, because an item that reached a settled exit
        while pinned would otherwise be permanently un-GC-able."""
        if self._pin is None:
            return unwired_response(PIN_UNWIRED)
        try:
            ns = namespace_on_the_wire(request)
            memory_id = pin_request_of(request).memory_id
        except (KeyError, TypeError, ValueError):
            return malformed_request_response()
        refusal = private_plane_refusal(ns)
        if refusal is not None:
            return refusal
        try:
            result = await self._pin.unpin(local_scope(ns), ns, memory_id)
        except PIN_SURFACE_ERRORS as exc:
            return pin_failure_response(exc)
        return {"status": 200, **result.model_dump(mode="json")}


def _peer_is_self(writer: asyncio.StreamWriter) -> bool:
    sock: socket.socket | None = writer.get_extra_info("socket")
    if sock is None:
        return False
    creds = sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
    _pid, uid, _gid = struct.unpack("3i", creds)
    return bool(uid == os.getuid())


async def _respond(
    writer: asyncio.StreamWriter, payload: dict[str, Any], *, timeout_s: float
) -> None:
    """Write ONE newline-delimited JSON reply. The drain is bounded (``request_io_timeout_s``):
    a peer that stops reading must not pin this handler — see :meth:`IpcServer._handle`."""
    writer.write((json.dumps(payload) + "\n").encode("utf-8"))
    await asyncio.wait_for(writer.drain(), timeout=timeout_s)


def _sha256_of(record: dict[str, Any]) -> str:
    encoded = json.dumps(record, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
