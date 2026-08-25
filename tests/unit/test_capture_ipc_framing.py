"""The capture fast path's FRAMING + REFUSAL semantics — a REAL :class:`IpcServer` on a REAL unix
socket, a REAL SQLite-WAL outbox per test, ZERO mocks (same tier justification as
``test_sqlite_outbox.py``: sockets and SQLite files are leaf adapters, not the mu-dev-* containers
the integration-only rule reaches for — nothing here needs a container, so nothing here is
marked ``integration``).

**What these guard.** ``capture_once`` skips its durability fallback whenever ``_try_daemon``
returns non-``None``. Before this file existed, ``_try_daemon`` never INSPECTED the daemon's reply:
an empty line (the connection closed with no response — what asyncio's 64 KiB ``StreamReader``
default does to an ordinary ``PostToolUse`` record) parsed to ``{}`` and was reported as a
success, and so were ``503 shutting_down`` (every daemon shutdown window) and ``401 foreign_uid``.
The activity was then silently dropped: no exception, no log, nothing on disk — breaking the
outbox's own contract that "the durability boundary is BEFORE the host is acked"
(``outbox/sqlite_outbox.py``).

**Provenance is observable here.** In production the daemon and the hook's fallback share ONE
outbox file; these tests give the daemon its own (``daemon-outbox.sqlite``) and the fallback the
configured client one (``client-outbox.sqlite``), so WHICH path ran is a fact about which file
holds the row — that is what makes the "genuine success does not double-write" control sharp.
Note that ``_direct_append_or_spool``'s first choice is a direct WAL append; the ``spool/``
directory is its own fallback-of-the-fallback (unparseable record / unusable outbox), so
"durably captured by the fallback" is asserted as a real row read back out of the real client
outbox file, and the spool is asserted EMPTY.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import sqlite3
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import NamedTuple

import pytest
import pytest_asyncio

from mu_client.capture.hook import capture_once
from mu_client.capture.model import HostKind
from mu_client.capture.parsers import ClaudeCodeParserV1, ParserRegistry
from mu_client.config import (
    CaptureSettings,
    ClientSettings,
    DaemonIpcSettings,
    InjectSettings,
    OutboxSettings,
)
from mu_client.daemon.ipc import IpcServer
from mu_client.host import LocalMemoryHost
from mu_client.inject.recall_bridge import RecallInjectBridge
from mu_client.outbox.sqlite_outbox import SqliteOutbox

pytestmark = pytest.mark.unit

_SESSION = "framing-s1"
#: asyncio's own ``StreamReader`` default (``_DEFAULT_LIMIT``) — the bound these tests exist to
#: prove the daemon no longer silently inherits.
_ASYNCIO_DEFAULT_LIMIT = 64 * 1024


def _hook_record(*, marker: str, payload_chars: int = 0, event: str = "PostToolUse") -> bytes:
    """A REAL Claude Code hook stdin payload (host-capture-integration-devdoc.md §2.1). The
    ``tool_response`` is the field that carries an untruncated tool result on the wire — the
    ``capture.tool_outcome_max_chars`` slice is taken DAEMON-side, after parsing, so the hook
    always sends the full thing."""
    return json.dumps(
        {
            "hook_event_name": event,
            "session_id": _SESSION,
            "cwd": "/home/user/D/mu_project/mu-client",
            "transcript_path": f"/home/user/.claude/projects/x/{_SESSION}.jsonl",
            "tool_name": "Read",
            "tool_use_id": marker,
            "tool_response": marker + ("x" * payload_chars),
        }
    ).encode("utf-8")


def _outbox_rows(path: Path) -> list[tuple[str, str, str]]:
    """Read the real SQLite outbox file back the way an operator would — no library in between."""
    if not path.exists():
        return []
    conn = sqlite3.connect(str(path))
    try:
        return [
            (str(r[0]), str(r[1]), str(r[2]))
            for r in conn.execute("SELECT activity_id, kind, activity_json FROM outbox").fetchall()
        ]
    finally:
        conn.close()


def _spool_files(settings: ClientSettings) -> list[Path]:
    spool_dir = settings.capture.spool_dir.expanduser()
    return sorted(spool_dir.glob("*.json")) if spool_dir.is_dir() else []


def _settings(tmp_path: Path, socket_path: Path, **ipc: object) -> ClientSettings:
    """Isolated on-disk everything: a fresh client outbox, spool dir and recall dir under
    ``tmp_path``, and a SHORT flat ``/tmp`` socket path (``AF_UNIX``'s ``sun_path`` caps at ~108
    bytes; pytest's nested ``tmp_path`` blows past that). NEVER the user's real
    ``~/.memory-universe``."""
    return ClientSettings().model_copy(
        update={
            "outbox": OutboxSettings(outbox_path=tmp_path / "client-outbox.sqlite"),
            "capture": CaptureSettings(spool_dir=tmp_path / "spool"),
            "inject": InjectSettings(recall_dir=tmp_path / "recall"),
            "ipc": DaemonIpcSettings(socket_path=socket_path, **ipc),
        }
    )


@pytest.fixture
def socket_path(uid: str) -> Path:
    path = Path(f"/tmp/mu-test-framing-{uid}.sock")  # noqa: S108 — deliberate, see _settings()
    yield path
    path.unlink(missing_ok=True)


class StartedDaemon(NamedTuple):
    """What a started test daemon exposes: its own settings, its OWN outbox file (so a row's
    provenance is observable, see the module docstring), and the real server object."""

    settings: ClientSettings
    outbox_path: Path
    server: IpcServer


@pytest_asyncio.fixture
async def daemon_ipc(
    tmp_path: Path, socket_path: Path
) -> AsyncIterator[Callable[..., Awaitable[StartedDaemon]]]:
    """Starts a REAL ``IpcServer`` (real parser registry, real SQLite outbox of its OWN) bound to
    the real socket, and hands the test back ``(settings, daemon_outbox_path)``.

    The ``bridge`` is a real :class:`RecallInjectBridge` over an UNSTARTED
    :class:`LocalMemoryHost` — ``LocalMemoryHost.__init__`` opens nothing (it builds the client's
    tracer/metric sinks and stores settings; every store connection happens in ``start()``), and
    the ``capture`` route under test never reaches the bridge. That is what keeps these tests
    container-free without faking a single object on the path being exercised.
    """
    started: list[tuple[IpcServer, SqliteOutbox]] = []

    async def _start(**ipc: object) -> StartedDaemon:
        settings = _settings(tmp_path, socket_path, **ipc)
        daemon_outbox_path = tmp_path / "daemon-outbox.sqlite"
        outbox = SqliteOutbox(daemon_outbox_path)
        await outbox.open()
        registry = ParserRegistry()
        registry.register(
            ClaudeCodeParserV1(tool_outcome_max_chars=settings.capture.tool_outcome_max_chars)
        )
        ipc_server = IpcServer(
            settings.ipc,
            registry=registry,
            outbox=outbox,
            bridge=RecallInjectBridge(LocalMemoryHost(settings), settings=settings.inject),
        )
        await ipc_server.bind()  # start_unix_server serves as soon as it is bound
        started.append((ipc_server, outbox))
        return StartedDaemon(settings, daemon_outbox_path, ipc_server)

    try:
        yield _start
    finally:
        for ipc_server, outbox in started:
            with contextlib.suppress(Exception):
                await ipc_server.stop_accepting()
            with contextlib.suppress(Exception):
                await outbox.aclose()


# ----------------------------------------------------------------- 1. the oversized record itself
async def test_record_larger_than_asyncios_default_limit_is_captured_not_dropped(
    daemon_ipc: Callable[..., Awaitable[StartedDaemon]],
) -> None:
    """THE BUG. A ``PostToolUse`` whose ``tool_response`` pushes the JSON line past asyncio's
    64 KiB default overran the daemon's ``readline()``; the connection closed with NO response and
    the activity vanished. It must survive — and here it survives via the daemon itself, because
    ``max_request_bytes`` is now what frames the socket."""
    settings, daemon_outbox_path, _ = await daemon_ipc()
    raw = _hook_record(marker="oversized-1", payload_chars=80_000)
    assert len(raw) > _ASYNCIO_DEFAULT_LIMIT, "payload must exceed the limit it is testing"

    response = await capture_once(settings, host=HostKind.CLAUDE_CODE, raw=raw)

    assert response == {}  # PostToolUse is not a dual-purpose event: no injection, just capture
    daemon_rows = _outbox_rows(daemon_outbox_path)
    assert len(daemon_rows) == 1, f"the {len(raw)}-byte record never reached the daemon's outbox"
    assert "oversized-1" in daemon_rows[0][2], "the row is not the record we sent"
    assert _outbox_rows(settings.outbox.outbox_path) == []  # daemon took it; no second write
    assert _spool_files(settings) == []


# --------------------------------------------------- 2. past the CONFIGURED limit -> still durable
async def test_record_past_the_configured_limit_is_refused_and_kept_by_the_fallback(
    daemon_ipc: Callable[..., Awaitable[StartedDaemon]],
) -> None:
    """Every limit has a far side. A record past ``max_request_bytes`` gets an explicit ``413``
    (never a silent close), and the hook must then treat it exactly like an unreachable daemon:
    keep it locally. Retrievability is asserted by reading the row back out of the real client
    outbox file."""
    settings, daemon_outbox_path, _ = await daemon_ipc(max_request_bytes=_ASYNCIO_DEFAULT_LIMIT)
    raw = _hook_record(marker="too-big-1", payload_chars=200_000)

    response = await capture_once(settings, host=HostKind.CLAUDE_CODE, raw=raw)

    assert response == {}
    assert _outbox_rows(daemon_outbox_path) == [], "the daemon cannot have framed this record"
    client_rows = _outbox_rows(settings.outbox.outbox_path)
    assert len(client_rows) == 1, "a 413-refused record was dropped instead of kept locally"
    assert "too-big-1" in client_rows[0][2]
    assert client_rows[0][1] == "tool_use"


# ------------------------------------------------------------------ 3. the likeliest refusal: 503
async def test_daemon_503_shutting_down_falls_back_to_a_durable_local_append(
    daemon_ipc: Callable[..., Awaitable[StartedDaemon]],
) -> None:
    """``503 shutting_down`` is a REFUSAL, not an error: the daemon is in its ordered shutdown and
    holds nothing, so the correct response is byte-identical to the daemon being unreachable —
    keep the record locally. This is the likeliest real-world trigger of the whole defect (every
    daemon restart has this window), and it was silently discarding captures.

    The server is put in the REAL state ``stop_accepting()`` leaves it in (``_accepting`` false)
    without also closing the listener — closing it would make the client fail to CONNECT, which
    exercises the long-standing unreachable path instead of the 503 branch under test."""
    settings, daemon_outbox_path, ipc_server = await daemon_ipc()
    ipc_server._accepting = False  # the real shutting-down state — see this test's docstring

    response = await capture_once(
        settings, host=HostKind.CLAUDE_CODE, raw=_hook_record(marker="shutdown-1")
    )

    assert response == {}
    assert _outbox_rows(daemon_outbox_path) == []
    client_rows = _outbox_rows(settings.outbox.outbox_path)
    assert len(client_rows) == 1, "a 503 shutting_down capture was silently discarded"
    assert "shutdown-1" in client_rows[0][2]


# --------------------------------------------------------------------------- 4. the 401 refusal
async def test_foreign_uid_401_is_not_treated_as_a_successful_capture(
    tmp_path: Path, socket_path: Path
) -> None:
    """``ipc.py``'s ``401 foreign_uid`` cannot be provoked from this test's own uid — that is the
    entire point of the ``SO_PEERCRED`` check. So the peer here is a real unix-socket server that
    replies with the exact line ``IpcServer._handle`` writes on that branch, and the assertion is
    on the CLIENT's handling of it: a rejected connection is not a captured activity."""
    settings = _settings(tmp_path, socket_path)
    async with _canned_responder(socket_path, {"status": 401, "error": "foreign_uid"}):
        response = await capture_once(
            settings, host=HostKind.CLAUDE_CODE, raw=_hook_record(marker="foreign-1")
        )

    assert response == {}
    client_rows = _outbox_rows(settings.outbox.outbox_path)
    assert len(client_rows) == 1, "a 401-refused capture was treated as a success and lost"
    assert "foreign-1" in client_rows[0][2]


# ---------------------------------------------------------- 5. the control: no over-eager spooling
async def test_genuine_success_takes_the_fast_path_and_does_not_double_write(
    daemon_ipc: Callable[..., Awaitable[StartedDaemon]],
) -> None:
    """The CONTROL for tests 1-4: an implementation that simply always fell back would satisfy
    every one of them and would be wrong. A ``200`` from the daemon means the record is already
    durable in the daemon's outbox, so the hook must NOT append it a second time — the fast path
    exists, and re-appending would double-write every captured activity in production (where both
    sides share one outbox file)."""
    settings, daemon_outbox_path, _ = await daemon_ipc()

    response = await capture_once(
        settings, host=HostKind.CLAUDE_CODE, raw=_hook_record(marker="happy-1")
    )

    assert response == {}
    daemon_rows = _outbox_rows(daemon_outbox_path)
    assert len(daemon_rows) == 1, "the daemon fast path did not run"
    assert "happy-1" in daemon_rows[0][2]
    assert _outbox_rows(settings.outbox.outbox_path) == [], "fallback double-wrote a captured row"
    assert _spool_files(settings) == []


# ------------------------------------------------------- 6. a silent peer must not pin the handler
async def test_peer_that_sends_nothing_is_released_and_never_blocks_shutdown(
    daemon_ipc: Callable[..., Awaitable[StartedDaemon]], socket_path: Path
) -> None:
    """The server's ``readline()`` had no timeout, so a peer that connects and never sends ``\\n``
    pinned its handler forever — and ``stop_accepting()`` -> ``wait_closed()`` (3.12 waits for
    live handlers) then hung the whole ordered shutdown behind it. Note the asymmetry this fixes:
    the client has bounded its own read since day one."""
    _, _, ipc_server = await daemon_ipc(request_io_timeout_s=0.25)

    reader, writer = await asyncio.open_unix_connection(path=str(socket_path))
    try:
        # The handler must release itself: EOF arrives without us ever sending a request line.
        eof = await asyncio.wait_for(reader.read(), timeout=5.0)
        assert eof == b"", "a silent peer's handler was never released"
        # ...and ordered shutdown step 1 must then complete rather than block on that handler.
        await asyncio.wait_for(ipc_server.stop_accepting(), timeout=5.0)
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


# ------------------------------------------------------------------------------------- test utils
@contextlib.asynccontextmanager
async def _canned_responder(
    socket_path: Path, reply: dict[str, object]
) -> AsyncIterator[asyncio.AbstractServer]:
    """A real unix-socket peer that reads one request line and answers ``reply`` verbatim — the
    same newline-delimited-JSON wire shape ``IpcServer`` speaks. Not a mock of anything under
    test: the code under test is the CLIENT, and this is the wire it reads."""

    async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await reader.readline()
            writer.write((json.dumps(reply) + "\n").encode("utf-8"))
            await writer.drain()
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    server = await asyncio.start_unix_server(_handle, path=str(socket_path))
    try:
        yield server
    finally:
        server.close()
        await server.wait_closed()
