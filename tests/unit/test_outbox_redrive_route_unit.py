"""``OUTBOX_REDRIVE_ROUTE`` — AD-270a fix (ADR 0072, PROTOTYPE-DEBT-0924.md B3).

``SqliteOutbox.redrive_dead`` was built, tested, and had NO CALLER — a dead-lettered capture was
unrecoverable. These tests prove the SURFACE (real ``IpcServer`` dispatch over a real, on-disk
``SqliteOutbox``; only the socket/network is out of scope per DEV-STANDARDS' unit-test carve-out),
end to end: dead-letter a real capture, redrive it through the route, confirm it is deliverable
again — the exact "prove it" the task asked for, at the unit tier (the daemon-level proof that a
redriven row actually reaches the store lives in ``tests/integration/test_outbox_redrive_int.py``).
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from mu_client.capture.model import ActivityKind, HostKind, RawActivity
from mu_client.config import DaemonIpcSettings
from mu_client.daemon.ipc import IpcServer
from mu_client.outbox.sqlite_outbox import OUTBOX_REDRIVE_ROUTE, SqliteOutbox

pytestmark = pytest.mark.unit


def _activity(offset: str = "off-1") -> RawActivity:
    return RawActivity(
        activity_id=f"act-{offset}",
        host=HostKind.CLAUDE_CODE,
        host_version="test",
        schema_version="claude_code.v1",
        kind=ActivityKind.USER_PROMPT,
        session_id="s1",
        occurred_at=datetime.now(UTC),
        text="hello",
        content_hash="deadbeef",
        source_offset=offset,
        provenance_id="prov_x",
    )


async def _server(tmp_path: Path, outbox: SqliteOutbox) -> IpcServer:
    return IpcServer(
        DaemonIpcSettings(socket_path=tmp_path / "d.sock"),
        registry=None,  # type: ignore[arg-type]  # unreached: this route touches no capture path
        outbox=outbox,
        bridge=None,  # type: ignore[arg-type]
    )


async def _dispatch(server: IpcServer, request: dict[str, Any]) -> dict[str, Any]:
    return await server._dispatch(request)  # exercising the real route table, module docstring


async def test_redrive_route_moves_dead_rows_back_to_pending(tmp_path: Path) -> None:
    """The end-to-end "prove it" scenario: append -> drain (delivery attempt) -> dead_letter
    (retries exhausted) -> the ROUTE (never the bare method) -> the row is PENDING again and the
    daemon's ordinary drain loop would pick it back up.

    **MUTATION:** delete the ``await self._outbox.redrive_dead(...)`` call in
    ``_route_outbox_redrive`` (return a fabricated ``{"status": 200, "redriven": 0}``) -> this
    test goes RED on ``reply["redriven"] == 1`` and on the post-condition depth/count asserts.
    """
    outbox = SqliteOutbox(tmp_path / "outbox.sqlite")
    await outbox.open()
    try:
        record = await outbox.append(_activity())
        await outbox.drain(batch_size=10)
        await outbox.dead_letter(record.seq, error="simulated permanent failure")
        assert await outbox.undelivered_count() == 1

        reply = await _dispatch(
            await _server(tmp_path, outbox), {"route": OUTBOX_REDRIVE_ROUTE, "limit": 10}
        )

        assert reply["status"] == 200
        assert reply["redriven"] == 1
        assert await outbox.undelivered_count() == 0
        assert await outbox.outbox_depth() == 1  # back in PENDING, deliverable again
    finally:
        await outbox.aclose()


async def test_redrive_route_is_bounded_by_limit(tmp_path: Path) -> None:
    """``limit`` genuinely bounds the redrive, oldest-first — never "redrive everything"."""
    outbox = SqliteOutbox(tmp_path / "outbox.sqlite")
    await outbox.open()
    try:
        records = [await outbox.append(_activity(offset=str(i))) for i in range(3)]
        await outbox.drain(batch_size=10)
        for record in records:
            await outbox.dead_letter(record.seq, error="boom")
        assert await outbox.undelivered_count() == 3

        reply = await _dispatch(
            await _server(tmp_path, outbox), {"route": OUTBOX_REDRIVE_ROUTE, "limit": 2}
        )

        assert reply["status"] == 200
        assert reply["redriven"] == 2
        assert await outbox.undelivered_count() == 1
    finally:
        await outbox.aclose()


@pytest.mark.parametrize("bad_limit", [None, 0, -1, "10", True])
async def test_redrive_route_rejects_a_malformed_limit(tmp_path: Path, bad_limit: object) -> None:
    """A malformed request is ANSWERED (module docstring: "this server never answers with
    silence"), never a raise that would close the socket with nothing sent back — and never
    silently coerced (``True`` looks like ``1`` to a bare ``isinstance(x, int)`` check, which is
    exactly the bug a bare check would hide)."""
    outbox = SqliteOutbox(tmp_path / "outbox.sqlite")
    await outbox.open()
    try:
        reply = await _dispatch(
            await _server(tmp_path, outbox),
            {"route": OUTBOX_REDRIVE_ROUTE, "limit": bad_limit},
        )
        assert reply["status"] == 400
    finally:
        await outbox.aclose()


async def test_redrive_route_on_an_empty_dead_letter_queue_is_a_no_op(tmp_path: Path) -> None:
    outbox = SqliteOutbox(tmp_path / "outbox.sqlite")
    await outbox.open()
    try:
        reply = await _dispatch(
            await _server(tmp_path, outbox), {"route": OUTBOX_REDRIVE_ROUTE, "limit": 10}
        )
        assert reply == {"status": 200, "redriven": 0}
    finally:
        await outbox.aclose()


async def test_unknown_route_is_unaffected(tmp_path: Path) -> None:
    """Dispatch-table addition regression guard: adding this route must not swallow the 404
    fallback for an unrelated unknown route."""
    outbox = SqliteOutbox(tmp_path / "outbox.sqlite")
    await outbox.open()
    try:
        reply = await _dispatch(await _server(tmp_path, outbox), {"route": "not-a-real-route"})
        assert reply["status"] == 404
    finally:
        await outbox.aclose()
