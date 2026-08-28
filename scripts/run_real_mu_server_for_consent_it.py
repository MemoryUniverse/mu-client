"""**Test harness — stands up a REAL ``mu-server`` for mu-client's D4 consent integration run.**

⚠ **This file is NOT part of ``mu_client``, is never imported by it, and lives outside ``src/`` on
purpose.** ``mu-client`` may not depend on ``mu-server`` (project ``CLAUDE.md``'s boundary rule;
``.importlinter``'s ``client-has-no-server`` contract; the grep backstop in
``tests/unit/test_import_boundaries.py``, which scans ``src/``). This script therefore runs in
**mu-server's own virtualenv, as a separate OS process**, and mu-client's test talks to it over
HTTP like any other client. No import crosses the boundary; only bytes on a socket do.

Run it exactly as ``tests/integration/test_agent_share_consent_int.py`` does::

    ../mu-server/.venv/bin/python scripts/run_real_mu_server_for_consent_it.py --port 18099

It prints one line — ``READY <base_url> <db_name>`` — once the app is serving, then serves until
SIGTERM, then drops its scratch database.

**Why a real server and not ``mu-sdk-python``'s conformance server.** A conformance server written
by the same lane that writes the client proves the two agree with each other. The D4 consent routes
are exactly where that is worth nothing: the receipt's ``state``, its ``unreachable`` residue list
and the 200-vs-204 split are *server judgements*, and the whole point of the client half is to
render them honestly. So this drives ``mu_server.app.build_app`` — the real composed app, the real
Valkey-backed consent store, the real Postgres trust ledger, the real Ed25519 signer.

**Centrifugo is deliberately DISABLED.** It is not running on this dev box, and the omission is
load-bearing rather than a compromise: with no push tier the real server reports
``CascadeResidue.CONTROL_FRAME_NOT_PUBLISHED`` on its receipt, which is precisely the kind of
server-authored residue the client must translate rather than invent. The test asserts on it.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import signal
import socket
import sys
import uuid

import asyncpg
import redis.asyncio as redis
import uvicorn
from fastapi import Request
from mu_server.app import build_app
from mu_server.auth import AuthContext
from mu_server.consent.ledger_pg import REQUIRED_DDL as LEDGER_DDL
from mu_server.deps import get_auth
from mu_server.rooms import room_log_pg, session_pg
from mu_server.settings import (
    CentrifugoSettings,
    ConsentSettings,
    GatewaySettings,
    PostgresSettings,
    ServerSettings,
)
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

#: Kept in sync with the constants in ``tests/integration/test_agent_share_consent_int.py``. They
#: are duplicated rather than shared because the two halves run in DIFFERENT virtualenvs.
ORG = "org-mu-client-d4-it"
WS = "ws-mu-client-d4-it"
OWNER = "prn-mu-client-d4-owner"
_SEED_B64 = base64.b64encode(bytes(range(32))).decode("ascii")


async def _purge_valkey_keys() -> None:
    """Delete every Valkey key belonging to this run's throwaway org.

    Uses ``SCAN`` (never ``KEYS``) so a shared dev Valkey is not blocked, and matches only the
    fixed test org id — nothing that could belong to another suite.

    ⚠ **Run ONE of these at a time.** The org id is a constant shared by every run of this harness
    (it has to be: the two halves live in different virtualenvs and cannot share a module), so two
    concurrent runs would purge each other's live keys on the first one's shutdown. That is the
    same "ONE pytest at a time across ALL repos" rule ``infra/mu-vm/vm_test.sh`` states for the
    shared VM stores, for the same reason.
    """
    valkey = ServerSettings().valkey
    client = redis.Redis(host=valkey.host, port=valkey.port, decode_responses=True)
    try:
        deleted = 0
        async for key in client.scan_iter(match=f"*{ORG}*", count=500):
            deleted += await client.delete(key)
        print(f"CLEANED {deleted} valkey keys", flush=True)  # noqa: T201 - operator visibility
    finally:
        await client.aclose()


async def _admin_execute(admin: PostgresSettings, sql: str) -> None:
    conn = await asyncpg.connect(
        host=admin.host,
        port=admin.port,
        user=admin.user,
        password=admin.password.get_secret_value(),
        database=admin.database,
        timeout=admin.connect_timeout_s,
    )
    try:
        await conn.execute(sql)
    finally:
        await conn.close()


async def _create_tables(scratch: PostgresSettings) -> None:
    """The three DDLs mu-server's own integration suite creates by hand.

    ⚠ Same REPORTED gap that suite records: ``room_log``/``room_session``/``room_participant`` and
    ``trust_ledger_chain`` are absent from ``mu-engine``'s ``schema.py`` and still need an additive
    Alembic revision (AD-28). Read from the module constants so this harness and the future
    migration cannot silently disagree.
    """
    engine = create_async_engine(scratch.dsn)
    try:
        async with engine.begin() as conn:
            for ddl in (room_log_pg.REQUIRED_DDL, session_pg.REQUIRED_DDL, LEDGER_DDL):
                for statement in ddl.split(";"):
                    if statement.strip() and not statement.strip().startswith("--"):
                        await conn.execute(text(statement))
    finally:
        await engine.dispose()


def _settings(scratch: PostgresSettings) -> ServerSettings:
    base = ServerSettings()
    return ServerSettings(
        postgres=scratch,
        # The gateway must be ENABLED — a real invariant, discovered by trying the opposite:
        # ``ServerSettings`` refuses ``MU_SERVER_GATEWAY__ENABLED=false`` outright, because *"an
        # edge-less plane authenticates nobody and revokes nothing, while it keeps verifying and
        # trusting any X-MU-Gateway-Assertion signed with its key."*
        #
        # ⚠ It is nevertheless BYPASSED for this run: ``app.state.client_gateway = None`` and the
        # ``get_auth`` override below mean no credential is ever presented or judged (the same
        # posture mu-server's own ``tests/integration/test_consent_revoke_int.py`` takes). So this
        # run proves the CONSENT routes against the real server; it proves nothing about
        # authentication, and the test says so in its own "what this does not prove" note.
        gateway=GatewaySettings(enabled=True, assertion_secret=base.gateway.assertion_secret),
        centrifugo=CentrifugoSettings(enabled=False),
        consent=ConsentSettings(signing_seed_b64=SecretStr(_SEED_B64)),
        valkey=base.valkey,
    )


async def _amain(port: int) -> int:
    admin = ServerSettings().postgres
    db_name = f"mu_client_d4_it_{uuid.uuid4().hex[:10]}"
    await _admin_execute(admin, f'CREATE DATABASE "{db_name}"')
    scratch = admin.model_copy(update={"database": db_name})
    try:
        await _create_tables(scratch)
        app = build_app(_settings(scratch))

        # ONE identity for the whole run — the owner, the only principal D4's affordance is
        # written for — but the SESSION is derived per request from the room in the path.
        #
        # ⚠ That derivation is not a shortcut around a check; it is what a real gateway assertion
        # would carry. ``routes/rooms.py:210-220`` reconciles the path's room id against the SIGNED
        # scope's ``session_id`` and answers the non-enumerating 404 when they differ — MEASURED:
        # a harness that pinned ``session`` to a constant made every room route 404, which is the
        # invariant behaving correctly. A long-lived server serving many rooms therefore has to
        # vary it, exactly as a per-room assertion would.
        def _auth_for_request(request: Request) -> AuthContext:
            parts = request.url.path.strip("/").split("/")
            room_id = parts[2] if len(parts) > 2 and parts[1] == "rooms" else "unscoped"
            return AuthContext(principal_id=OWNER, org=ORG, workspace=WS, session=room_id)

        app.dependency_overrides[get_auth] = _auth_for_request
        app.state.client_gateway = None

        # BIND THE SOCKET HERE, then hand it to uvicorn — do not ask uvicorn for the port after
        # the fact. With ``port=0``, ``server.servers[0].sockets[0].getsockname()[1]`` still reads
        # 0 (MEASURED: the READY line said ``http://127.0.0.1:0``, httpx resolved that zero port
        # to the default 80, and every request was answered by this box's Apache with a 400 — a
        # test talking to the WRONG server, which is worse than one that fails to start). A socket
        # this process bound itself is the only authoritative answer.
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", port))
        bound = sock.getsockname()[1]
        sock.listen(2048)

        config = uvicorn.Config(app, log_level="warning")
        server = uvicorn.Server(config)
        task = asyncio.create_task(server.serve(sockets=[sock]))
        while not server.started and not task.done():  # noqa: ASYNC110 - uvicorn
            # exposes `started` as a flag, not an event; polling it is the documented idiom and
            # this loop runs once at startup, not on any hot path.
            await asyncio.sleep(0.05)
        if task.done():  # startup failed — surface it rather than printing READY
            await task
            return 1
        print(f"READY http://127.0.0.1:{bound} {db_name}", flush=True)  # noqa: T201 - the protocol

        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
        await stop.wait()

        server.should_exit = True
        with contextlib.suppress(asyncio.CancelledError):
            await task
        return 0
    finally:
        # Clean up after ourselves — the shared dev stores are not a scratchpad (project rule:
        # "clean up anything you create in the stores; the disk is at 86%"). Postgres first (the
        # scratch database is this run's only relational footprint), then the Valkey keys the room
        # + consent stores wrote. Those keys DO carry a TTL (``grant_record_ttl_s`` defaults to a
        # week — ``consent/store.py:117-119``), so this is hygiene rather than correctness; a week
        # of dead keys per run on a shared box is still worth not leaving behind.
        await _admin_execute(admin, f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)')
        await _purge_valkey_keys()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, required=True)
    return asyncio.run(_amain(parser.parse_args().port))


if __name__ == "__main__":
    sys.exit(main())
