"""**THE cross-plane privacy invariant — the gate that was named in Phase 1 and never written.**

``MU-SERVER-BUILD-PLAN.md`` §2, invariant 6:

    *The privacy invariant is an ACCEPTANCE TEST, not a hope — a private local fact must produce
    zero server request, node, vector, edge, or placeholder. This test is written in Phase 1 (where
    it is trivially true) and must still pass at the end of every later phase.*

``mu-server``'s ``tests/acceptance/test_privacy_invariant.py`` already asserts the **single-plane,
structural** half of that sentence: mu-server cannot import the on-device planes, and every
namespace it can build is SHARED. What no test asserted is the **cross-plane behavioural** half —
the half the sentence actually makes: *a private local fact, captured on the client, produces zero
server artifact.* That claim needs two planes running at once, and it is the crown jewel of the
design (``SERVER-AND-COLLAB-DESIGN-REVIEW.md``), so it is asserted here against a REAL client and a
REAL server over a REAL socket, with REAL stores. Zero mocks.

--------------------------------------------------------------------------------------------
The instrument, and why each half of it exists
--------------------------------------------------------------------------------------------
``mu-client`` may not import ``mu-server``, so the server runs as a **separate OS process in
mu-server's own virtualenv** — ``mu-server/tests/acceptance/privacy_cross_plane_server.py``, which
composes the real ``mu_server.app.build_app`` over a **freshly created, empty** scratch Postgres
database and a **freshly generated** org/workspace, and wraps the ASGI app in a request recorder
that logs every inbound connection *at the door* (before routing, before auth, before any 404).

The client then captures a private fact through the real local plane — the installed ``mu``
console script, as a real subprocess, against the real Valkey/Qdrant/FalkorDB stores.

**The client process is fully CONFIGURED AND ABLE to reach the server.**
``MU_CONSENT__SERVER_BASE_URL`` and ``MU_CONSENT__API_TOKEN`` are exported into the capturing
process's environment, pointing at the live server. This is the load-bearing detail: without it,
"zero server requests" would be a statement about a missing address rather than about the client's
behaviour. The address is present, reachable and authenticated — and a private capture still says
nothing to it.

--------------------------------------------------------------------------------------------
"Nothing leaked" vs. "nothing happened" — the three positive controls
--------------------------------------------------------------------------------------------
An all-zeros observation is worthless unless the observer is proven able to see a one. So the same
run, with the same instruments, also proves:

* **PC-1 — the fact really was captured.** The identical Valkey/Qdrant footprint function that
  reports *empty* for the server plane reports *non-empty* for the client's own PRIVATE partition,
  and a real recall returns the body. A green here cannot mean the capture no-oped.
* **PC-2 — the request log is armed.** One deliberate ``mu agent-share status`` from the same
  client binary, in the same environment, appends exactly one line to the server's request log.
  The recorder was listening the whole time.
* **PC-3 — the server-side observers are armed, BOTH of them.** One real ``POST
  /v1/rooms/{id}/open`` turns the server's "every table has zero rows" reading into a non-zero
  one, and one real ``POST /v1/rooms/{id}/heartbeat`` turns the server plane's *store* footprint —
  the very reading phase 2 asserts is empty, taken with the very same arguments — into a non-empty
  one. Without that second half the store reading could not tell *"nothing leaked"* from *"I
  looked in the wrong place"*: REPRODUCED 2026-08-28, the whole test still passed with the
  server-plane coordinates replaced by ``skeptic-bogus-org``/``skeptic-bogus-ws``.

--------------------------------------------------------------------------------------------
What this run does NOT prove — stated so nobody reads more into a green than is there
--------------------------------------------------------------------------------------------
* **Authentication.** The harness overrides ``get_auth`` and sets ``app.state.client_gateway =
  None`` (mu-server's own integration suite's posture). This run is about what the client SENDS,
  not about what the edge would judge.
* **Egress the server never sees.** The observation point is the server's door. A client that
  shipped a private fact to some *other* host would not appear in this log; that is a different
  invariant (and mu-client's only outbound HTTP surface is ``consent/client.py``'s two routes,
  which this run drives on purpose).
* **The daemon capture path.** The fact is captured through the daemonless one-shot path (the
  ``mu`` console script). The hook/outbox/worker path shares the same ``LocalMemory`` verb but is
  not separately exercised here.
* **The server-plane store control arms the VALKEY arm, and the addressing for all three.**
  MEASURED against this very harness: the shared plane's only memory-store write on this surface
  is ``ValkeyPresenceStore``'s η-scoped ``mu:presence:{org}:{workspace}:{room}``; ``open``,
  ``heartbeat`` and ``messages`` create no Qdrant collection and no FalkorDB graph at all. So the
  control proves (a) the Valkey probe can see a key in the SERVER plane and (b) the org/workspace
  pair the whole reading is addressed by is the LIVE server tenancy — which is what the bogus-org
  reproduction destroyed. The Qdrant and FalkorDB probes are proven able to see a one on the
  CLIENT plane (PC-1 and the graph re-read below), and they are addressed by
  ``tenant_partition_digest`` of that same now-proven-live pair. A server-side vector/graph write
  does not exist to be observed today; when one does, this control should be extended to it.
* **The GRAPH arm is armed at PARTITION grain, not at edge grain.** MEASURED: a salient ``add``
  writes STM + MTM (``tiers_written=stm,mtm``) and no graph at all; the client's LTM partition
  ``mu_g__{digest}__u_{user}`` materializes on the recall, and PC-1 re-reads the footprint there to
  prove the falkor probe can see a partition when one exists. No graph NODE or EDGE is written by
  this run, so "zero server nodes/edges" is argued from "zero server graph partitions". Tightening
  that needs a capture that drives LTM extraction; it is REPORTED rather than papered over.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import subprocess
import sys
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from pathlib import Path

import asyncpg  # type: ignore[import-untyped]  # no py.typed marker
import httpx
import pytest
import pytest_asyncio
from falkordb.asyncio import FalkorDB
from mu_contracts.contracts.recall import RecallResult
from mu_engine.storage.domain.namespace import Namespace, Visibility
from mu_engine.storage.mappers.tenancy import tenant_partition_digest
from pydantic import BaseModel, ConfigDict
from qdrant_client import AsyncQdrantClient
from redis.asyncio import Redis

from mu_client.config import ClientSettings
from mu_client.host import daemonless_host

pytestmark = pytest.mark.integration

_REPO = Path(__file__).resolve().parents[2]
#: The harness lives in **mu-server**, not here: it imports ``mu_server``, and this repo's own
#: boundary gate (``tests/unit/test_import_boundaries.py``) scans the whole repo and permits exactly
#: one argued exemption. A cross-plane test is not a reason to widen that list — see the harness's
#: own docstring for the full argument.
_HARNESS = _REPO.parent / "mu-server" / "tests" / "acceptance" / "privacy_cross_plane_server.py"
#: mu-server's own venv interpreter. Absent = the run is BLOCKED and reported, never faked
#: (DEV-STANDARDS: *"if it can't be stood up, the test is BLOCKED (reported), never faked"*).
_SERVER_PY = Path(
    os.environ.get(
        "MU_PRIVACY_IT_SERVER_PY", str(_REPO.parent / "mu-server" / ".venv" / "bin" / "python")
    )
)
#: ⚠ A skip is the right answer for a developer without the sibling repo and the WRONG answer for
#: CI: this is the ONLY test of §2 invariant 6's cross-plane half, and a silent skip makes its
#: absence indistinguishable from its success. Set ``MU_REQUIRE_PRIVACY_IT=1`` wherever the run is
#: supposed to be possible and the skip becomes a FAILURE naming what is missing.
_REQUIRED = os.environ.get("MU_REQUIRE_PRIVACY_IT", "").strip().lower() in {"1", "true", "yes"}

_STARTUP_TIMEOUT_S = 120.0
_USER = "u1"
_SESSION = "s1"
#: A fact whose whole point is that it is nobody's business but this device's.
_PRIVATE_FACT = "Mira's therapy appointment is every Tuesday at 18:00 with Dr. Okonjo"
#: Drives the REAL ``DeterministicPromoteStage`` gate (``importance >= 0.6``) above threshold, so
#: the capture reaches the MTM vector tier as well as STM — a bigger, more visible footprint for
#: the negative assertion to have to find nothing of.
_SALIENT = 0.9


def _blocked(reason: str) -> None:
    """Skip, or FAIL when this run was declared possible. One place the decision is made."""
    if _REQUIRED:
        pytest.fail(f"MU_REQUIRE_PRIVACY_IT is set but the cross-plane privacy run is {reason}")
    pytest.skip(f"BLOCKED: {reason}")


class ServerPlane(BaseModel):
    """Everything the test needs to observe the running server plane."""

    model_config = ConfigDict(frozen=True)

    base_url: str
    database: str
    org: str
    workspace: str
    request_log: Path


class Footprint(BaseModel):
    """What one tenancy partition physically occupies in the three memory stores.

    ONE function builds this for BOTH planes, so the reading that says *"the server plane is
    empty"* is produced by the same code, against the same live stores, as the reading that says
    *"the client's private partition is not"*. An observer that can see one can see the other.
    """

    model_config = ConfigDict(frozen=True)

    valkey_keys: tuple[str, ...]
    qdrant_points: tuple[tuple[str, int], ...]
    falkor_graphs: tuple[str, ...]

    @property
    def total(self) -> int:
        return (
            len(self.valkey_keys)
            + sum(count for _, count in self.qdrant_points)
            + len(self.falkor_graphs)
        )

    def describe(self) -> str:
        return (
            f"valkey={list(self.valkey_keys)} qdrant={list(self.qdrant_points)} "
            f"falkor={list(self.falkor_graphs)}"
        )


# ==================================================================================================
# Observers — the store side
# ==================================================================================================
async def _footprint(
    settings: ClientSettings, org: str, workspace: str, *, visibility: Visibility | None
) -> Footprint:
    """Every physical artifact the three memory stores hold for ``(org, workspace)``.

    Addressed the way the adapters themselves address the stores, never by guessing:
    ``Namespace.to_prefix()`` for the Valkey key-space, ``tenant_partition_digest`` for the Qdrant
    collection and FalkorDB graph names (``qdrant_mapper.collection_name`` /
    ``falkor_ltm.graph_name_for`` are built from exactly that digest — the digest is a function of
    ``org``+``workspace`` only, so the ``visibility`` used to build the probe namespace does not
    change it).

    ``visibility=None`` asks the STRICTEST question — *does this tenancy occupy anything at all,
    under any visibility, under any key shape?* — and is what the SERVER plane is asked, because
    that plane's org is generated fresh for this run and must own nothing whatsoever. A concrete
    visibility narrows the reading to that one slice, which is what the CLIENT's own tenancy is
    asked twice: its PRIVATE arm must be occupied, its SHARED arm must be empty.
    """
    ns = Namespace(
        org=org,
        workspace=workspace,
        user=_USER if visibility is not Visibility.SHARED else "*",
        session=_SESSION,
        visibility=visibility or Visibility.PRIVATE,
    )
    digest = tenant_partition_digest(ns)

    def in_slice(name: str) -> bool:
        if visibility is None:
            return True
        return f"/{visibility.value}/" in name or f"__{visibility.value}__" in name

    redis: Redis = Redis.from_url(settings.storage.cache.url, decode_responses=True)
    try:
        keys = tuple(
            sorted([k async for k in redis.scan_iter(match=f"*{org}*", count=500) if in_slice(k)])
        )
    finally:
        await redis.aclose()

    qdrant = AsyncQdrantClient(url=settings.storage.vector.url)
    try:
        points: list[tuple[str, int]] = []
        for coll in (await qdrant.get_collections()).collections:
            if digest in coll.name and in_slice(coll.name):
                points.append((coll.name, (await qdrant.count(coll.name, exact=True)).count))
    finally:
        await qdrant.close()

    db = FalkorDB(host=settings.storage.graph.host, port=settings.storage.graph.port)
    try:
        # Graph names are ``mu_g__{digest}__shared`` / ``mu_g__{digest}__u_{user}``
        # (``storage/migrations/naming.py:132-133``) — the PRIVATE arm's marker is ``__u_``.
        marker = {None: "", Visibility.SHARED: "__shared", Visibility.PRIVATE: "__u_"}[visibility]
        listed = [g.decode() if isinstance(g, bytes) else g for g in await db.list_graphs()]
        graphs = tuple(sorted(n for n in listed if digest in n and marker in n))
    finally:
        with contextlib.suppress(Exception):
            await db.connection.aclose()

    return Footprint(valkey_keys=keys, qdrant_points=tuple(points), falkor_graphs=graphs)


async def _relational_rows(plane: ServerPlane, settings: ClientSettings) -> dict[str, int]:
    """Row counts for every table in the server's scratch database.

    The database was CREATED for this run and nothing has written to it, so this is not a diff
    against noise: every row is, by construction, a server-side artifact of something that
    happened during the run.
    """
    pg = settings.storage.postgres
    conn = await asyncpg.connect(
        host=pg.host,
        port=pg.port,
        user=pg.user,
        password=pg.password.get_secret_value(),
        database=plane.database,
        timeout=pg.store_io_timeout_s,
    )
    try:
        tables = await conn.fetch(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
        )
        counts: dict[str, int] = {}
        for row in tables:
            name = row["tablename"]
            counts[name] = await conn.fetchval(f'SELECT count(*) FROM public."{name}"')  # noqa: S608
        return counts
    finally:
        await conn.close()


def _requests(plane: ServerPlane) -> list[dict[str, object]]:
    """The SERVER's own request log — one JSON line per inbound connection, recorded at the door."""
    raw = plane.request_log.read_text(encoding="utf-8")
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


# ==================================================================================================
# Fixtures
# ==================================================================================================
@pytest.fixture(scope="module")
def server_plane(tmp_path_factory: pytest.TempPathFactory) -> Iterator[ServerPlane]:
    """Launch the REAL mu-server with its recorder; yield what is needed to observe the plane."""
    if not _SERVER_PY.exists():
        _blocked(f"impossible: no mu-server venv at {_SERVER_PY} — cannot stand up a real server")
    request_log = tmp_path_factory.mktemp("privacy-it") / "requests.jsonl"
    proc = subprocess.Popen(  # noqa: S603 — fixed argv, resolved interpreter, no shell
        [str(_SERVER_PY), str(_HARNESS), "--port", "0", "--request-log", str(request_log)],
        cwd=str(_REPO.parent / "mu-server"),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    plane: ServerPlane | None = None
    try:
        assert proc.stdout is not None
        started = time.monotonic()
        while time.monotonic() - started < _STARTUP_TIMEOUT_S:
            line = proc.stdout.readline()
            if not line:
                break
            if line.startswith("READY "):
                _, base_url, database, org, workspace = line.split()
                plane = ServerPlane(
                    base_url=base_url,
                    database=database,
                    org=org,
                    workspace=workspace,
                    request_log=request_log,
                )
                break
        if plane is None:
            proc.kill()
            _blocked("not READY (the real mu-server did not start — stores unreachable?)")
            raise AssertionError("unreachable — _blocked always raises")  # narrows for mypy
        yield plane
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=60)
        except subprocess.TimeoutExpired:  # pragma: no cover - defensive
            proc.kill()


@pytest_asyncio.fixture
async def client_plane(client_settings: ClientSettings, uid: str) -> AsyncIterator[ClientSettings]:
    """A ClientSettings on its own η partition; teardown drops everything the run created."""
    settings = client_settings.model_copy(
        update={"default_workspace": f"pws{uid}", "default_namespace": f"porg{uid}"}
    )
    try:
        yield settings
    finally:
        await _teardown(settings, uid)


async def _teardown(settings: ClientSettings, uid: str) -> None:
    """Drop everything this run created. ⚠ Matched on the tenancy DIGEST, not on ``uid``.

    MEASURED: a ``uid``-substring teardown (the shape the older integration tests use) removes
    NOTHING from Qdrant or FalkorDB, because collection and graph names are
    ``mu_mtm__{digest}__…`` / ``mu_g__{digest}__…`` — the org/workspace are hashed out of the name
    by ``tenant_partition_digest``. It left ``mu_mtm__ed5d03a0df036d99__private__384`` behind on a
    shared dev box. Valkey keys DO carry the org in the clear, so ``uid`` is right there.
    """
    digest = tenant_partition_digest(
        Namespace(
            org=settings.default_namespace,
            workspace=settings.default_workspace,
            user=_USER,
            session=_SESSION,
            visibility=Visibility.PRIVATE,
        )
    )

    qdrant = AsyncQdrantClient(url=settings.storage.vector.url)
    try:
        for coll in (await qdrant.get_collections()).collections:
            if digest in coll.name or uid in coll.name:
                with contextlib.suppress(Exception):
                    await qdrant.delete_collection(coll.name)
    finally:
        await qdrant.close()

    db = FalkorDB(host=settings.storage.graph.host, port=settings.storage.graph.port)
    try:
        for g in await db.list_graphs():
            name = g.decode() if isinstance(g, bytes) else g
            if digest in name or uid in name:
                with contextlib.suppress(Exception):
                    await db.select_graph(name).delete()
    finally:
        with contextlib.suppress(Exception):
            await db.connection.aclose()

    redis: Redis = Redis.from_url(settings.storage.cache.url, decode_responses=False)
    try:
        keys = [k async for k in redis.scan_iter(match=f"*{uid}*".encode())]
        if keys:
            await redis.delete(*keys)
    finally:
        await redis.aclose()


def _client_env(plane: ServerPlane, settings: ClientSettings) -> dict[str, str]:
    """The environment the capturing client process runs in.

    ⚠ The two ``MU_CONSENT__*`` lines are the load-bearing part of this whole test: they make the
    client **able** to reach the server. "Zero requests" from a client that has no server address
    is a fact about configuration; "zero requests" from a client holding a live URL and a token is
    a fact about the client.
    """
    return {
        **os.environ,
        "MU_DEFAULT_WORKSPACE": settings.default_workspace,
        "MU_DEFAULT_NAMESPACE": settings.default_namespace,
        "MU_CONSENT__SERVER_BASE_URL": plane.base_url,
        "MU_CONSENT__API_TOKEN": "privacy-it-token",
    }


async def _run_client_cli(
    args: list[str], env: dict[str, str], *, timeout_s: float
) -> tuple[int, str, str]:
    """Run the installed client entrypoint as a real child process, without blocking the loop.

    ``asyncio.create_subprocess_exec`` rather than ``subprocess.run`` — DEV-STANDARDS' fully-async
    rule binds test code too, and a blocking call here would stall the very event loop the
    observers below poll the stores on.
    """
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "mu_client",
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout_s)
    except TimeoutError:  # pragma: no cover - defensive
        proc.kill()
        raise
    return proc.returncode or 0, out.decode(errors="replace"), err.decode(errors="replace")


async def _eventually(read: Callable[[], Awaitable[RecallResult]]) -> RecallResult:
    """Poll a recall until it returns hits (Qdrant applies upserts asynchronously — the store's real
    eventual-consistency model, NOT a masked bug; bounded so a genuine empty still fails)."""
    last = await read()
    for _ in range(40):  # ~8s ceiling
        if last.items:
            return last
        await asyncio.sleep(0.2)
        last = await read()
    return last


# ==================================================================================================
# THE GATE
# ==================================================================================================
async def test_a_private_local_fact_produces_zero_server_artifact(
    server_plane: ServerPlane, client_plane: ClientSettings, uid: str
) -> None:
    """``MU-SERVER-BUILD-PLAN.md`` §2 invariant 6, end to end, across two live planes.

    Deliberately ONE test rather than four: the negative assertions and the positive controls that
    arm the observers have to hold **in the same run**, or a later edit could leave the gate green
    with a blind instrument — which is exactly the failure mode this invariant cannot afford.

    **MUTATIONS PROVEN** — each planted for real against the live pair of planes, watched RED, and
    reverted (2026-08-28):

    * **M1, a real leak.** ``LocalMemoryHost.add`` POSTs the captured content to the configured
      server after the local write. RED at phase 2: *"the server's own door recorded [{'method':
      'POST', 'path': '/v1/memories', 'content_length': '82', ...}]"*.
    * **M2, a capture that silently does nothing.** ``LocalMemoryHost.add`` returns a well-formed
      receipt without writing. RED at **PC-1**, not at phase 2 — which is the whole point: the
      server reading was still all zeros, and the run refused to call that a pass.
    * **M3, a placeholder in the shared key-space.** The client writes one Valkey key under
      ``mu/{org}/{ws}/shared/*/{session}``. RED at phase 3, naming the key.
    * **M4, a blinded observer.** The harness's ``RequestRecorder`` stops writing lines. RED at
      **PC-2** — an all-zero request log that means "the recorder is dead" is caught and separated
      from one that means "nothing was sent".
    * **M5, an observer pointed at the wrong plane.** ``_server_footprint`` addressed by
      ``"skeptic-bogus-org"``/``"skeptic-bogus-ws"`` instead of the coordinates the harness printed
      on its READY line. This was a REAL DEFECT, not a hypothetical: before **PC-3** grew its store
      half the mutated run PASSED in 314s, because the server-plane reading is all zeros for any
      tenancy nothing has ever written under. It is now RED at PC-3.
    * **M6, a positive control that does not actually write.** PC-3's ``heartbeat`` call removed,
      leaving only the ``open`` — the shape this control was first specified with. RED at PC-3's
      store arm, which is the point: MEASURED, ``open`` writes Postgres ONLY, so arming the store
      observer on it would have re-created M5's blindness while looking like a control.
    """

    # ONE closure, used for the baseline reading, for the INVARIANT's reading and for the
    # positive control in PC-3, so the negative assertion and the control that arms it cannot be
    # addressed at two different places by a later edit — the exact failure M5 records.
    async def _server_footprint() -> Footprint:
        return await _footprint(
            client_plane, server_plane.org, server_plane.workspace, visibility=None
        )

    # ---- Phase 0 — baseline: the server plane starts with nothing at all -------------------
    assert _requests(server_plane) == [], "the request log was not empty before the run started"
    baseline_rows = await _relational_rows(server_plane, client_plane)
    assert baseline_rows, "no tables in the server's scratch database — the harness DDL did not run"
    assert sum(baseline_rows.values()) == 0, f"scratch database was not empty: {baseline_rows}"
    server_before = await _server_footprint()
    assert (
        server_before.total == 0
    ), f"server plane was not empty at baseline: {server_before.describe()}"

    # ---- Phase 1 — capture a PRIVATE fact on the client, in a process that CAN reach the server
    env = _client_env(server_plane, client_plane)
    rc, stdout, stderr = await _run_client_cli(
        [
            "add",
            _PRIVATE_FACT,
            "--user",
            _USER,
            "--session",
            _SESSION,
            "--importance",
            str(_SALIENT),
        ],
        env,
        timeout_s=300.0,
    )
    assert rc == 0, stderr
    assert "memory_id=" in stdout, stdout
    print(f"CAPTURED (client, PRIVATE): {stdout.strip()}")  # noqa: T201 — required evidence

    # ---- Phase 2 — THE INVARIANT: zero server request, and zero server-side artifact ---------
    leaked = _requests(server_plane)
    assert leaked == [], (
        "PRIVACY INVARIANT VIOLATED — a private local capture produced server request(s). "
        f"The server's own door recorded: {leaked}"
    )
    after_rows = await _relational_rows(server_plane, client_plane)
    assert sum(after_rows.values()) == 0, (
        "PRIVACY INVARIANT VIOLATED — a private local capture produced rows in the server's "
        f"database (node/edge/placeholder): {[(t, n) for t, n in after_rows.items() if n]}"
    )
    server_after = await _server_footprint()
    assert server_after.total == 0, (
        "PRIVACY INVARIANT VIOLATED — a private local capture produced an artifact in the SERVER "
        f"plane's store partition: {server_after.describe()}"
    )

    # ---- Phase 3 — nothing the CLIENT wrote landed in a SHARED partition either --------------
    # The invariant names "placeholder": a marker written into the shared key-space under the
    # client's OWN tenancy would be invisible to the server-side reading above, so it is asked
    # about separately, in the client's own org.
    client_shared = await _footprint(
        client_plane,
        client_plane.default_namespace,
        client_plane.default_workspace,
        visibility=Visibility.SHARED,
    )
    assert client_shared.total == 0, (
        "PRIVACY INVARIANT VIOLATED — the client wrote into a SHARED partition of its own "
        f"tenancy while capturing a PRIVATE fact: {client_shared.describe()}"
    )

    # ---- PC-1 — the fact really was captured (this is not "nothing happened") ----------------
    client_private = await _footprint(
        client_plane,
        client_plane.default_namespace,
        client_plane.default_workspace,
        visibility=Visibility.PRIVATE,
    )
    print(f"CLIENT PRIVATE FOOTPRINT: {client_private.describe()}")  # noqa: T201 — evidence
    assert client_private.valkey_keys, (
        "POSITIVE CONTROL FAILED — the client's own PRIVATE partition is empty too, so the "
        "all-zero server reading above proves nothing: nothing was captured at all."
    )
    assert client_private.total > 0
    async with daemonless_host(client_plane) as host:
        recalled = await _eventually(
            lambda: host.recall("When is the therapy appointment?", user=_USER, session=_SESSION)
        )
    assert recalled.items, "POSITIVE CONTROL FAILED — the captured fact is not locally recallable"
    assert any("Okonjo" in (item.content or "") for item in recalled.items), (
        "POSITIVE CONTROL FAILED — the local plane did not return the captured body: "
        f"{[item.content for item in recalled.items]}"
    )
    # The GRAPH arm of the observer, armed. MEASURED: the LTM graph partition
    # ``mu_g__{digest}__u_{user}`` materializes on the local plane's LTM path (the recall above),
    # not on the ``add`` — which is why the footprint printed ``falkor=[]`` a moment ago. Re-reading
    # it here proves the falkor half of :func:`_footprint` can see a graph partition when one
    # exists, so the empty falkor reading taken against the SERVER plane is a real observation
    # rather than a dead probe. It arms the arm at PARTITION grain; this run writes no graph NODE,
    # so "zero edges" remains argued from "zero partitions", and that limit is stated up top.
    after_recall = await _footprint(
        client_plane,
        client_plane.default_namespace,
        client_plane.default_workspace,
        visibility=Visibility.PRIVATE,
    )
    assert after_recall.falkor_graphs, (
        "POSITIVE CONTROL FAILED — no FalkorDB partition exists for the client's own tenancy even "
        "after a real local recall, so the falkor arm of the store observer is proving nothing"
    )
    # The VECTOR arm, armed in the same breath and for the same reason. ``client_private.total > 0``
    # above is satisfied by the Valkey keys alone, so it says nothing about whether the Qdrant
    # probe — the arm that has to report "no server vector" — can see a point when one exists.
    assert [name for name, count in after_recall.qdrant_points if count > 0], (
        "POSITIVE CONTROL FAILED — no Qdrant collection holds a point for the client's own tenancy "
        "after a real capture and recall, so the qdrant arm is proving nothing: "
        f"{after_recall.describe()}"
    )
    print(  # noqa: T201 — evidence
        f"GRAPH+VECTOR ARMS ARMED: falkor={list(after_recall.falkor_graphs)} "
        f"qdrant={list(after_recall.qdrant_points)}"
    )

    # ---- PC-2 — the request log is ARMED: the same binary, asked to talk, does talk ----------
    room_id = f"privroom{uid}"
    share_rc, share_out, share_err = await _run_client_cli(
        ["agent-share", "status", "--room", room_id, "--agent", f"agt-{uid}"],
        env,
        timeout_s=180.0,
    )
    observed = _requests(server_plane)
    assert len(observed) == 1, (
        "POSITIVE CONTROL FAILED — one deliberate shared-plane call from the same client binary "
        f"should appear in the server's request log exactly once, got {observed} "
        f"(cli rc={share_rc} stdout={share_out!r} stderr={share_err!r})"
    )
    assert observed[0]["path"] == f"/v1/rooms/{room_id}/agent-share/agt-{uid}", observed
    print(f"REQUEST LOG ARMED: {observed}")  # noqa: T201 — evidence the observer can see a one

    # ---- PC-3 — the SERVER-SIDE observers are ARMED: a real server write shows up ------------
    # ⚠ **Two observers, and the second one is why this block exists at all.**
    #
    # Phase 2 makes three negative claims about the server plane: no request, no relational row,
    # and no artifact in the three memory stores. The first two are guarded — the request log is
    # armed by PC-2 and the relational reading by the ``open`` below. The THIRD was not, and an
    # unguarded ``server_after.total == 0`` cannot distinguish *"nothing leaked"* from *"I looked
    # in the wrong place"*: ``_footprint`` finds the server plane by ``scan_iter(match=f"*{org}*")``
    # and by ``tenant_partition_digest(org, workspace)``, and BOTH read empty for any coordinates
    # nothing has ever written under. REPRODUCED 2026-08-28: with the two arguments replaced by
    # ``"skeptic-bogus-org"``/``"skeptic-bogus-ws"`` the entire test still passed, in 314s. That is
    # the most dangerous kind of green — an instrument that reports zero because it is blind — on
    # the one gate the product's privacy promise rests on.
    #
    # So ``_server_footprint()`` is re-taken here, after a REAL server-side write, and must come
    # back NON-EMPTY. Same function, same closure, same arguments as the assertion it arms.
    #
    # ⚠ **MEASURED — and this is why the control is two calls, not one.** ``POST
    # /v1/rooms/{id}/open`` writes **Postgres only**: probed live against this very harness, after
    # ``open`` the server org's Valkey key-space was still empty and no Qdrant collection or
    # FalkorDB graph had appeared. It is the HEARTBEAT that materialises the one memory-store
    # artifact this surface writes — ``ValkeyPresenceStore``'s η-scoped
    # ``mu:presence:{org}:{workspace}:{room}``
    # (``mu-server/src/mu_server/rooms/presence_valkey.py:73-75``), which the harness purges on
    # shutdown. Arming the store observer on ``open`` alone would have looked like a control and
    # been the same blindness (M6).
    async with httpx.AsyncClient(timeout=30.0) as http:
        opened = await http.post(f"{server_plane.base_url}/v1/rooms/{room_id}/open", json={})
        assert opened.status_code == 201, opened.text
        written = await _relational_rows(server_plane, client_plane)
        assert sum(written.values()) > 0, (
            "POSITIVE CONTROL FAILED — a real room was opened on the server and the relational "
            f"observer still reads zero rows everywhere: {written}"
        )
        print(  # noqa: T201 — evidence the observer can see a one
            f"SERVER RELATIONAL ARMED: {[(t, n) for t, n in written.items() if n]}"
        )
        beat = await http.post(
            f"{server_plane.base_url}/v1/rooms/{room_id}/heartbeat", json={"focused": True}
        )
    assert beat.status_code == 200, beat.text
    server_armed = await _server_footprint()
    assert server_armed.valkey_keys, (
        "POSITIVE CONTROL FAILED — the server just wrote presence for a room it opened, and the "
        "SERVER-PLANE store reading is still empty. The all-zero reading asserted in phase 2 is "
        "therefore NOT evidence that nothing leaked; it is evidence that this probe cannot see "
        f"the server plane at all (org={server_plane.org!r} workspace={server_plane.workspace!r}): "
        f"{server_armed.describe()}"
    )
    # Not merely "some key matched": the key the server actually wrote, in the server's OWN
    # tenancy, for the room this run opened. An ambient key that happened to contain the org
    # substring would satisfy the assertion above and prove nothing.
    assert any(
        key.startswith("mu:presence:") and server_plane.org in key and room_id in key
        for key in server_armed.valkey_keys
    ), (
        "POSITIVE CONTROL FAILED — the server-plane reading is non-empty but holds no presence key "
        f"for the room just opened, so it is not observing this run's server writes: "
        f"{server_armed.describe()}"
    )
    print(f"SERVER STORE ARMED: {server_armed.describe()}")  # noqa: T201 — evidence
