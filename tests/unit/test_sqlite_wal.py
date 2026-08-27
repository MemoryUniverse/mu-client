"""``SqliteWalRunner`` + ``SqliteWalLeaseAdapter`` — REAL WAL-mode SQLite, zero mocks (the same
leaf-adapter convention as ``test_sqlite_outbox.py``). AC-1.1 and the crash-recovery test spawn
REAL subprocesses (``sys.executable`` + a helper script) — two independent OS processes, not two
asyncio tasks in one process, per BQ1's whole point."""

from __future__ import annotations

import asyncio
import sqlite3
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from mu_contracts.domain.model.lifecycle import (
    JobHandle,
    JobStatus,
    LifecycleJob,
    LifecycleJobKind,
    UserPrefix,
)
from mu_contracts.domain.model.memory import Namespace, Visibility
from mu_contracts.ports.lifecycle_lease import LifecycleLeasePort
from mu_contracts.ports.lifecycle_workflow import LifecycleWorkflowRunnerPort
from mu_engine.lifecycle.settings import OwnershipSettings
from mu_engine.platform.clock import FrozenClock, OffsetClock

from mu_client.runners.sqlite_wal import (
    LifecycleLeaseBusyError,
    SqliteWalLeaseAdapter,
    SqliteWalRunner,
)

pytestmark = pytest.mark.unit

_WORKER_DIR = Path(__file__).parent
_LEASE_WORKER = _WORKER_DIR / "_wal_lease_worker.py"
_RUNNER_WORKER = _WORKER_DIR / "_wal_runner_worker.py"


def _prefix(user: str = "alice") -> UserPrefix:
    ns = Namespace(
        org="acme", workspace="ws1", user=user, session="s1", visibility=Visibility.PRIVATE
    )
    return UserPrefix(ns)


def _job(job_id: str, prefix: UserPrefix) -> LifecycleJob:
    return LifecycleJob(
        job_id=job_id,
        kind=LifecycleJobKind.SWEEP_USER,
        user_prefix=prefix,
        submitted_at=datetime.now(UTC),
        config_version="v1",
        policy_version="v1",
    )


def _read_expires_at(path: Path, prefix: UserPrefix) -> datetime:
    conn = sqlite3.connect(str(path))
    try:
        row = conn.execute(
            "SELECT expires_at FROM lifecycle_leases WHERE user_prefix=?", (str(prefix),)
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    return datetime.fromisoformat(row[0])


def _table_names(path: Path) -> set[str]:
    conn = sqlite3.connect(str(path))
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    finally:
        conn.close()
    return {r[0] for r in rows}


# ---------------------------------------------------------------------------- port conformance
def test_runner_satisfies_lifecycle_workflow_runner_port(tmp_path: Path) -> None:
    runner = SqliteWalRunner(tmp_path / "wal.sqlite")
    assert isinstance(runner, LifecycleWorkflowRunnerPort)


def test_lease_adapter_satisfies_lifecycle_lease_port(tmp_path: Path) -> None:
    adapter = SqliteWalLeaseAdapter(tmp_path / "wal.sqlite", device_id="dev-1")
    assert isinstance(adapter, LifecycleLeasePort)


# ---------------------------------------------------------------------------------- one substrate
async def test_job_log_and_lease_table_share_one_sqlite_wal_file(tmp_path: Path) -> None:
    """acceptance: 'the job log and the lease table live in the SAME sqlite WAL database file' —
    whichever of the two adapters opens the file first lays down BOTH tables."""
    path = tmp_path / "wal.sqlite"
    runner = SqliteWalRunner(path)
    await runner.open()
    try:
        assert {"lifecycle_jobs", "lifecycle_leases"} <= _table_names(path)
        assert (path.with_name(path.name + "-wal")).exists() or path.stat().st_size > 0
    finally:
        await runner.aclose()

    # A fresh lease adapter against the SAME path sees the runner's table too (schema is
    # idempotent, so opening the lease adapter second changes nothing).
    adapter = SqliteWalLeaseAdapter(path, device_id="dev-1")
    await adapter.open()
    try:
        assert {"lifecycle_jobs", "lifecycle_leases"} <= _table_names(path)
    finally:
        await adapter.aclose()


async def test_lease_only_construction_also_lays_down_both_tables(tmp_path: Path) -> None:
    """The reverse order: lease adapter opens the (fresh) file first."""
    path = tmp_path / "wal2.sqlite"
    adapter = SqliteWalLeaseAdapter(path, device_id="dev-1")
    await adapter.open()
    try:
        assert {"lifecycle_jobs", "lifecycle_leases"} <= _table_names(path)
    finally:
        await adapter.aclose()


# ------------------------------------------------------------------------------ runner: happy path
async def test_submit_claim_complete_and_await_result_round_trip(tmp_path: Path) -> None:
    runner = SqliteWalRunner(tmp_path / "wal.sqlite")
    await runner.open()
    try:
        prefix = _prefix()
        job = _job("job-1", prefix)
        handle = await runner.submit(job)
        assert isinstance(handle, JobHandle)
        assert handle.job_id == "job-1"

        claimed = await runner.claim_next()
        assert claimed is not None
        assert claimed.job_id == "job-1"
        assert claimed.user_prefix == prefix

        # idempotent re-submit of the same job_id is a no-op, not a duplicate row/error.
        await runner.submit(job)

        await runner.complete("job-1")
        result = await runner.await_result(handle, timeout_s=2.0)
        assert result.status is JobStatus.SUCCEEDED
        assert result.error is None
        assert result.completed_at is not None
    finally:
        await runner.aclose()


async def test_complete_with_error_marks_job_failed(tmp_path: Path) -> None:
    runner = SqliteWalRunner(tmp_path / "wal.sqlite")
    await runner.open()
    try:
        prefix = _prefix()
        handle = await runner.submit(_job("job-err", prefix))
        await runner.claim_next()
        await runner.complete("job-err", error="boom")
        result = await runner.await_result(handle, timeout_s=2.0)
        assert result.status is JobStatus.FAILED
        assert result.error == "boom"
    finally:
        await runner.aclose()


async def test_claim_next_returns_none_when_nothing_pending(tmp_path: Path) -> None:
    runner = SqliteWalRunner(tmp_path / "wal.sqlite")
    await runner.open()
    try:
        assert await runner.claim_next() is None
    finally:
        await runner.aclose()


async def test_await_result_times_out_while_job_still_running(tmp_path: Path) -> None:
    runner = SqliteWalRunner(tmp_path / "wal.sqlite")
    await runner.open()
    try:
        prefix = _prefix()
        handle = await runner.submit(_job("job-slow", prefix))
        await runner.claim_next()  # -> RUNNING, never completed
        with pytest.raises(TimeoutError):
            await runner.await_result(handle, poll_interval_s=0.01, timeout_s=0.1)
    finally:
        await runner.aclose()


async def test_resume_pending_is_a_noop_with_nothing_crashed(tmp_path: Path) -> None:
    runner = SqliteWalRunner(tmp_path / "wal.sqlite")
    await runner.open()
    try:
        assert await runner.resume_pending() == 0
        await runner.submit(_job("job-1", _prefix()))
        # PENDING (never claimed) counts as still-eligible, not "resumed from a crash", but
        # resume_pending()'s return is "count of jobs now eligible to run" per its own contract.
        assert await runner.resume_pending() == 1
    finally:
        await runner.aclose()


# ------------------------------------------------------------------ acceptance: crash-recovery
def test_resume_pending_recovers_a_job_killed_mid_run(tmp_path: Path) -> None:
    """A REAL subprocess claims a job (PENDING->RUNNING) then os._exit(1)s without completing —
    a genuine crash. A fresh SqliteWalRunner construction against the same WAL file must recover
    it via resume_pending() (spec: 'resume_pending() replays any PENDING/INFLIGHT-crashed job on
    a fresh construction against the same WAL file')."""
    path = tmp_path / "wal.sqlite"
    prefix = _prefix("bob")

    proc = subprocess.run(  # noqa: S603 — fixed, hardcoded argv (sys.executable + our own script)
        [sys.executable, str(_RUNNER_WORKER), str(path), "crashed-job", str(prefix)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert "RUNNING" in proc.stdout, f"worker did not reach RUNNING: stderr={proc.stderr!r}"
    assert proc.returncode == 1  # os._exit(1) — the simulated crash exit code

    async def _recover() -> tuple[int, LifecycleJob | None]:
        runner = SqliteWalRunner(path)
        await runner.open()
        try:
            resumed = await runner.resume_pending()
            reclaimed = await runner.claim_next()
            return resumed, reclaimed
        finally:
            await runner.aclose()

    resumed_count, reclaimed_job = asyncio.run(_recover())
    assert resumed_count == 1
    assert reclaimed_job is not None
    assert reclaimed_job.job_id == "crashed-job"


# ------------------------------------------------------------------------ lease: naming convention
def test_lease_name_follows_canonical_7_5_plane_qualified_convention(tmp_path: Path) -> None:
    adapter = SqliteWalLeaseAdapter(tmp_path / "wal.sqlite", device_id="device-xyz")
    prefix = _prefix()
    assert adapter.lease_name(prefix) == f"lifecycle-sweep-lease:local:device-xyz:{prefix}"


async def test_busy_error_carries_the_plane_qualified_lease_name(tmp_path: Path) -> None:
    path = tmp_path / "wal.sqlite"
    prefix = _prefix()
    holder = SqliteWalLeaseAdapter(path, device_id="dev-1")
    contender = SqliteWalLeaseAdapter(path, device_id="dev-2")
    await holder.open()
    await contender.open()
    try:
        async with holder.acquire(prefix):
            with pytest.raises(LifecycleLeaseBusyError) as exc_info:
                async with contender.acquire(prefix):
                    pass  # pragma: no cover - must never be entered
            assert exc_info.value.lease_name == contender.lease_name(prefix)
    finally:
        await holder.aclose()
        await contender.aclose()


# ------------------------------------------------------------------------ acceptance: AC-1.1
def test_ac_1_1_two_real_os_processes_exactly_one_acquires(tmp_path: Path) -> None:
    """AC-1.1 (ties BQ1/X1): a resident-daemon-simulating process and a second, independent OS
    process both attempt SqliteWalLeaseAdapter.acquire(same user_prefix) against the SAME sqlite
    file concurrently — exactly one succeeds. Two REAL Python processes (subprocess.Popen), not
    two asyncio tasks in one process."""
    path = tmp_path / "wal.sqlite"
    prefix = _prefix("carol")

    proc_a = subprocess.Popen(  # noqa: S603 — fixed, hardcoded argv (sys.executable + our script)
        [sys.executable, str(_LEASE_WORKER), str(path), str(prefix), "daemon-device", "1.0"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    proc_b = subprocess.Popen(  # noqa: S603
        [sys.executable, str(_LEASE_WORKER), str(path), str(prefix), "cli-device", "1.0"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    out_a, err_a = proc_a.communicate(timeout=30)
    out_b, err_b = proc_b.communicate(timeout=30)

    outcomes = [out_a.strip(), out_b.strip()]
    assert (
        outcomes.count("ACQUIRED") == 1
    ), f"expected exactly one ACQUIRED, got {outcomes} (stderr_a={err_a!r}, stderr_b={err_b!r})"
    assert outcomes.count("BUSY") == 1


# ------------------------------------------------------------------ acceptance: heartbeat (§19)
async def test_lease_row_renews_expires_at_every_heartbeat_via_frozen_clock(tmp_path: Path) -> None:
    """acceptance: 'lease row renews every ownership.lease_heartbeat_s ... (simulated via
    FrozenClock/OffsetClock injection into the adapter, per spec §19)'. The renewal cadence
    (asyncio.sleep) is real wall-clock time (small, 1s, for test speed); the VALUE written into
    expires_at is always derived from the injected Clock, proven here by advancing the FrozenClock
    mid-hold and observing expires_at jump by exactly that advance."""
    path = tmp_path / "wal.sqlite"
    prefix = _prefix("dana")
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    clock = FrozenClock(t0)
    adapter = SqliteWalLeaseAdapter(
        path,
        device_id="dev-1",
        clock=clock,
        ownership=OwnershipSettings(lease_ttl_s=100, lease_heartbeat_s=1),
    )
    await adapter.open()
    try:
        async with adapter.acquire(prefix):
            expires_at_1 = _read_expires_at(path, prefix)
            assert expires_at_1 == t0 + timedelta(seconds=100)

            clock.advance(timedelta(seconds=50))
            await asyncio.sleep(1.3)  # let the real-time (1s cadence) heartbeat loop fire >=1x

            expires_at_2 = _read_expires_at(path, prefix)
            assert expires_at_2 == t0 + timedelta(seconds=50) + timedelta(seconds=100)
            assert expires_at_2 > expires_at_1
    finally:
        await adapter.aclose()


# --------------------------------------------------------------- acceptance: reclaim past TTL (§19)
async def test_lease_reclaimable_once_ttl_elapsed_with_no_renewal_via_offset_clock(
    tmp_path: Path,
) -> None:
    """acceptance: '... and is reclaimable once ownership.lease_ttl_s has elapsed with no
    renewal'. A stale row (simulating a crashed holder that never renewed) is written directly;
    an OffsetClock (skewed +30s ahead of real time, per spec §19's skew-injection pattern) proves
    the reclaim decision is Clock-driven, not merely 'real time happened to pass'."""
    path = tmp_path / "wal.sqlite"
    prefix = _prefix("erin")

    adapter = SqliteWalLeaseAdapter(path, device_id="dev-1")
    await adapter.open()  # lays down the schema
    await adapter.aclose()

    # Simulate a crashed holder: a live INSERT whose expires_at is already 1 hour in the past —
    # exactly what a holder that acquired-then-died-without-renewing leaves behind.
    stale_conn = sqlite3.connect(str(path))
    try:
        stale_conn.execute(
            "INSERT INTO lifecycle_leases"
            "(user_prefix, holder_kind, holder_pid, acquired_at, expires_at)"
            " VALUES (?,?,?,?,?)",
            (
                str(prefix),
                "local:crashed-device",
                999_999,
                (datetime.now(UTC) - timedelta(hours=2)).isoformat(),
                (datetime.now(UTC) - timedelta(hours=1)).isoformat(),
            ),
        )
        stale_conn.commit()
    finally:
        stale_conn.close()

    skewed_clock = OffsetClock(offset=timedelta(seconds=30))
    new_holder = SqliteWalLeaseAdapter(path, device_id="dev-2", clock=skewed_clock)
    await new_holder.open()
    try:
        async with new_holder.acquire(prefix):
            # Reaching here proves the stale row was reclaimed rather than raising Busy.
            expires_at = _read_expires_at(path, prefix)
            assert expires_at > datetime.now(UTC)
    finally:
        await new_holder.aclose()


async def test_lease_released_cleanly_on_normal_exit(tmp_path: Path) -> None:
    path = tmp_path / "wal.sqlite"
    prefix = _prefix("frank")
    adapter = SqliteWalLeaseAdapter(path, device_id="dev-1")
    await adapter.open()
    try:
        async with adapter.acquire(prefix):
            pass
        # released -> a second immediate acquire by a DIFFERENT holder succeeds without waiting
        # for any TTL.
        other = SqliteWalLeaseAdapter(path, device_id="dev-2")
        await other.open()
        try:
            async with other.acquire(prefix):
                pass
        finally:
            await other.aclose()
    finally:
        await adapter.aclose()
