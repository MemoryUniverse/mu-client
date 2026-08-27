"""Crash-recovery helper: submits one job, claims it (``PENDING -> RUNNING``), then hard-exits
via ``os._exit`` WITHOUT calling ``complete()`` — a genuine OS-process crash mid-job (no
interpreter shutdown, no ``finally``/``atexit`` cleanup runs). Invoked by
``test_sqlite_wal.py::test_resume_pending_recovers_a_job_killed_mid_run`` as a real subprocess so
the parent's fresh ``SqliteWalRunner.resume_pending()`` call proves crash recovery, not merely an
in-process simulation.
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

from mu_contracts.domain.model.lifecycle import LifecycleJob, LifecycleJobKind, UserPrefix

from mu_client.runners.sqlite_wal import SqliteWalRunner


async def _main(db_path: str, job_id: str, prefix_str: str) -> None:
    prefix = UserPrefix._from_validated_str(prefix_str)
    runner = SqliteWalRunner(Path(db_path))
    await runner.open()
    job = LifecycleJob(
        job_id=job_id,
        kind=LifecycleJobKind.SWEEP_USER,
        user_prefix=prefix,
        submitted_at=datetime.now(UTC),
        config_version="v1",
        policy_version="v1",
    )
    await runner.submit(job)
    claimed = await runner.claim_next()
    assert claimed is not None
    assert claimed.job_id == job_id
    print("RUNNING", flush=True)  # noqa: T201 — this stdout line IS the test's IPC channel
    os._exit(1)  # simulate a hard crash: no complete(), no clean close, no cleanup at all


if __name__ == "__main__":
    _db_path, _job_id, _prefix_str = sys.argv[1:4]
    asyncio.run(_main(_db_path, _job_id, _prefix_str))
