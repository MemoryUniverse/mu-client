"""AC-1.1 helper: a standalone OS process that attempts to acquire the
``SqliteWalLeaseAdapter`` lease for one ``user_prefix`` against a shared sqlite file, holds it
briefly if acquired, and reports the outcome on stdout. Invoked by
``test_sqlite_wal.py::test_ac_1_1_two_real_os_processes_exactly_one_acquires`` via
``subprocess.Popen([sys.executable, __file__, db_path, user_prefix, device_id, hold_s])`` — a
REAL second OS process, not a second asyncio task in the parent (the whole point of BQ1's fix).
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from mu_contracts.domain.model.lifecycle import UserPrefix

from mu_client.runners.sqlite_wal import LifecycleLeaseBusyError, SqliteWalLeaseAdapter


async def _main(db_path: str, prefix_str: str, device_id: str, hold_s: float) -> None:
    prefix = UserPrefix._from_validated_str(prefix_str)
    adapter = SqliteWalLeaseAdapter(Path(db_path), device_id=device_id)
    await adapter.open()
    try:
        async with adapter.acquire(prefix):
            print("ACQUIRED", flush=True)  # noqa: T201 — this stdout line IS the test's IPC channel
            await asyncio.sleep(hold_s)
    except LifecycleLeaseBusyError:
        print("BUSY", flush=True)  # noqa: T201
    finally:
        await adapter.aclose()


if __name__ == "__main__":
    _db_path, _prefix_str, _device_id, _hold_s = sys.argv[1:5]
    asyncio.run(_main(_db_path, _prefix_str, _device_id, float(_hold_s)))
