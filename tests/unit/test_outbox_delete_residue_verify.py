"""VERIFY pass on FAULT-HUNT-0924 F4a: after a user deletes a memory, is the captured text still
in the daemon outbox? (AD-252.)

**This file asserts a LIVE GAP, not a fix.** ADR 0056 gave ``SqliteOutbox`` two removal verbs —
``purge_acked_before`` (age-based, driven by ``OutboxRetentionLoop``) and
``delete_by_content_hash`` (targeted). It wired the first and, by its own report, left the second
with no caller. The one user-facing delete verb (``tool_delete`` -> ``SurfaceFacade.delete``, in
mu-core) invalidates the tiers and GCs the artifact body; nothing on that path reaches the
client's outbox at all.

So the honest answer to "does delete delete?" is: the tiers yes, the artifact body yes (after the
verify pass's own ``_maybe_gc_artifact`` fix), **the outbox no** — the raw ``activity_json``
survives the delete and leaves only when its age crosses ``OutboxSettings.acked_retention_days``
(30d default). That is a BOUNDED residue rather than the unbounded one F4a measured (374 rows, 34
days, zero deletions ever), but it is not deletion, and ``ClientCascadeResidue`` should stay the
place that says so.

REAL WAL-mode SQLite on disk (``tmp_path``), zero mocks. Greps the FILE, not the API.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from mu_client.capture.model import ActivityKind, HostKind, RawActivity
from mu_client.consent.residue import ClientCascadeResidue
from mu_client.outbox.sqlite_outbox import SqliteOutbox

pytestmark = pytest.mark.unit

_BODY_TEXT = "my bank sort code is written down in the kitchen drawer"


def _activity() -> RawActivity:
    return RawActivity(
        activity_id="act-verify-1",
        host=HostKind.CLAUDE_CODE,
        host_version="test",
        schema_version="claude_code.v1",
        kind=ActivityKind.USER_PROMPT,
        session_id="s1",
        occurred_at=datetime.now(UTC),
        text=_BODY_TEXT,
        content_hash="cafe1234",
        source_offset="1",
        provenance_id="prov_verify",
    )


def _bytes_on_disk(db: Path) -> bytes:
    raw = db.read_bytes()
    wal = db.with_name(db.name + "-wal")
    if wal.exists():
        raw += wal.read_bytes()
    return raw


async def test_the_captured_text_is_verbatim_in_the_outbox_file_after_delivery(
    tmp_path: Path,
) -> None:
    """CONTROL for the test below: the raw body really is on disk in plaintext after the activity
    has been delivered and acked, so an "absent afterwards" claim would mean something. This is
    F4a's own measurement, reproduced on a throwaway file."""
    db = tmp_path / "outbox.sqlite"
    box = SqliteOutbox(db)
    await box.open()
    try:
        rec = await box.append(_activity())
        await box.ack([rec.seq])
    finally:
        await box.aclose()
    assert _BODY_TEXT.encode("utf-8") in _bytes_on_disk(db)


async def test_the_targeted_outbox_delete_verb_really_removes_the_bytes(tmp_path: Path) -> None:
    """``delete_by_content_hash`` works — this is what a ``forget`` verb WOULD get if it called
    it. ``VACUUM`` is needed for the bytes to actually leave the file (a plain ``DELETE`` frees
    the page without overwriting it), which is itself part of the honest answer about on-disk
    residue and is NOT something the shipped code does today.

    MUTATION CHECK (run, red): change ``delete_by_content_hash``'s ``DELETE FROM outbox`` to
    ``DELETE FROM outbox WHERE 0`` — ``removed == 1`` fails."""
    db = tmp_path / "outbox.sqlite"
    box = SqliteOutbox(db)
    await box.open()
    try:
        rec = await box.append(_activity())
        await box.ack([rec.seq])
        removed = await box.delete_by_content_hash("cafe1234")
        assert removed == 1
        conn = box._conn
        assert conn is not None
        conn.execute("VACUUM")
        conn.commit()
    finally:
        await box.aclose()
    assert _BODY_TEXT.encode("utf-8") not in _bytes_on_disk(db)


def test_no_production_caller_reaches_the_targeted_outbox_delete() -> None:
    """THE GAP (AD-252): ``delete_by_content_hash`` has no caller outside its own module, so the
    user-facing delete verb cannot reach it. Asserted structurally — a walk over the shipped
    source of every repo that could call it — rather than trusted from a report.

    When a ``forget`` verb is built and wired, this test goes red; delete it together with AD-252
    at that point."""
    repo_root = Path(__file__).resolve().parents[2]
    roots = (
        repo_root / "src",
        repo_root.parent / "mu-core" / "packages",
        repo_root.parent / "mu-server" / "src",
    )
    owning = ("sqlite_outbox.py", "retention.py")
    callers = [
        f"{path}:{lineno}"
        for root in roots
        if root.is_dir()
        for path in root.rglob("*.py")
        if path.name not in owning
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        # a CALL, not a docstring/`:meth:` cross-reference (residue.py cites it by name)
        if "delete_by_content_hash(" in line and not line.lstrip().startswith("def ")
    ]
    assert callers == [], f"delete_by_content_hash now HAS callers — close AD-252: {callers}"
    # ...and the residue vocabulary still names the outbox, which is the honest compensation.
    assert ClientCascadeResidue.OUTBOX_ROW_PENDING_SETTLEMENT_OR_RETENTION in ClientCascadeResidue
