"""The durable local withdrawal — **the thing a revoke actually reaches on this device.**

Real sqlite, real files, no fakes: this store IS the local half of D4's cascade, so a test over a
double would prove nothing about durability, which is the only property it has.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from mu_client.consent.tombstone import GrantTombstone, SqliteGrantTombstones
from mu_client.errors import ConsentStoreCorruptionError, NaiveConsentTimestampError

pytestmark = pytest.mark.unit

_T0 = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)
ROOM, AGENT, GRANT = "room-42", "agt-claude", "agentshare_deadbeef"


async def _open(tmp_path: Path) -> SqliteGrantTombstones:
    store = SqliteGrantTombstones(tmp_path / "consent.sqlite")
    await store.open()
    return store


async def test_a_recorded_grant_reads_as_cut(tmp_path: Path) -> None:
    """The base case.

    **MUTATION:** make ``is_cut`` return ``False`` unconditionally -> RED.
    """
    store = await _open(tmp_path)
    try:
        assert (
            await store.is_cut(
                room_id=ROOM, agent_principal_id=AGENT, grant_id=GRANT, issued_at=_T0
            )
            is False
        )
        await store.record(
            GrantTombstone(room_id=ROOM, agent_principal_id=AGENT, grant_id=GRANT, revoked_at=_T0)
        )
        assert (
            await store.is_cut(
                room_id=ROOM, agent_principal_id=AGENT, grant_id=GRANT, issued_at=_T0
            )
            is True
        )
    finally:
        await store.aclose()


async def test_the_cut_survives_a_process_restart(tmp_path: Path) -> None:
    """Durability is the point: a withdrawal lost to a restart is a share the owner believes is cut
    and is not.

    Written by one store instance, read by a second over the same file — the closest a unit test
    gets to a daemon restart without spawning one.

    **MUTATION:** in ``SqliteGrantTombstones.open``, run the schema against ``":memory:"`` -> RED.
    """
    path = tmp_path / "consent.sqlite"
    writer = SqliteGrantTombstones(path)
    await writer.open()
    await writer.record(
        GrantTombstone(room_id=ROOM, agent_principal_id=AGENT, grant_id=GRANT, revoked_at=_T0)
    )
    await writer.aclose()

    reader = SqliteGrantTombstones(path)
    await reader.open()
    try:
        assert (
            await reader.is_cut(
                room_id=ROOM, agent_principal_id=AGENT, grant_id=GRANT, issued_at=_T0
            )
            is True
        )
    finally:
        await reader.aclose()


async def test_a_blanket_cut_covers_a_grant_this_device_never_read(tmp_path: Path) -> None:
    """The server-unreachable path: the grant id was never learned, so the cut must be wider.

    **MUTATION:** in ``is_cut``, drop the ``OR (grant_id = ? AND revoked_at >= ?)`` clause -> RED
    (a revoke made while offline cuts nothing at all). VERIFIED RED.
    """
    store = await _open(tmp_path)
    try:
        await store.record(
            SqliteGrantTombstones.blanket(
                room_id=ROOM, agent_principal_id=AGENT, revoked_at=_T0, reason="user_revoked"
            )
        )
        assert (
            await store.is_cut(
                room_id=ROOM,
                agent_principal_id=AGENT,
                grant_id="agentshare_never_seen",
                issued_at=_T0 - timedelta(hours=1),
            )
            is True
        )
    finally:
        await store.aclose()


async def test_a_blanket_cut_does_not_reach_a_later_deliberate_re_share(tmp_path: Path) -> None:
    """**The invariant that keeps re-sharing possible.**

    ``mu-server/src/mu_server/consent/model.py:91-113``: a grant id is *"UNIQUE PER ISSUANCE ACT …
    the identity of a grant is the identity of the CONSENT ACT, and two consent acts are two
    grants."* So an owner who revokes and later deliberately re-shares the same agent into the same
    room has performed a NEW consent act, and the old withdrawal must not silently override it —
    that would be the client refusing a permission its owner just granted, with no way to see why.

    **MUTATION:** in ``is_cut``, drop the ``revoked_at >= ?`` comparison (make the blanket cover
    everything) -> RED. VERIFIED RED.
    """
    store = await _open(tmp_path)
    try:
        await store.record(
            SqliteGrantTombstones.blanket(room_id=ROOM, agent_principal_id=AGENT, revoked_at=_T0)
        )
        assert (
            await store.is_cut(
                room_id=ROOM,
                agent_principal_id=AGENT,
                grant_id="agentshare_reshare",
                issued_at=_T0 + timedelta(minutes=1),
            )
            is False
        )
    finally:
        await store.aclose()


async def test_a_cut_is_scoped_to_its_own_room_and_agent(tmp_path: Path) -> None:
    """Withdrawing one share must not withdraw another.

    **MUTATION:** drop ``agent_principal_id = ?`` from ``is_cut``'s WHERE clause -> RED.
    """
    store = await _open(tmp_path)
    try:
        await store.record(
            GrantTombstone(room_id=ROOM, agent_principal_id=AGENT, grant_id=GRANT, revoked_at=_T0)
        )
        assert (
            await store.is_cut(
                room_id=ROOM, agent_principal_id="agt-other", grant_id=GRANT, issued_at=_T0
            )
            is False
        )
        assert (
            await store.is_cut(
                room_id="room-other", agent_principal_id=AGENT, grant_id=GRANT, issued_at=_T0
            )
            is False
        )
    finally:
        await store.aclose()


async def test_server_confirmation_is_monotonic(tmp_path: Path) -> None:
    """A later failed attempt must never un-confirm a cut that WAS confirmed.

    **MUTATION:** replace the ``MAX(...)`` in the ``ON CONFLICT`` clause with
    ``excluded.server_confirmed`` -> RED.
    """
    store = await _open(tmp_path)
    try:
        base = GrantTombstone(
            room_id=ROOM, agent_principal_id=AGENT, grant_id=GRANT, revoked_at=_T0
        )
        await store.record(base.model_copy(update={"server_confirmed": True}))
        await store.record(base)  # a later attempt that failed
        assert await store.blanket_cut_at(room_id=ROOM, agent_principal_id=AGENT) is None
        # Read the flag back through the same public store, over the real row.
        assert (
            await store.is_cut(
                room_id=ROOM, agent_principal_id=AGENT, grant_id=GRANT, issued_at=_T0
            )
            is True
        )
        # Asserting the stored ROW, not the API's view of it — the monotonicity lives in SQL.
        conn = store._require_conn()
        (confirmed,) = conn.execute(
            "SELECT server_confirmed FROM agent_share_tombstones WHERE grant_id = ?", (GRANT,)
        ).fetchone()
        assert confirmed == 1
    finally:
        await store.aclose()


async def test_a_store_that_cannot_be_opened_refuses_loudly(tmp_path: Path) -> None:
    """A consent surface backed by an unreadable store must REFUSE, never guess.

    **MUTATION:** swallow the ``sqlite3.DatabaseError`` in ``open`` and return a fresh connection
    -> RED.
    """
    path = tmp_path / "consent.sqlite"
    path.write_bytes(b"this is not a sqlite database" * 8)
    with pytest.raises(ConsentStoreCorruptionError):
        await SqliteGrantTombstones(path).open()


# ==================================================================================================
# The blanket row is ONE row per (room, agent) — so its cutoff has to MOVE
# ==================================================================================================
async def test_a_second_blanket_cut_advances_the_frozen_cutoff(tmp_path: Path) -> None:
    """**A second offline revoke of a re-shared agent was silently discarded.**

    The blanket row's primary key is ``(room, agent, "")``, so every blanket cut for a pair lands on
    the SAME row, and the ``ON CONFLICT`` clause updated ``server_confirmed`` only. ``revoked_at``
    stayed frozen at the FIRST blanket cut ever recorded.

    The sequence that breaks: cut offline at T0 → the owner deliberately RE-SHARES at T0+1m (a new
    grant, legitimately not covered) → the owner cuts offline again at T0+2m. The second cut was
    written, ``ClientRevocationOutcome.locally_cut`` reported *"CUT (durable, effective
    immediately)"*, and the cutoff was still T0 — so the next status screen rendered the share LIVE.
    A revoke that silently succeeds, which is the module docstring's own condemned case.

    **MUTATION:** drop ``revoked_at = MAX(...)`` from the ``ON CONFLICT`` clause -> RED.
    """
    store = await _open(tmp_path)
    try:
        await store.record(
            SqliteGrantTombstones.blanket(room_id=ROOM, agent_principal_id=AGENT, revoked_at=_T0)
        )
        reshared_at = _T0 + timedelta(minutes=1)
        assert (
            await store.is_cut(
                room_id=ROOM,
                agent_principal_id=AGENT,
                grant_id="agentshare_reshare",
                issued_at=reshared_at,
            )
            is False
        ), "precondition: the re-share is deliberately NOT covered by the first cut"

        await store.record(
            SqliteGrantTombstones.blanket(
                room_id=ROOM, agent_principal_id=AGENT, revoked_at=_T0 + timedelta(minutes=2)
            )
        )
        assert await store.blanket_cut_at(
            room_id=ROOM, agent_principal_id=AGENT
        ) == _T0 + timedelta(minutes=2)
        assert (
            await store.is_cut(
                room_id=ROOM,
                agent_principal_id=AGENT,
                grant_id="agentshare_reshare",
                issued_at=reshared_at,
            )
            is True
        )
    finally:
        await store.aclose()


async def test_a_later_cut_never_moves_the_cutoff_backwards(tmp_path: Path) -> None:
    """``MAX``, not assignment — a clock that jumps backwards must not NARROW a recorded cut.

    **MUTATION:** replace ``revoked_at = MAX(...)`` with ``revoked_at = excluded.revoked_at``
    -> RED.
    """
    store = await _open(tmp_path)
    try:
        await store.record(
            SqliteGrantTombstones.blanket(
                room_id=ROOM, agent_principal_id=AGENT, revoked_at=_T0 + timedelta(minutes=5)
            )
        )
        await store.record(
            SqliteGrantTombstones.blanket(room_id=ROOM, agent_principal_id=AGENT, revoked_at=_T0)
        )
        assert await store.blanket_cut_at(
            room_id=ROOM, agent_principal_id=AGENT
        ) == _T0 + timedelta(minutes=5)
    finally:
        await store.aclose()


# ==================================================================================================
# The LATER READ that `GrantTombstone.server_confirmed`'s docstring promised and nothing performed
# ==================================================================================================
async def test_the_server_confirmation_is_readable_back(tmp_path: Path) -> None:
    """Every reference to the ``server_confirmed`` column was a WRITE.

    ``is_cut`` collapsed the row to a bare ``bool`` and discarded it, so the fact that a revoke's
    server leg had failed survived only on the transient receipt: close the terminal and it was
    unrecoverable through any client surface, one column away from being stored.

    **MUTATION:** in ``cut_of``, hardcode ``server_confirmed=True`` -> RED.
    """
    store = await _open(tmp_path)
    try:
        unconfirmed = GrantTombstone(
            room_id=ROOM, agent_principal_id=AGENT, grant_id=GRANT, revoked_at=_T0
        )
        await store.record(unconfirmed)
        cut = await store.cut_of(
            room_id=ROOM, agent_principal_id=AGENT, grant_id=GRANT, issued_at=_T0
        )
        assert cut is not None
        assert cut.server_confirmed is False
        assert cut.revoked_at == _T0

        await store.record(unconfirmed.model_copy(update={"server_confirmed": True}))
        confirmed = await store.cut_of(
            room_id=ROOM, agent_principal_id=AGENT, grant_id=GRANT, issued_at=_T0
        )
        assert confirmed is not None and confirmed.server_confirmed is True
    finally:
        await store.aclose()


async def test_latest_cut_answers_without_any_grant_at_all(tmp_path: Path) -> None:
    """Read the durable record with NO network and NO grant id — the case the tombstone exists for.

    ``describe`` raised ``SharedPlaneUnreachableError`` before this, which made the withdrawal
    unreadable during exactly the failure it was written to survive.

    **MUTATION:** make ``latest_cut`` return ``None`` unconditionally -> RED.
    """
    store = await _open(tmp_path)
    try:
        assert await store.latest_cut(room_id=ROOM, agent_principal_id=AGENT) is None
        await store.record(
            SqliteGrantTombstones.blanket(room_id=ROOM, agent_principal_id=AGENT, revoked_at=_T0)
        )
        cut = await store.latest_cut(room_id=ROOM, agent_principal_id=AGENT)
        assert cut is not None and cut.revoked_at == _T0 and cut.server_confirmed is False
        assert await store.latest_cut(room_id=ROOM, agent_principal_id="agt-other") is None
    finally:
        await store.aclose()


async def test_an_uncovering_blanket_cut_is_named_rather_than_resolved(tmp_path: Path) -> None:
    """The ambiguity this device genuinely cannot settle, handed to the caller instead of guessed.

    **MUTATION:** make ``uncovering_blanket_cut_at`` return ``None`` unconditionally -> RED.
    """
    store = await _open(tmp_path)
    try:
        await store.record(
            SqliteGrantTombstones.blanket(room_id=ROOM, agent_principal_id=AGENT, revoked_at=_T0)
        )
        # A grant the cut DOES cover is not ambiguous — it is simply cut.
        assert (
            await store.uncovering_blanket_cut_at(
                room_id=ROOM, agent_principal_id=AGENT, issued_at=_T0 - timedelta(minutes=1)
            )
            is None
        )
        assert (
            await store.uncovering_blanket_cut_at(
                room_id=ROOM, agent_principal_id=AGENT, issued_at=_T0 + timedelta(minutes=1)
            )
            == _T0
        )
    finally:
        await store.aclose()


# ==================================================================================================
# Timezone — the silent fail-open in the one function "there is one place it can be wrong"
# ==================================================================================================
async def test_a_naive_timestamp_is_refused_rather_than_read_as_local_time(
    tmp_path: Path,
) -> None:
    """``datetime.astimezone`` reads a naive value as LOCAL time.

    ``AgentShareGrantView.issued_at`` was an unconstrained ``datetime``, so a naive value off the
    wire silently shifted the instant a blanket cut is compared against. Measured, same data, same
    instant: ``TZ=UTC`` -> covered; ``TZ=America/New_York`` -> NOT covered. On any host west of UTC
    the device re-presented a withdrawn share as live, with no error anywhere. A refusal is the only
    safe reading.

    **MUTATION:** in ``_utc_isoformat``, drop the ``tzinfo`` check -> RED.
    """
    store = await _open(tmp_path)
    naive = _T0.replace(tzinfo=None)
    try:
        with pytest.raises(NaiveConsentTimestampError):
            await store.is_cut(
                room_id=ROOM, agent_principal_id=AGENT, grant_id=GRANT, issued_at=naive
            )
        with pytest.raises(NaiveConsentTimestampError):
            await store.record(
                GrantTombstone(
                    room_id=ROOM, agent_principal_id=AGENT, grant_id=GRANT, revoked_at=naive
                )
            )
    finally:
        await store.aclose()


async def test_a_reason_that_is_not_a_name_cannot_reach_the_local_row(tmp_path: Path) -> None:
    """``trust-ledger-spec.md`` §2 rule 3: an entry carries *"only ids, content hashes, principal
    ids, enums, timestamps, and counts"*. A 64-character cap does not make prose into a name — 62
    characters of conversation content fitted inside it and were written here and POSTed onward.

    **MUTATION:** drop ``pattern=NAMED_REASON_PATTERN`` from ``GrantTombstone.reason`` -> RED.
    """
    with pytest.raises(ValidationError):
        GrantTombstone(
            room_id=ROOM,
            agent_principal_id=AGENT,
            grant_id=GRANT,
            revoked_at=_T0,
            reason="the user asked me to summarise their therapy notes",
        )
    assert (
        GrantTombstone(
            room_id=ROOM,
            agent_principal_id=AGENT,
            grant_id=GRANT,
            revoked_at=_T0,
            reason="user_revoked",
        ).reason
        == "user_revoked"
    )
