"""``SqliteGrantTombstones`` — **the thing a revoke actually reaches on this device.**

D4's revocation cascade (``SERVER-AND-COLLAB-DESIGN-REVIEW.md:124``) is, in this build, almost
entirely a server-side sequence: the consent CAS, the roster ``left_at``, the dispatch record, the
``revoke_signal`` publish. The daemon's own leg — consume the frame, stop the subprocess, purge the
warm cache, emit ``revoke_ack`` — is unreachable here for reasons
:class:`~mu_client.consent.residue.ClientCascadeResidue` names one by one.

**What IS reachable, and is not nothing: the local record of withdrawal.** This device is where the
owner's *"your agent is shared here"* affordance lives (D4 §4.2-D step 4), so this device is where
"withdrawn" has to become durable, and it has to become durable **before** the server is called —
the ordering ``mu-server/src/mu_server/agents/bridge.py:519-526`` chose for the same reason:

    *"the cascade is ordered consent-first precisely so a crash mid-cascade leaves access CUT,
    never open."*

Consequently a revoke whose network leg fails still leaves this device refusing to present the
share as live (:attr:`~mu_client.consent.exposure.AgentExposureContract.effectively_live` is
fail-closed), and the failure is reported as
:attr:`~mu_client.consent.residue.ClientCascadeResidue.SERVER_REVOKE_NOT_CONFIRMED` rather than
swallowed.

--------------------------------------------------------------------------------------------
Why a tombstone is keyed by GRANT and not by (room, agent)
--------------------------------------------------------------------------------------------
A grant id is *"UNIQUE PER ISSUANCE ACT"* (``mu-server/src/mu_server/consent/model.py:91``): sharing
the same agent into the same room again mints a NEW grant, because *"the identity of a grant is the
identity of the CONSENT ACT, and two consent acts are two grants."* A tombstone keyed by
``(room, agent)`` would therefore make re-sharing impossible — the owner's second, deliberate
consent would be silently overridden by their first withdrawal. So a tombstone names a grant.

**The blanket row, and why it exists.** A revoke can be initiated when the server is unreachable, in
which case this device never learned the grant id. Refusing to cut in that case would be exactly
backwards. Such a revoke writes a row with an empty ``grant_id`` — a *blanket cut* — which covers
every grant for that ``(room, agent)`` whose ``issued_at`` is at or before the cut instant, and
leaves a genuinely later consent act live. The rule is one function,
:meth:`SqliteGrantTombstones.is_cut`, so there is one place it can be wrong.

**Cancellation-safe (DEV-STANDARDS rule 1)**, and by the same construction ``SqliteOutbox`` uses:
every blocking ``sqlite3`` call runs off the event loop via ``asyncio.to_thread``, and one
``asyncio.Lock`` serializes the single shared connection so a cancelled caller never leaves it
mid-statement.

**Content-free (rule 3).** Ids, a named reason bounded at 64 characters, and timestamps. No memory
content, no room body, no capability values.
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from mu_client.consent.wire import MAX_REVOKE_REASON_CHARS, NAMED_REASON_PATTERN
from mu_client.errors import ConsentStoreCorruptionError, NaiveConsentTimestampError

__all__ = ["GrantTombstone", "LocalCut", "SqliteGrantTombstones"]


def _utc_isoformat(value: datetime) -> str:
    """The ONE way a datetime becomes a comparable column value here.

    Refuses a naive datetime by name rather than letting ``astimezone`` silently read it as LOCAL
    time: on any host west of UTC that turned a covering blanket cut into a non-covering one, i.e.
    silently re-presented a withdrawn share as live. Fail-open in the one function this module's
    docstring designates as *"one place it can be wrong"*.
    """
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise NaiveConsentTimestampError(str(value))
    return value.astimezone(UTC).isoformat()


#: The empty ``grant_id`` sentinel that marks a BLANKET cut. Empty rather than ``NULL`` so the
#: primary key stays simple and a lookup never has to reason about SQL NULL semantics.
_BLANKET = ""

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS agent_share_tombstones (
  room_id TEXT NOT NULL,
  agent_principal_id TEXT NOT NULL,
  grant_id TEXT NOT NULL,
  revoked_at TEXT NOT NULL,
  reason TEXT,
  server_confirmed INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (room_id, agent_principal_id, grant_id)
);
CREATE INDEX IF NOT EXISTS idx_tombstone_pair
  ON agent_share_tombstones(room_id, agent_principal_id);
"""


class GrantTombstone(BaseModel):
    """One durable local withdrawal."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    room_id: str = Field(min_length=1)
    agent_principal_id: str = Field(min_length=1)
    #: Empty string = a blanket cut (see the module docstring).
    grant_id: str = ""
    revoked_at: datetime
    #: A NAMED reason, on the same rule the wire body uses — the local row and the ledger row must
    #: not be able to disagree about what may be written into this field.
    reason: str | None = Field(
        default=None, max_length=MAX_REVOKE_REASON_CHARS, pattern=NAMED_REASON_PATTERN
    )
    #: Whether the server's leg of this revoke was confirmed. ``False`` is a REAL, reportable
    #: state, not a missing value: it is what
    #: :attr:`~mu_client.consent.residue.ClientCascadeResidue.SERVER_REVOKE_NOT_CONFIRMED` is
    #: derived from on a later read.
    server_confirmed: bool = False


class LocalCut(BaseModel):
    """**What the durable record SAYS about a withdrawal — read back, not merely written.**

    :attr:`GrantTombstone.server_confirmed`'s docstring promised this read and nothing performed it:
    every reference to the column was a write, and :meth:`SqliteGrantTombstones.is_cut` collapsed
    the row to a bare ``bool``. So a revoke whose server leg failed reported
    ``SERVER_REVOKE_NOT_CONFIRMED`` once, on a transient receipt, and the fact became unrecoverable
    through any client surface the moment the process exited — while the server still held the grant
    ACTIVE and the agent could still act. This type is that later read.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    #: The most recent covering cut instant.
    revoked_at: datetime
    #: ``True`` iff the server confirmed at least one covering cut for this pair.
    server_confirmed: bool


class SqliteGrantTombstones:
    """Durable, WAL-mode local record of which agent shares this device has withdrawn."""

    def __init__(self, path: Path) -> None:
        self._path = path.expanduser()
        self._conn: sqlite3.Connection | None = None
        self._lock = asyncio.Lock()

    async def open(self) -> None:
        """Create the file + schema and hold the one connection. Idempotent."""

        def _connect() -> sqlite3.Connection:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            try:
                conn = sqlite3.connect(self._path, check_same_thread=False, isolation_level=None)
                conn.execute("PRAGMA journal_mode=WAL")
                # FULL, not NORMAL: a withdrawal that is lost to a power cut is a share the owner
                # believes is cut and is not. The durability boundary is before the caller returns,
                # exactly as SqliteOutbox.append reasons about a capture it must not lose.
                conn.execute("PRAGMA synchronous=FULL")
                conn.executescript(_SCHEMA_SQL)
                return conn
            except sqlite3.DatabaseError as exc:
                raise ConsentStoreCorruptionError(str(self._path)) from exc

        if self._conn is None:
            self._conn = await asyncio.to_thread(_connect)

    async def aclose(self) -> None:
        conn, self._conn = self._conn, None
        if conn is not None:
            await asyncio.to_thread(conn.close)

    def _require_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise ConsentStoreCorruptionError(f"{self._path} is not open")
        return self._conn

    async def record(self, tombstone: GrantTombstone) -> None:
        """Write (or upgrade) a withdrawal. Durable before this returns.

        ``ON CONFLICT`` upgrades ``server_confirmed`` monotonically 0→1 and never back: a second
        revoke attempt that finally reaches the server must be able to clear the
        "not confirmed" residue, while a later failed attempt must never un-confirm a cut that WAS
        confirmed.

        ⚠ **``revoked_at`` advances too, and that is not cosmetic.** The blanket row's primary key
        is ``(room, agent, "")``, so EVERY blanket cut for a pair lands on ONE row. Leaving
        ``revoked_at`` at its inserted value froze it at the FIRST blanket cut ever recorded, and
        the whole re-share rule — :meth:`is_cut` comparing a grant's ``issued_at`` against that
        instant — then read a stale cutoff. Concretely: cut offline, deliberately re-share, cut
        offline again; the second cut was written, reported *"CUT (durable, effective immediately)"*
        and covered nothing, and the next status screen rendered the share LIVE. That is the module
        docstring's own condemned case — a revoke that silently succeeds — produced by an
        ``ON CONFLICT`` clause that updated one column too few.
        """
        conn = self._require_conn()

        def _do() -> None:
            conn.execute(
                """
                INSERT INTO agent_share_tombstones
                  (room_id, agent_principal_id, grant_id, revoked_at, reason, server_confirmed)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(room_id, agent_principal_id, grant_id) DO UPDATE SET
                  server_confirmed = MAX(
                    agent_share_tombstones.server_confirmed, excluded.server_confirmed
                  ),
                  reason = CASE
                    WHEN excluded.revoked_at > agent_share_tombstones.revoked_at
                    THEN excluded.reason ELSE agent_share_tombstones.reason END,
                  revoked_at = MAX(agent_share_tombstones.revoked_at, excluded.revoked_at)
                """,
                (
                    tombstone.room_id,
                    tombstone.agent_principal_id,
                    tombstone.grant_id,
                    _utc_isoformat(tombstone.revoked_at),
                    tombstone.reason,
                    int(tombstone.server_confirmed),
                ),
            )

        async with self._lock:
            await asyncio.to_thread(_do)

    async def cut_of(
        self,
        *,
        room_id: str,
        agent_principal_id: str,
        grant_id: str,
        issued_at: datetime,
    ) -> LocalCut | None:
        """**The one rule.** The covering local cut for this grant, or ``None``.

        A row covers the grant when it names this exact ``grant_id``, or when it is a blanket cut
        for the pair recorded at or after the grant's ``issued_at``. The ``issued_at`` comparison is
        what lets a deliberate RE-SHARE survive an earlier blanket cut — see the module docstring.

        ``server_confirmed`` is the MAX over covering rows: "did the server confirm a withdrawal
        that covers this grant?", which is the question the consent screen has to answer.

        ⚠ **This comparison is CROSS-CLOCK and cannot be made correct here.** ``revoked_at`` is this
        laptop's clock; ``issued_at`` is the server's, carried verbatim (``wire.py``: *"two clocks
        disagreeing about whether a consent is live is exactly the ambiguity a single authority
        removes"*). A blanket cut is written precisely when the server was unreachable — i.e. when
        this device is most likely to be running unsynchronised — so a laptop whose clock is behind
        writes a cut that does not cover a grant it was meant to cover. Narrowing the comparison
        would swallow real re-shares; widening it would be a fudge factor with the same failure at a
        different offset. So the ambiguity is neither hidden nor guessed: the caller is handed
        :meth:`uncovering_blanket_cut_at` and the consent screen REPORTS it, naming both causes.
        The correct fix is server-side and is recorded as an architecture delta.
        """
        conn = self._require_conn()
        cutoff = _utc_isoformat(issued_at)

        def _do() -> tuple[str | None, int | None]:
            row = conn.execute(
                """
                SELECT MAX(revoked_at), MAX(server_confirmed) FROM agent_share_tombstones
                 WHERE room_id = ? AND agent_principal_id = ?
                   AND (grant_id = ? OR (grant_id = ? AND revoked_at >= ?))
                """,
                (room_id, agent_principal_id, grant_id, _BLANKET, cutoff),
            ).fetchone()
            return (None, None) if row is None else (row[0], row[1])

        async with self._lock:
            revoked_at, server_confirmed = await asyncio.to_thread(_do)
        if revoked_at is None:
            return None
        return LocalCut(
            revoked_at=datetime.fromisoformat(revoked_at),
            server_confirmed=bool(server_confirmed),
        )

    async def is_cut(
        self,
        *,
        room_id: str,
        agent_principal_id: str,
        grant_id: str,
        issued_at: datetime,
    ) -> bool:
        """Whether THIS device has withdrawn this grant. :meth:`cut_of`, reduced to a bool."""
        return (
            await self.cut_of(
                room_id=room_id,
                agent_principal_id=agent_principal_id,
                grant_id=grant_id,
                issued_at=issued_at,
            )
            is not None
        )

    async def uncovering_blanket_cut_at(
        self, *, room_id: str, agent_principal_id: str, issued_at: datetime
    ) -> datetime | None:
        """A blanket cut for the pair that does **not** cover a grant issued at ``issued_at``.

        Exactly the ambiguous case :meth:`cut_of` refuses to guess about: the owner withdrew, and
        then either deliberately re-shared, or this device's clock disagrees with the server's.
        Returning it (rather than silently resolving it) is what lets the consent screen say so.
        """
        blanket = await self.blanket_cut_at(room_id=room_id, agent_principal_id=agent_principal_id)
        # Compared as the SAME normalised strings the SQL predicate uses, through the SAME
        # naive-datetime refusal — two comparison rules would be two places this can be wrong.
        if blanket is None or _utc_isoformat(blanket) >= _utc_isoformat(issued_at):
            return None
        return blanket

    async def latest_cut(self, *, room_id: str, agent_principal_id: str) -> LocalCut | None:
        """The most recent cut of ANY kind for a pair — used when this device cannot read the
        grant at all (the server 404'd, or is unreachable) and must still answer from its own
        durable record. A store that can only be read while the network is up would defeat the
        reason the tombstone is written before the server is called.
        """
        conn = self._require_conn()

        def _do() -> tuple[str | None, int | None]:
            row = conn.execute(
                """
                SELECT MAX(revoked_at), MAX(server_confirmed) FROM agent_share_tombstones
                 WHERE room_id = ? AND agent_principal_id = ?
                """,
                (room_id, agent_principal_id),
            ).fetchone()
            return (None, None) if row is None else (row[0], row[1])

        async with self._lock:
            revoked_at, server_confirmed = await asyncio.to_thread(_do)
        if revoked_at is None:
            return None
        return LocalCut(
            revoked_at=datetime.fromisoformat(revoked_at),
            server_confirmed=bool(server_confirmed),
        )

    async def blanket_cut_at(self, *, room_id: str, agent_principal_id: str) -> datetime | None:
        """The most recent blanket cut for a pair, or ``None``. Used to report an unconfirmed
        withdrawal for an agent whose grant this device could never read."""
        conn = self._require_conn()

        def _do() -> str | None:
            row = conn.execute(
                """
                SELECT MAX(revoked_at) FROM agent_share_tombstones
                 WHERE room_id = ? AND agent_principal_id = ? AND grant_id = ?
                """,
                (room_id, agent_principal_id, _BLANKET),
            ).fetchone()
            return None if row is None else row[0]

        async with self._lock:
            raw = await asyncio.to_thread(_do)
        return None if raw is None else datetime.fromisoformat(raw)

    @staticmethod
    def blanket(
        *,
        room_id: str,
        agent_principal_id: str,
        revoked_at: datetime,
        reason: str | None = None,
    ) -> GrantTombstone:
        """A blanket cut — the shape used when the grant id could not be learned."""
        return GrantTombstone(
            room_id=room_id,
            agent_principal_id=agent_principal_id,
            grant_id=_BLANKET,
            revoked_at=revoked_at,
            reason=reason,
        )
