"""``SessionSaveTrigger`` — the two event-driven consolidation triggers.

Pure logic against a recording fake promoter: no stores, no daemon (DEV-STANDARDS allows mocks at
the unit tier). The REAL machinery these delegate to (`promote_session_now`) is already covered by
the promotion/distill integration tests; what needs proving HERE is the trigger policy.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from mu_contracts.domain.model.memory import Namespace, Visibility

from mu_client.capture.model import ActivityKind, HostKind, RawActivity
from mu_client.lifecycle.session_save import SessionSaveTrigger

pytestmark = pytest.mark.unit


class _RecordingPromoter:
    def __init__(self) -> None:
        self.calls: list[tuple[Namespace, bool]] = []

    async def promote_session_now(self, ns: Namespace, *, force: bool = False) -> None:
        self.calls.append((ns, force))


def _trigger(promoter: _RecordingPromoter, *, every_n: int = 3) -> SessionSaveTrigger:
    return SessionSaveTrigger(
        promoter=promoter, org="o", workspace="w", user="u", consolidate_every_n=every_n
    )


def _activity(kind: ActivityKind, session: str = "s1") -> RawActivity:
    """A real ``RawActivity`` through the real model — never a hand-built stand-in, so a field the
    parser genuinely produces cannot silently drift away from what this test asserts on."""
    return RawActivity(
        activity_id="a1",
        host=HostKind.CLAUDE_CODE,
        host_version="test",
        schema_version="1",
        kind=kind,
        session_id=session,
        occurred_at=datetime(2026, 8, 20, tzinfo=UTC),
        text=None,
        content_hash=None,
        provenance_id="prov_test",
    )


async def test_session_end_drains_the_buffer_with_force() -> None:
    """A closing session has no "later" for a turn to become salient in, so the routine salience
    gate must be BYPASSED — same reasoning as PreCompact. `force=True` is the whole point."""
    promoter = _RecordingPromoter()
    trigger = _trigger(promoter)
    await trigger.on_session_end(_activity(ActivityKind.SESSION_END))
    await trigger.aclose()  # the drain is BACKGROUNDED — settle it before asserting on it

    assert len(promoter.calls) == 1
    ns, force = promoter.calls[0]
    assert force is True, "session-end consolidation must bypass the salience gate"
    assert (ns.org, ns.workspace, ns.user, ns.session) == ("o", "w", "u", "s1")
    assert ns.visibility is Visibility.PRIVATE


async def test_session_end_rejects_a_misrouted_activity() -> None:
    """A caller bug is raised, never silently ignored (mirrors PreCompactPromoter)."""
    promoter = _RecordingPromoter()
    with pytest.raises(ValueError, match="non-SessionEnd"):
        await _trigger(promoter).on_session_end(_activity(ActivityKind.USER_PROMPT))
    assert promoter.calls == []


async def test_capture_pressure_drains_only_at_the_threshold() -> None:
    """THE regression this trigger exists for. Before it, the only routine route out of STM was a
    salience gate an auto-captured turn can never clear (best-possible 0.650 vs 0.700), so captured
    memory stayed session-trapped forever."""
    promoter = _RecordingPromoter()
    trigger = _trigger(promoter, every_n=3)

    assert await trigger.on_capture("s1") is False
    assert await trigger.on_capture("s1") is False
    assert promoter.calls == [], "drained before reaching the threshold"

    assert await trigger.on_capture("s1") is True
    await trigger.aclose()
    assert len(promoter.calls) == 1
    assert promoter.calls[0][1] is True  # force


async def test_the_counter_resets_after_a_drain() -> None:
    """Pressure is per drain cycle, so a long session drains repeatedly rather than once."""
    promoter = _RecordingPromoter()
    trigger = _trigger(promoter, every_n=2)

    for _ in range(4):
        await trigger.on_capture("s1")
        await trigger.aclose()  # settle each drain so the next one is not coalesced away

    assert len(promoter.calls) == 2


async def test_pressure_is_counted_per_session_not_globally() -> None:
    """Two concurrent agent sessions must not drain each other's buffers early — the count that
    matters is how full THIS session's STM window is."""
    promoter = _RecordingPromoter()
    trigger = _trigger(promoter, every_n=3)

    await trigger.on_capture("s1")
    await trigger.on_capture("s2")
    await trigger.on_capture("s1")
    await trigger.on_capture("s2")
    assert promoter.calls == [], "counts leaked across sessions"

    await trigger.on_capture("s1")
    await trigger.aclose()
    assert [ns.session for ns, _ in promoter.calls] == ["s1"]


async def test_session_end_clears_that_sessions_counter() -> None:
    """A finished session must not leave a partial count behind for a later session id reuse."""
    promoter = _RecordingPromoter()
    trigger = _trigger(promoter, every_n=3)

    await trigger.on_capture("s1")
    await trigger.on_capture("s1")
    await trigger.on_session_end(_activity(ActivityKind.SESSION_END))
    await trigger.aclose()

    # the two pre-end captures must not count toward the next cycle
    await trigger.on_capture("s1")
    await trigger.aclose()
    assert len(promoter.calls) == 1, "a stale counter drained the buffer early"


def test_a_zero_threshold_is_rejected() -> None:
    with pytest.raises(ValueError, match="consolidate_every_n"):
        _trigger(_RecordingPromoter(), every_n=0)


async def test_a_second_consolidation_for_the_same_session_coalesces() -> None:
    """Consolidation is the SLOWEST thing on this path (a real extraction, possibly through an
    SLM). It runs in the outbox WORKER, so a pile-up never stalls the host turn — but it does stall
    the drain queue behind it, and re-running the same window is pure waste. One at a time per
    session, mirroring `MaintenanceLoop._inflight`.
    """
    import asyncio

    gate = asyncio.Event()

    class _BlockingPromoter(_RecordingPromoter):
        async def promote_session_now(self, ns: Namespace, *, force: bool = False) -> None:
            self.calls.append((ns, force))
            await gate.wait()

    promoter = _BlockingPromoter()
    trigger = _trigger(promoter, every_n=1)

    assert await trigger.on_capture("s1") is True  # schedules, does NOT await
    await asyncio.sleep(0)  # let the background task reach the blocking promoter
    second = await trigger.on_capture("s1")  # must NOT queue a duplicate

    assert second is False, "a duplicate consolidation was queued for a session already draining"
    assert trigger.coalesced_count == 1
    assert len(promoter.calls) == 1

    gate.set()
    await trigger.aclose()


async def test_a_different_session_is_not_blocked_by_one_in_flight() -> None:
    """Coalescing is PER SESSION: two agents working concurrently must not serialise behind each
    other, which would turn one slow extraction into a global stall."""
    import asyncio

    gate = asyncio.Event()

    class _BlockingPromoter(_RecordingPromoter):
        async def promote_session_now(self, ns: Namespace, *, force: bool = False) -> None:
            self.calls.append((ns, force))
            if ns.session == "s1":
                await gate.wait()

    promoter = _BlockingPromoter()
    trigger = _trigger(promoter, every_n=1)

    assert await trigger.on_capture("s1") is True
    await asyncio.sleep(0)
    assert await trigger.on_capture("s2") is True, "a different session was blocked"
    assert trigger.coalesced_count == 0

    gate.set()
    await trigger.aclose()


async def test_consolidation_never_blocks_the_capture_path() -> None:
    """THE latency regression this design exists to avoid, caught by AC-1.2's capture-ack p99 test.

    Consolidation is CPU-bound (SPO extraction + a real MiniLM embedding pass) and the daemon
    serves capture ACKs from the SAME event loop. Awaiting it inline stalled every ack queued
    behind it and blew the 50ms p99 delta budget. The trigger must SCHEDULE and return, never wait.
    """
    import asyncio

    gate = asyncio.Event()

    class _BlockingPromoter(_RecordingPromoter):
        async def promote_session_now(self, ns: Namespace, *, force: bool = False) -> None:
            self.calls.append((ns, force))
            await gate.wait()

    promoter = _BlockingPromoter()
    trigger = _trigger(promoter, every_n=1)

    # The promoter blocks forever until `gate` is set. If `on_capture` awaited the drain, this
    # line would hang and the test would time out.
    await asyncio.wait_for(trigger.on_capture("s1"), timeout=1.0)

    gate.set()
    await trigger.aclose()
    assert len(promoter.calls) == 1, "the scheduled consolidation never actually ran"


async def test_a_failing_consolidation_is_logged_not_raised_into_the_capture_path() -> None:
    """A background task must never die silently, and must never surface as a capture failure —
    the turns are already durable in STM and the next trigger retries."""

    class _FailingPromoter(_RecordingPromoter):
        async def promote_session_now(self, ns: Namespace, *, force: bool = False) -> None:
            self.calls.append((ns, force))
            raise RuntimeError("store down")

    promoter = _FailingPromoter()
    trigger = _trigger(promoter, every_n=1)

    await trigger.on_capture("s1")  # must not raise
    await trigger.aclose()

    assert len(promoter.calls) == 1
    # the session must not be left stuck "in flight" after a failure, or it could never drain again
    assert await trigger.on_capture("s1") is True
    await trigger.aclose()
