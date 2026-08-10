"""Unit: PreCompact routing (AGENT-INTEGRATION-AUDIT-AND-PLAN.md §4 Phase 3).

Proves the WIRING, not the store side (the real-store promotion proof lives in
``tests/integration/test_precompact_promote_int.py``): with a promoter wired, a ``PreCompact``
control activity is routed PAST the control-kind skip into ``PreCompactPromoter.on_precompact``
(force-promote the session) instead of being silently swallowed; without one it falls back to the
pre-Phase-3 skip; and a normal activity's path is untouched. The doubles here are tiny in-process
spies for the two collaborators (host.add / the session promoter) — a unit test's legitimate seam,
never a stand-in for a real store.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from mu_contracts.domain.model.memory import Namespace, Visibility

from mu_client.capture.model import ActivityKind, HostKind, RawActivity
from mu_client.lifecycle.precompact import PreCompactPromoter
from mu_client.workers.ingest_client import ExtractionSkippedError, InProcessLocalIngest

pytestmark = pytest.mark.unit


class _SpyHost:
    """Records ``add`` calls — stands in for ``LocalMemoryHost`` in the ingest routing test."""

    def __init__(self) -> None:
        self.adds: list[str] = []

    async def add(self, text: str, **_: object) -> object:
        self.adds.append(text)
        return object()


class _SpyPromoter:
    """Records ``promote_session_now`` calls — satisfies ``SessionPromoterPort`` structurally."""

    def __init__(self) -> None:
        self.calls: list[tuple[Namespace, bool]] = []

    async def promote_session_now(self, ns: Namespace, *, force: bool = False) -> None:
        self.calls.append((ns, force))


class _SpyOnPrecompact:
    """Records ``on_precompact`` — stands in for ``PreCompactPromoter`` in the ingest routing test.

    Only the one method ``InProcessLocalIngest`` calls is needed; the ingest never touches anything
    else on the promoter, so this narrow spy is a faithful unit seam."""

    def __init__(self) -> None:
        self.seen: list[RawActivity] = []

    async def on_precompact(self, activity: RawActivity) -> None:
        self.seen.append(activity)


def _activity(kind: ActivityKind, *, text: str | None, session: str = "s1") -> RawActivity:
    return RawActivity(
        activity_id=f"act-{kind.value}",
        host=HostKind.CLAUDE_CODE,
        host_version="test",
        schema_version="claude_code.v1",
        kind=kind,
        session_id=session,
        occurred_at=datetime.now(UTC),
        text=text,
        content_hash=None,
        source_offset="0",
        provenance_id="prov_test",
        payload={"trigger": "auto"} if kind is ActivityKind.PRE_COMPACT else {},
    )


async def test_precompact_routed_to_promoter_then_ack_skips_without_storing() -> None:
    host = _SpyHost()
    promoter = _SpyOnPrecompact()
    ingest = InProcessLocalIngest(host, user="u", precompact_promoter=promoter)  # type: ignore[arg-type]
    activity = _activity(ActivityKind.PRE_COMPACT, text=None)

    # Routed to the promoter (promotion side-effect ran), then ack'd via the skip sentinel — the
    # PreCompact control event itself never becomes a MemoryItem.
    with pytest.raises(ExtractionSkippedError):
        await ingest.ingest(activity)

    assert promoter.seen == [activity], "PreCompact was NOT routed to the promoter (still a no-op?)"
    assert host.adds == [], "PreCompact must never be stored as a MemoryItem"


async def test_precompact_without_promoter_falls_back_to_plain_skip() -> None:
    host = _SpyHost()
    ingest = InProcessLocalIngest(host, user="u", precompact_promoter=None)  # type: ignore[arg-type]

    with pytest.raises(ExtractionSkippedError):
        await ingest.ingest(_activity(ActivityKind.PRE_COMPACT, text=None))

    assert host.adds == [], "backward-compat: no promoter ⇒ old skip-and-ack, nothing stored"


async def test_normal_activity_still_reaches_host_add_unchanged() -> None:
    host = _SpyHost()
    promoter = _SpyOnPrecompact()
    ingest = InProcessLocalIngest(host, user="u", precompact_promoter=promoter)  # type: ignore[arg-type]

    await ingest.ingest(_activity(ActivityKind.USER_PROMPT, text="remember me"))

    assert host.adds == ["remember me"], "a normal turn must ingest unchanged (no PreCompact path)"
    assert promoter.seen == [], "a non-PreCompact activity must never touch the promoter"


async def test_promoter_builds_session_namespace_and_forces_promotion() -> None:
    spy = _SpyPromoter()
    promoter = PreCompactPromoter(promoter=spy, org="orgX", workspace="wsX", user="alice")
    await promoter.on_precompact(_activity(ActivityKind.PRE_COMPACT, text=None, session="sess-42"))

    assert len(spy.calls) == 1
    ns, force = spy.calls[0]
    assert force is True, "PreCompact must FORCE-promote (bypass the routine salience gate)"
    assert ns == Namespace(
        org="orgX", workspace="wsX", user="alice", session="sess-42", visibility=Visibility.PRIVATE
    )


async def test_promoter_rejects_misrouted_non_precompact_activity() -> None:
    spy = _SpyPromoter()
    promoter = PreCompactPromoter(promoter=spy, org="o", workspace="w", user="u")
    with pytest.raises(ValueError, match="non-PreCompact"):
        await promoter.on_precompact(_activity(ActivityKind.USER_PROMPT, text="hi"))
    assert spy.calls == [], "a misrouted activity must never trigger a promotion"
