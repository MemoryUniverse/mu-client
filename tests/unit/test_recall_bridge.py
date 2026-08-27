"""``RecallInjectBridge`` — fresh/cold/stale/budget-spill, isolated logic (mocks permitted)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from mu_contracts.contracts.recall import RecallItemView
from mu_contracts.domain.errors import StoreUnavailableError
from mu_contracts.domain.model.memory import Tier
from mu_local.views import MemoryListView

from mu_client.config import InjectSettings
from mu_client.host import LocalMemoryHost
from mu_client.inject.recall_bridge import RecallInjectBridge

pytestmark = pytest.mark.unit


def _listing(*contents: str) -> MemoryListView:
    return MemoryListView(
        items=[
            RecallItemView(
                memory_id=f"m{i}", content=c, tier=Tier.STM, channel="stm", fused_score=1.0
            )
            for i, c in enumerate(contents)
        ]
    )


@pytest.fixture
async def started_host(monkeypatch: pytest.MonkeyPatch) -> LocalMemoryHost:
    fake_memory = AsyncMock()
    monkeypatch.setattr("mu_client.host.LocalMemory", lambda *a, **kw: fake_memory)
    host = LocalMemoryHost()
    await host.start()
    return host


async def test_fresh_render_includes_recalled_content(started_host: LocalMemoryHost) -> None:
    started_host._memory.recall.return_value = _listing("Ada lives in Paris")  # type: ignore[union-attr]
    bridge = RecallInjectBridge(started_host, settings=InjectSettings())
    rendered = await bridge.render("s1", query="Where does Ada live?")
    assert rendered.staleness == "fresh"
    assert "Ada lives in Paris" in rendered.body
    assert rendered.etag


async def test_cold_cache_on_empty_recall(started_host: LocalMemoryHost) -> None:
    started_host._memory.recall.return_value = _listing()  # type: ignore[union-attr]
    bridge = RecallInjectBridge(started_host, settings=InjectSettings())
    rendered = await bridge.render("s1")
    assert rendered.staleness == "cold"
    assert rendered.body == ""


async def test_stale_fallback_serves_last_good_snapshot_on_engine_down(
    started_host: LocalMemoryHost,
) -> None:
    started_host._memory.recall.return_value = _listing("fact one")  # type: ignore[union-attr]
    bridge = RecallInjectBridge(started_host, settings=InjectSettings())
    fresh = await bridge.render("s1")
    assert fresh.staleness == "fresh"

    started_host._memory.recall.side_effect = StoreUnavailableError("down")  # type: ignore[union-attr]
    stale = await bridge.render("s1")
    assert stale.staleness == "stale"
    assert "fact one" in stale.body


async def test_cold_when_engine_down_and_never_rendered_before(
    started_host: LocalMemoryHost,
) -> None:
    started_host._memory.recall.side_effect = StoreUnavailableError("down")  # type: ignore[union-attr]
    bridge = RecallInjectBridge(started_host, settings=InjectSettings())
    rendered = await bridge.render("never-seen-session")
    assert rendered.staleness == "cold"
    assert rendered.body == ""


async def test_render_drops_tool_noise_keeps_facts(started_host: LocalMemoryHost) -> None:
    # Gap D: a mixed listing — 2 human facts + a Write tool-capture + a Bash-output line. The
    # rendered inject body KEEPS the facts and DROPS the tool noise.
    started_host._memory.recall.return_value = _listing(  # type: ignore[union-attr]
        "My deploy target is staging-eu",
        'Write: {"file_path": "/app/main.py", "content": "print(1)"}',
        "The on-call engineer is Ada",
        "Bash: total 48\ndrwxr-xr-x 2 user user 4096 main.py",
    )
    bridge = RecallInjectBridge(started_host, settings=InjectSettings())
    rendered = await bridge.render("s1")
    assert "My deploy target is staging-eu" in rendered.body
    assert "The on-call engineer is Ada" in rendered.body
    assert "Write:" not in rendered.body and "Bash:" not in rendered.body


async def test_render_dedupes_repeated_content(started_host: LocalMemoryHost) -> None:
    started_host._memory.recall.return_value = _listing(  # type: ignore[union-attr]
        "Ada lives in Paris", "Ada lives in Paris", "Ada lives in Paris"
    )
    bridge = RecallInjectBridge(started_host, settings=InjectSettings())
    rendered = await bridge.render("s1")
    assert rendered.body.count("Ada lives in Paris") == 1


async def test_over_budget_spills_to_file_never_silently_truncated(
    started_host: LocalMemoryHost, tmp_path: Path
) -> None:
    # Distinct facts (dedup would otherwise collapse identical lines) — each a genuine,
    # non-noise memory, so the budget spill path is what is exercised, not the distiller.
    started_host._memory.recall.return_value = _listing(  # type: ignore[union-attr]
        *(f"distinct salient fact number {i} " + "x" * 200 for i in range(100))
    )
    settings = InjectSettings(body_budget_chars=500)
    bridge = RecallInjectBridge(started_host, settings=settings, recall_dir=tmp_path / "recall")
    rendered = await bridge.render("s1")
    assert rendered.staleness == "fresh"
    assert len(rendered.body) <= 500
    assert "spilled to" in rendered.body
    spill_files = list((tmp_path / "recall").glob("*.txt"))
    assert len(spill_files) == 1
    assert len(spill_files[0].read_text()) > 500
