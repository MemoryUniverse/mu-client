"""``RecallInjectBridge`` — fresh/cold/stale/budget-spill, isolated logic (mocks permitted)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from mu_contracts.domain.errors import StoreUnavailableError
from mu_local.views import MemoryListView, MemoryRecordView

from mu_client.config import InjectSettings
from mu_client.host import LocalMemoryHost
from mu_client.inject.recall_bridge import RecallInjectBridge

pytestmark = pytest.mark.unit


def _listing(*contents: str) -> MemoryListView:
    return MemoryListView(
        items=[
            MemoryRecordView(memory_id=f"m{i}", content=c, tier="stm", channel="stm", score=1.0)
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


async def test_over_budget_spills_to_file_never_silently_truncated(
    started_host: LocalMemoryHost, tmp_path: Path
) -> None:
    huge = "x" * 200
    started_host._memory.recall.return_value = _listing(*(huge for _ in range(100)))  # type: ignore[union-attr]
    settings = InjectSettings(body_budget_chars=500)
    bridge = RecallInjectBridge(started_host, settings=settings, recall_dir=tmp_path / "recall")
    rendered = await bridge.render("s1")
    assert rendered.staleness == "fresh"
    assert len(rendered.body) <= 500
    assert "spilled to" in rendered.body
    spill_files = list((tmp_path / "recall").glob("*.txt"))
    assert len(spill_files) == 1
    assert len(spill_files[0].read_text()) > 500
