"""CLI arg-parsing + rendering — isolated logic, mocks permitted (DEV-STANDARDS: pure unit test).
The REAL end-to-end CLI round-trip (subprocess, real stores) lives in
``tests/integration/test_daemonless_roundtrip_int.py``."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from mu_local.views import MemoryListView, MemoryRecordView, MemoryWriteResult

from mu_client import cli

pytestmark = pytest.mark.unit


def test_parser_requires_a_subcommand() -> None:
    with pytest.raises(SystemExit):
        cli._build_parser().parse_args([])


def test_add_parses_content_and_optional_flags() -> None:
    args = cli._build_parser().parse_args(["add", "hello world", "--user", "u1", "--session", "s1"])
    assert args.command == "add"
    assert args.content == "hello world"
    assert args.user == "u1"
    assert args.session == "s1"


def test_recall_defaults_tier_to_none_and_limit_to_ten() -> None:
    args = cli._build_parser().parse_args(["recall", "where is Ada"])
    assert args.tier is None
    assert args.limit == 10


def test_recall_rejects_an_unknown_tier() -> None:
    with pytest.raises(SystemExit):
        cli._build_parser().parse_args(["recall", "q", "--tier", "not-a-tier"])


async def test_run_add_calls_host_add_and_renders_result(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_host = AsyncMock()
    fake_host.add.return_value = MemoryWriteResult(
        memory_id="m1", content_hash="deadbeef", promoted=True, tiers_written=("stm", "mtm")
    )

    class _FakeCtx:
        async def __aenter__(self) -> AsyncMock:
            return fake_host

        async def __aexit__(self, *exc: object) -> None:
            return None

    monkeypatch.setattr(cli, "daemonless_host", lambda *a, **kw: _FakeCtx())
    code = await cli._run(["add", "Ada lives in Paris"])
    assert code == 0
    fake_host.add.assert_awaited_once_with("Ada lives in Paris", user=None, session=None)


async def test_run_recall_calls_host_recall_and_renders_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_host = AsyncMock()
    fake_host.recall.return_value = MemoryListView(
        items=[
            MemoryRecordView(
                memory_id="m1", content="Ada lives in Paris", tier="mtm", channel="mtm", score=0.9
            )
        ]
    )

    class _FakeCtx:
        async def __aenter__(self) -> AsyncMock:
            return fake_host

        async def __aexit__(self, *exc: object) -> None:
            return None

    monkeypatch.setattr(cli, "daemonless_host", lambda *a, **kw: _FakeCtx())
    code = await cli._run(["recall", "where does Ada live"])
    assert code == 0
    fake_host.recall.assert_awaited_once()
