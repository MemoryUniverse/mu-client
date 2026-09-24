"""CLI arg-parsing + rendering — isolated logic, mocks permitted (DEV-STANDARDS: pure unit test).
The REAL end-to-end CLI round-trip (subprocess, real stores) lives in
``tests/integration/test_daemonless_roundtrip_int.py``."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from mu_contracts.contracts.recall import RecallItemView
from mu_contracts.domain.model.memory import Tier
from mu_local.views import MemoryListView, MemoryWriteResult

from mu_client import cli
from mu_client.outbox.sqlite_outbox import OUTBOX_REDRIVE_ROUTE

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
    # ``importance_score`` is the new ``--importance`` lever (Phase 3): omitting the flag threads
    # ``None`` (engine default applies), so the default add is byte-for-byte its prior behaviour.
    fake_host.add.assert_awaited_once_with(
        "Ada lives in Paris", user=None, session=None, importance_score=None
    )


async def test_run_add_threads_importance_flag(monkeypatch: pytest.MonkeyPatch) -> None:
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
    code = await cli._run(["add", "Ada lives in Paris", "--importance", "0.9"])
    assert code == 0
    fake_host.add.assert_awaited_once_with(
        "Ada lives in Paris", user=None, session=None, importance_score=0.9
    )


async def test_run_recall_calls_host_recall_and_renders_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_host = AsyncMock()
    fake_host.recall.return_value = MemoryListView(
        items=[
            RecallItemView(
                memory_id="m1",
                content="Ada lives in Paris",
                tier=Tier.MTM,
                channel="mtm",
                fused_score=0.9,
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


# =====================================================================================
# AD-270a fix (ADR 0072, PROTOTYPE-DEBT-0924.md B3) — `mu outbox redrive`. The route/store logic
# itself is proven end-to-end in `tests/unit/test_outbox_redrive_route_unit.py`; these cover only
# this module's own responsibility: parsing + the default-limit resolution + the IPC call shape.
# =====================================================================================


def test_outbox_redrive_parses_with_no_limit() -> None:
    args = cli._build_parser().parse_args(["outbox", "redrive"])
    assert args.command == "outbox"
    assert args.outbox_action == "redrive"
    assert args.limit is None


def test_outbox_redrive_parses_an_explicit_limit() -> None:
    args = cli._build_parser().parse_args(["outbox", "redrive", "--limit", "5"])
    assert args.limit == 5


def test_outbox_requires_a_subaction() -> None:
    with pytest.raises(SystemExit):
        cli._build_parser().parse_args(["outbox"])


async def test_run_outbox_redrive_defaults_limit_to_outbox_batch_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """**MUTATION:** hardcode ``limit=100`` in ``_run_outbox_redrive`` instead of reading
    ``settings.outbox.batch_size`` -> this test goes RED (default ``OutboxSettings.batch_size`` is
    64, not 100)."""
    fake_client = AsyncMock()
    fake_client.request.return_value = {"status": 200, "redriven": 3}
    monkeypatch.setattr(cli, "IpcClient", lambda *a, **kw: fake_client)

    code = await cli._run(["outbox", "redrive"])

    assert code == 0
    fake_client.request.assert_awaited_once_with(OUTBOX_REDRIVE_ROUTE, {"limit": 64})


async def test_run_outbox_redrive_threads_an_explicit_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_client = AsyncMock()
    fake_client.request.return_value = {"status": 200, "redriven": 0}
    monkeypatch.setattr(cli, "IpcClient", lambda *a, **kw: fake_client)

    code = await cli._run(["outbox", "redrive", "--limit", "7"])

    assert code == 0
    fake_client.request.assert_awaited_once_with(OUTBOX_REDRIVE_ROUTE, {"limit": 7})


async def test_run_outbox_redrive_surfaces_an_ipc_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """A daemon that is not running must be a named CLI failure, never a silent 0."""
    fake_client = AsyncMock()
    fake_client.request.return_value = {"status": 503, "error": "daemon_unreachable"}
    monkeypatch.setattr(cli, "IpcClient", lambda *a, **kw: fake_client)

    code = await cli._run(["outbox", "redrive"])

    assert code != 0
