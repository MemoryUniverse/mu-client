"""``LocalMemoryHost`` lifecycle — isolated logic, mocks permitted (DEV-STANDARDS: mocks ONLY in
pure unit tests). The REAL round-trip against real stores lives in
``tests/integration/test_daemonless_roundtrip_int.py``."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from mu_client.errors import ClientNotStartedError
from mu_client.host import LocalMemoryHost

pytestmark = pytest.mark.unit


async def test_verb_before_start_raises_client_not_started() -> None:
    host = LocalMemoryHost()
    with pytest.raises(ClientNotStartedError):
        await host.add("hello")


async def test_double_start_returns_same_memory_instance(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_memory = AsyncMock()
    monkeypatch.setattr("mu_client.host.LocalMemory", lambda *a, **kw: fake_memory)
    host = LocalMemoryHost()
    first = await host.start()
    second = await host.start()
    assert first is fake_memory
    assert second is first  # idempotent: no silent second engine construction


async def test_aclose_before_start_is_a_safe_noop() -> None:
    host = LocalMemoryHost()
    await host.aclose()  # must not raise
    assert host.is_started is False


async def test_aclose_clears_reference_before_awaiting_teardown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancellation-safety contract (host.py docstring): ``is_started`` flips to False BEFORE the
    underlying ``aclose()`` coroutine is awaited, so a cancelled teardown never reports 'still
    started'."""
    fake_memory = AsyncMock()
    monkeypatch.setattr("mu_client.host.LocalMemory", lambda *a, **kw: fake_memory)
    host = LocalMemoryHost()
    await host.start()
    started: bool = host.is_started
    assert started

    await host.aclose()
    closed: bool = host.is_started
    assert not closed
    fake_memory.aclose.assert_awaited_once()


async def test_context_manager_starts_and_closes(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_memory = AsyncMock()
    monkeypatch.setattr("mu_client.host.LocalMemory", lambda *a, **kw: fake_memory)
    async with LocalMemoryHost() as host:
        started: bool = host.is_started
        assert started
    closed: bool = host.is_started
    assert not closed
    fake_memory.aclose.assert_awaited_once()
