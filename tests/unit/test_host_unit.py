"""``LocalMemoryHost`` lifecycle — isolated logic, mocks permitted (DEV-STANDARDS: mocks ONLY in
pure unit tests). The REAL round-trip against real stores lives in
``tests/integration/test_daemonless_roundtrip_int.py``."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from mu_local.config import StorageSettings as LocalBackendSettings

from mu_client.config import ClientSettings, ModelProfileSettings
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


async def test_start_wires_configured_model_profile_into_backend_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The defect this locks in: ``start()`` used to construct ``LocalBackendSettings()`` with NO
    arguments, so ``storage.llm`` was always ``None`` regardless of ``ClientSettings.model`` — the
    daemon's ``LocalMemory`` silently stayed in heuristic mode even with a real profile configured.
    A configured (non-``None``) ``ClientSettings.model`` must now reach ``LocalMemory`` as a real
    ``mu_local.config.ModelProfileSettings`` on ``StorageSettings.llm``."""
    captured: dict[str, object] = {}
    fake_memory = AsyncMock()

    def _fake_local_memory(storage: LocalBackendSettings, **_: object) -> AsyncMock:
        captured["storage"] = storage
        return fake_memory

    monkeypatch.setattr("mu_client.host.LocalMemory", _fake_local_memory)
    settings = ClientSettings(
        model=ModelProfileSettings(
            provider="openai", base_url="http://127.0.0.1:11435/v1", model_name="qwen2.5:0.5b"
        )
    )
    host = LocalMemoryHost(settings)
    await host.start()

    storage = captured["storage"]
    assert isinstance(storage, LocalBackendSettings)
    assert storage.llm is not None
    assert storage.llm.provider == "openai"
    assert storage.llm.base_url == "http://127.0.0.1:11435/v1"
    assert storage.llm.model == "qwen2.5:0.5b"


async def test_start_with_none_model_profile_keeps_heuristic_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Backward compat: the explicit ``ClientSettings(model=None)`` opt-out must still reach
    ``LocalMemory`` as ``StorageSettings.llm=None`` — byte-for-byte the prior heuristic-mode
    behaviour, never silently upgraded to an LLM profile."""
    captured: dict[str, object] = {}
    fake_memory = AsyncMock()

    def _fake_local_memory(storage: LocalBackendSettings, **_: object) -> AsyncMock:
        captured["storage"] = storage
        return fake_memory

    monkeypatch.setattr("mu_client.host.LocalMemory", _fake_local_memory)
    settings = ClientSettings(model=None)
    host = LocalMemoryHost(settings)
    await host.start()

    storage = captured["storage"]
    assert isinstance(storage, LocalBackendSettings)
    assert storage.llm is None
