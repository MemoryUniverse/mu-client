"""``ClientSettings`` — pure config-shape tests (no I/O, no containers)."""

from __future__ import annotations

from pathlib import Path

import pytest
from mu_contracts.config.settings import StorageSettings as CoreStorageSettings

from mu_client.config import ClientSettings, ModelProfileSettings

pytestmark = pytest.mark.unit


def test_storage_reuses_mu_core_storage_settings_class() -> None:
    """The brief: "store endpoints (reuse mu-core's StorageSettings shape)" — literally the SAME
    class, not a re-shaped copy."""
    settings = ClientSettings()
    assert isinstance(settings.storage, CoreStorageSettings)


def test_model_profile_points_at_mu_dev_slm_by_default() -> None:
    settings = ClientSettings()
    assert settings.model == ModelProfileSettings()
    assert settings.model.base_url == "http://127.0.0.1:11435"
    assert settings.model.model_name == "qwen2.5:0.5b"


def test_daemon_and_outbox_paths_default_under_memory_universe_dir() -> None:
    settings = ClientSettings()
    assert settings.daemon_socket_path == Path("~/.memory-universe/daemon.sock")
    assert settings.outbox_db_path == Path("~/.memory-universe/outbox.sqlite")


def test_env_override_reaches_storage_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    """No hardcoded literal: an env var override on the SAME MU_ namespace mu-core uses reaches
    ClientSettings.storage, proving no separate/duplicated translation layer exists."""
    monkeypatch.setenv("MU_STORAGE__CACHE__PORT", "19999")
    settings = ClientSettings()
    assert settings.storage.cache.port == 19999


def test_defaults_are_frozen_model_values_not_mutable_globals() -> None:
    a = ClientSettings()
    b = ClientSettings()
    assert a.model == b.model
    assert a.model is not b.model  # each construction builds its own instance (no shared mutable)


def test_nested_daemon_stage_subtrees_default_under_memory_universe_dir() -> None:
    """capture-spec.md §10 / daemon-app-skeleton-spec.md §9 — THIS stage's subtrees, same literal
    default paths as the pre-existing flat ``daemon_socket_path``/``outbox_db_path`` seams."""
    settings = ClientSettings()
    assert settings.outbox.outbox_path == Path("~/.memory-universe/outbox.sqlite")
    assert settings.ipc.socket_path == Path("~/.memory-universe/daemon.sock")
    assert settings.inject.body_budget_chars == 10_000  # F4
    assert settings.capture.tool_outcome_max_chars == 500


def test_nested_subtree_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MU_OUTBOX__BATCH_SIZE", "128")
    monkeypatch.setenv("MU_INJECT__TOP_K", "3")
    settings = ClientSettings()
    assert settings.outbox.batch_size == 128
    assert settings.inject.top_k == 3
