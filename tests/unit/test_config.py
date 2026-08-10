"""``ClientSettings`` — pure config-shape tests (no I/O, no containers)."""

from __future__ import annotations

from pathlib import Path

import pytest
from mu_contracts.config.settings import StorageSettings as CoreStorageSettings

from mu_client.config import (
    MU_ENV_FILE_VAR,
    ClientSettings,
    ModelProfileSettings,
    render_endpoint_env,
    resolve_env_files,
)

pytestmark = pytest.mark.unit


def test_storage_reuses_mu_core_storage_settings_class() -> None:
    """The brief: "store endpoints (reuse mu-core's StorageSettings shape)" — literally the SAME
    class, not a re-shaped copy."""
    settings = ClientSettings()
    assert isinstance(settings.storage, CoreStorageSettings)


def test_model_profile_points_at_mu_dev_slm_by_default() -> None:
    settings = ClientSettings()
    assert settings.model == ModelProfileSettings()
    assert settings.model is not None
    assert settings.model.base_url == "http://127.0.0.1:11435/v1"
    assert settings.model.model_name == "qwen2.5:0.5b"
    assert settings.model.provider == "openai"


def test_model_profile_can_opt_out_to_none_for_heuristic_mode() -> None:
    """Backward-compat escape hatch: an explicit ``model=None`` keeps ``LocalMemoryHost.start()``
    in heuristic mode (mirrors ``mu_local``'s own ``StorageSettings(llm=None)`` default)."""
    settings = ClientSettings(model=None)
    assert settings.model is None


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


# ── gap A: CWD-independent env-file resolution ────────────────────────────────────────────────


def test_resolve_env_files_order_is_ascending_precedence(tmp_path: Path) -> None:
    """The fixed user-config comes FIRST (lowest priority among files) and the cwd's ``.env``/
    ``.env.test`` LAST (highest), matching pydantic-settings' last-file-wins tuple semantics."""
    user_cfg = tmp_path / "config.env"
    cwd = tmp_path / "proj"
    files = resolve_env_files(cwd=cwd, environ={}, user_config=user_cfg)
    assert files == (user_cfg, cwd / ".env", cwd / ".env.test")


def test_resolve_env_files_inserts_explicit_mu_env_file(tmp_path: Path) -> None:
    explicit = tmp_path / "abs" / "endpoints.env"
    cwd = tmp_path / "proj"
    files = resolve_env_files(
        cwd=cwd, environ={MU_ENV_FILE_VAR: str(explicit)}, user_config=tmp_path / "config.env"
    )
    # explicit override sits between the fixed user-config and the cwd dev files
    assert files == (tmp_path / "config.env", explicit, cwd / ".env", cwd / ".env.test")


def test_fixed_path_endpoints_reach_storage_regardless_of_cwd(tmp_path: Path) -> None:
    """The core gap-A proof, at unit scope: with NO cwd ``.env`` at all, an absolute fixed-path
    config file still populates the store endpoints — so a ``mu-mcp`` spawned from an arbitrary
    directory does NOT fall back to the in-container defaults (:6379/:6333)."""
    fixed = tmp_path / "config.env"
    fixed.write_text(
        "MU_STORAGE__CACHE__HOST=127.0.0.1\n"
        "MU_STORAGE__CACHE__PORT=16379\n"
        "MU_STORAGE__VECTOR__HOST=127.0.0.1\n"
        "MU_STORAGE__VECTOR__HTTP_PORT=16333\n"
        "MU_STORAGE__GRAPH__PORT=16380\n",
        encoding="utf-8",
    )
    empty_cwd = tmp_path / "arbitrary_project"  # no .env / .env.test here
    files = resolve_env_files(cwd=empty_cwd, environ={}, user_config=fixed)

    settings = ClientSettings(_env_file=files)  # type: ignore[call-arg]

    assert settings.storage.cache.port == 16379  # NOT the 6379 in-container default
    assert settings.storage.vector.http_port == 16333  # NOT 6333
    assert settings.storage.graph.port == 16380
    assert settings.storage.cache.host == "127.0.0.1"


def test_os_env_var_beats_the_fixed_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The installer-written MCP ``env`` block is a real OS env var — it must win over any file, so
    the registered server's endpoints are authoritative."""
    fixed = tmp_path / "config.env"
    fixed.write_text("MU_STORAGE__CACHE__PORT=16379\n", encoding="utf-8")
    monkeypatch.setenv("MU_STORAGE__CACHE__PORT", "26379")
    files = resolve_env_files(cwd=tmp_path / "x", environ={}, user_config=fixed)

    settings = ClientSettings(_env_file=files)  # type: ignore[call-arg]

    assert settings.storage.cache.port == 26379  # OS env wins over the fixed file


def test_render_endpoint_env_flattens_resolved_endpoints() -> None:
    """The self-contained MCP ``env`` block: resolved endpoints flattened back to MU_* vars."""
    settings = ClientSettings(model=None)
    env = render_endpoint_env(settings)

    assert env["MU_RUNTIME_MODE"] == "local"
    # a representative endpoint from each private-plane store role is present + stringified
    assert env["MU_STORAGE__CACHE__HOST"] == settings.storage.cache.host
    assert env["MU_STORAGE__CACHE__PORT"] == str(settings.storage.cache.port)
    assert env["MU_STORAGE__VECTOR__HTTP_PORT"] == str(settings.storage.vector.http_port)
    assert env["MU_STORAGE__GRAPH__PORT"] == str(settings.storage.graph.port)
    assert all(isinstance(v, str) for v in env.values())  # a valid env block is all-strings


def test_render_endpoint_env_includes_model_profile_when_present() -> None:
    settings = ClientSettings()  # default model profile (mu-dev-slm)
    env = render_endpoint_env(settings)
    assert env["MU_MODEL__PROVIDER"] == "openai"
    assert env["MU_MODEL__BASE_URL"] == "http://127.0.0.1:11435/v1"
    assert env["MU_MODEL__MODEL_NAME"] == "qwen2.5:0.5b"


def test_roundtrip_endpoint_env_reconstructs_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A full loop: custom endpoints -> render_endpoint_env -> feed back as OS env -> identical
    resolved endpoints. Proves the MCP ``env`` block is a faithful, self-contained carrier."""
    monkeypatch.setenv("MU_STORAGE__CACHE__PORT", "17000")
    monkeypatch.setenv("MU_STORAGE__VECTOR__HTTP_PORT", "17001")
    source = ClientSettings(model=None)
    env = render_endpoint_env(source)

    monkeypatch.delenv("MU_STORAGE__CACHE__PORT")
    monkeypatch.delenv("MU_STORAGE__VECTOR__HTTP_PORT")
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    rebuilt = ClientSettings(_env_file=(tmp_path / "none.env",), model=None)  # type: ignore[call-arg]

    assert rebuilt.storage.cache.port == 17000
    assert rebuilt.storage.vector.http_port == 17001
