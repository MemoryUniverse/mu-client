"""Real-file unit tests for ``mu install|uninstall codex`` (no mocks — plain temp TOML files, per
the Phase-4 acceptance gate): idempotent re-install, backup-first, never-clobber a foreign
``notify`` / an unrelated ``mcp_servers.*`` entry / an unrelated top-level key. Reads results back
with stdlib ``tomllib`` (the authoritative parser) so we assert on what codex actually loads."""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

from mu_client.install import codex

pytestmark = pytest.mark.unit

_NOTIFY = Path("/opt/mu/scripts/hooks/mu_codex_notify.sh")
_ENV = {"MU_RUNTIME_MODE": "local", "MU_STORAGE__CACHE__HOST": "127.0.0.1"}


def _read(path: Path) -> dict[str, object]:
    with path.open("rb") as fh:
        return tomllib.load(fh)


def test_install_fresh_file_writes_notify_and_mcp(tmp_path: Path) -> None:
    config = tmp_path / "config.toml"
    result = codex.install(config, notify_script_path=_NOTIFY, mcp_env=_ENV)

    assert result.backup_path is None  # nothing existed to back up
    assert result.notify_written is True
    assert result.notify_conflict is False
    assert result.mcp_server_registered is True
    assert result.endpoint_vars_written == len(_ENV)

    doc = _read(config)
    assert doc["notify"] == [str(_NOTIFY)]
    server = doc["mcp_servers"]["memory-universe"]
    assert server["command"] == codex.DEFAULT_MCP_COMMAND
    assert server["args"] == []
    assert server["env"] == _ENV


def test_install_is_idempotent(tmp_path: Path) -> None:
    config = tmp_path / "config.toml"
    codex.install(config, notify_script_path=_NOTIFY, mcp_env=_ENV)
    before = config.read_text()

    result = codex.install(config, notify_script_path=_NOTIFY, mcp_env=_ENV)

    assert result.notify_written is False
    assert result.notify_already_present is True
    # notify stays a single managed entry; the one memory-universe server stays single.
    doc = _read(config)
    assert doc["notify"] == [str(_NOTIFY)]
    assert list(doc["mcp_servers"]).count("memory-universe") == 1
    # a backup was taken (file existed), but the notify content is unchanged.
    assert _read(Path(str(config) + ".bak"))["notify"] == [str(_NOTIFY)]
    assert "notify" in before


def test_install_backs_up_existing_file(tmp_path: Path) -> None:
    config = tmp_path / "config.toml"
    config.write_text('approval_policy = "never"\n', encoding="utf-8")

    result = codex.install(config, notify_script_path=_NOTIFY, mcp_env=_ENV)

    assert result.backup_path == config.with_name("config.toml.bak")
    assert _read(result.backup_path) == {"approval_policy": "never"}
    # the original top-level key survives in the written file.
    assert _read(config)["approval_policy"] == "never"


def test_install_never_clobbers_foreign_notify(tmp_path: Path) -> None:
    config = tmp_path / "config.toml"
    config.write_text('notify = ["/usr/bin/their-own-notifier"]\n', encoding="utf-8")

    result = codex.install(config, notify_script_path=_NOTIFY, mcp_env=_ENV)

    assert result.notify_conflict is True
    assert result.notify_written is False
    # their notify is left intact — we did NOT overwrite it.
    assert _read(config)["notify"] == ["/usr/bin/their-own-notifier"]
    # but the MCP server is still registered alongside it.
    assert _read(config)["mcp_servers"]["memory-universe"]["command"] == codex.DEFAULT_MCP_COMMAND


def test_install_preserves_other_mcp_servers_and_keys(tmp_path: Path) -> None:
    config = tmp_path / "config.toml"
    config.write_text(
        'approval_policy = "never"\n\n'
        "[mcp_servers.openaiDeveloperDocs]\n"
        'url = "https://developers.openai.com/mcp"\n\n'
        "[mcp_servers.other-tool]\n"
        'command = "other-mcp"\n'
        'args = ["--x"]\n',
        encoding="utf-8",
    )

    result = codex.install(config, notify_script_path=_NOTIFY, mcp_env=_ENV)

    assert result.mcp_servers_preserved == 2  # openaiDeveloperDocs + other-tool untouched
    doc = _read(config)
    assert doc["approval_policy"] == "never"
    assert doc["mcp_servers"]["openaiDeveloperDocs"]["url"] == "https://developers.openai.com/mcp"
    assert doc["mcp_servers"]["other-tool"] == {"command": "other-mcp", "args": ["--x"]}
    assert doc["mcp_servers"]["memory-universe"]["command"] == codex.DEFAULT_MCP_COMMAND


def test_install_no_mcp_writes_only_notify(tmp_path: Path) -> None:
    config = tmp_path / "config.toml"
    result = codex.install(config, notify_script_path=_NOTIFY, with_mcp=False)

    assert result.mcp_server_registered is False
    doc = _read(config)
    assert doc["notify"] == [str(_NOTIFY)]
    assert "mcp_servers" not in doc


def test_uninstall_removes_only_managed_notify_and_server(tmp_path: Path) -> None:
    config = tmp_path / "config.toml"
    config.write_text(
        "[mcp_servers.other-tool]\n" 'command = "other-mcp"\n', encoding="utf-8"
    )
    codex.install(config, notify_script_path=_NOTIFY, mcp_env=_ENV)  # adds ours alongside theirs

    result = codex.uninstall(config, notify_script_path=_NOTIFY)

    assert result.notify_removed is True
    assert result.mcp_server_removed is True
    assert result.mcp_servers_preserved == 1  # other-tool survives
    doc = _read(config)
    assert "notify" not in doc
    assert "memory-universe" not in doc["mcp_servers"]
    assert doc["mcp_servers"]["other-tool"]["command"] == "other-mcp"


def test_uninstall_leaves_a_foreign_notify_intact(tmp_path: Path) -> None:
    config = tmp_path / "config.toml"
    config.write_text('notify = ["/usr/bin/their-own-notifier"]\n', encoding="utf-8")

    result = codex.uninstall(config, notify_script_path=_NOTIFY)

    assert result.notify_removed is False
    assert result.notify_foreign_left is True
    assert _read(config)["notify"] == ["/usr/bin/their-own-notifier"]


def test_uninstall_missing_file_is_safe_no_op(tmp_path: Path) -> None:
    config = tmp_path / "does_not_exist.toml"
    result = codex.uninstall(config, notify_script_path=_NOTIFY)
    assert not config.exists()
    assert result.backup_path is None
    assert result.notify_removed is False
    assert result.mcp_server_removed is False


def test_install_uninstall_round_trips_to_empty_mcp_table(tmp_path: Path) -> None:
    config = tmp_path / "config.toml"
    codex.install(config, notify_script_path=_NOTIFY, mcp_env=_ENV)
    codex.uninstall(config, notify_script_path=_NOTIFY)
    doc = _read(config)
    assert "notify" not in doc
    assert "mcp_servers" not in doc  # emptied table pruned clean


def test_post_install_guidance_names_both_channels(tmp_path: Path) -> None:
    config = tmp_path / "config.toml"
    text = codex.post_install_guidance(config_path=config, with_mcp=True)
    assert str(config) in text
    assert "backfill-codex" in text
    assert "capture-once --host codex" in text
    assert "notify" in text.lower()
