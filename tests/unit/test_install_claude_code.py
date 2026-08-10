"""Real-file unit tests for ``mu install|uninstall claude-code`` (no mocks — plain temp files,
per the Phase 0 acceptance gate in AGENT-INTEGRATION-AUDIT-AND-PLAN.md §4/§5): idempotent
re-install, backup-first, and never-clobber-unrelated-hooks."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mu_client.install import claude_code

pytestmark = pytest.mark.unit

_SCRIPT = Path("/opt/mu/scripts/hooks/mu_capture_once.sh")


def _managed_entry() -> dict[str, object]:
    return {"matcher": "*", "hooks": [{"type": "command", "command": str(_SCRIPT)}]}


def test_install_on_a_fresh_missing_file_writes_every_managed_event(tmp_path: Path) -> None:
    settings_path = tmp_path / "settings.json"

    result = claude_code.install(settings_path, hook_script_path=_SCRIPT)

    assert result.backup_path is None  # nothing existed to back up
    assert set(result.events_added) == set(claude_code.CLAUDE_CODE_HOOK_EVENTS)
    assert result.events_already_present == ()
    assert settings_path.exists()

    written = json.loads(settings_path.read_text())
    for event in claude_code.CLAUDE_CODE_HOOK_EVENTS:
        assert written["hooks"][event] == [_managed_entry()]


def test_install_is_idempotent_no_duplicate_block(tmp_path: Path) -> None:
    settings_path = tmp_path / "settings.json"
    claude_code.install(settings_path, hook_script_path=_SCRIPT)
    before = json.loads(settings_path.read_text())

    result = claude_code.install(settings_path, hook_script_path=_SCRIPT)

    after = json.loads(settings_path.read_text())
    assert after == before  # byte-for-byte identical content on re-run
    assert result.events_added == ()
    assert set(result.events_already_present) == set(claude_code.CLAUDE_CODE_HOOK_EVENTS)
    for event in claude_code.CLAUDE_CODE_HOOK_EVENTS:
        assert after["hooks"][event] == [_managed_entry()]  # exactly one entry, never two


def test_install_backs_up_an_existing_file_before_writing(tmp_path: Path) -> None:
    settings_path = tmp_path / "settings.json"
    original = {
        "hooks": {
            "Stop": [{"matcher": "foo", "hooks": [{"type": "command", "command": "echo hi"}]}]
        }
    }
    settings_path.write_text(json.dumps(original))

    result = claude_code.install(settings_path, hook_script_path=_SCRIPT)

    assert result.backup_path is not None
    assert result.backup_path == settings_path.with_name("settings.json.bak")
    assert json.loads(result.backup_path.read_text()) == original


def test_install_preserves_a_preexisting_unrelated_hook(tmp_path: Path) -> None:
    settings_path = tmp_path / "settings.json"
    original = {
        "some_other_top_level_key": "untouched",
        "hooks": {
            "Stop": [
                {
                    "matcher": "some-other-matcher",
                    "hooks": [{"type": "command", "command": "/usr/bin/some-other-tool"}],
                }
            ]
        },
    }
    settings_path.write_text(json.dumps(original))

    result = claude_code.install(settings_path, hook_script_path=_SCRIPT)

    written = json.loads(settings_path.read_text())
    assert written["some_other_top_level_key"] == "untouched"
    # The pre-existing unrelated entry survives, and OUR entry is appended alongside it.
    assert original["hooks"]["Stop"][0] in written["hooks"]["Stop"]
    assert _managed_entry() in written["hooks"]["Stop"]
    assert len(written["hooks"]["Stop"]) == 2
    assert "Stop" in result.events_added
    assert result.unrelated_hooks_preserved == 1


def test_uninstall_removes_only_the_managed_entries(tmp_path: Path) -> None:
    settings_path = tmp_path / "settings.json"
    claude_code.install(settings_path, hook_script_path=_SCRIPT)

    result = claude_code.uninstall(settings_path, hook_script_path=_SCRIPT)

    assert set(result.events_removed) == set(claude_code.CLAUDE_CODE_HOOK_EVENTS)
    written = json.loads(settings_path.read_text())
    assert "hooks" not in written  # every event was ours; the whole block is pruned clean


def test_uninstall_preserves_unrelated_entries_and_prunes_only_the_managed_one(
    tmp_path: Path,
) -> None:
    settings_path = tmp_path / "settings.json"
    original = {
        "hooks": {
            "Stop": [
                {
                    "matcher": "some-other-matcher",
                    "hooks": [{"type": "command", "command": "/usr/bin/some-other-tool"}],
                }
            ]
        }
    }
    settings_path.write_text(json.dumps(original))
    claude_code.install(settings_path, hook_script_path=_SCRIPT)  # appends ours alongside it

    result = claude_code.uninstall(settings_path, hook_script_path=_SCRIPT)

    written = json.loads(settings_path.read_text())
    assert written["hooks"]["Stop"] == [original["hooks"]["Stop"][0]]  # only theirs remains
    assert "Stop" in result.events_removed
    assert result.unrelated_hooks_preserved == 1


def test_uninstall_on_a_missing_file_is_a_safe_no_op(tmp_path: Path) -> None:
    settings_path = tmp_path / "does_not_exist.json"

    result = claude_code.uninstall(settings_path, hook_script_path=_SCRIPT)

    assert not settings_path.exists()
    assert result.backup_path is None
    assert result.events_removed == ()
    assert set(result.events_not_present) == set(claude_code.CLAUDE_CODE_HOOK_EVENTS)


def test_uninstall_then_reinstall_round_trips_cleanly(tmp_path: Path) -> None:
    settings_path = tmp_path / "settings.json"
    claude_code.install(settings_path, hook_script_path=_SCRIPT)
    claude_code.uninstall(settings_path, hook_script_path=_SCRIPT)

    result = claude_code.install(settings_path, hook_script_path=_SCRIPT)

    assert set(result.events_added) == set(claude_code.CLAUDE_CODE_HOOK_EVENTS)
    written = json.loads(settings_path.read_text())
    for event in claude_code.CLAUDE_CODE_HOOK_EVENTS:
        assert written["hooks"][event] == [_managed_entry()]


# ── gap A part 2: self-contained MCP server registration ──────────────────────────────────────


def test_register_mcp_server_writes_self_contained_env_block(tmp_path: Path) -> None:
    mcp_path = tmp_path / ".mcp.json"
    env = {"MU_STORAGE__CACHE__HOST": "127.0.0.1", "MU_STORAGE__CACHE__PORT": "16379"}

    result = claude_code.register_mcp_server(mcp_path, env=env)

    assert result.backup_path is None  # nothing existed
    assert result.server_name == claude_code.DEFAULT_MCP_SERVER_NAME
    assert result.endpoint_vars_written == 2
    doc = json.loads(mcp_path.read_text())
    server = doc["mcpServers"][claude_code.DEFAULT_MCP_SERVER_NAME]
    assert server["type"] == "stdio"
    assert server["command"] == claude_code.DEFAULT_MCP_COMMAND
    assert server["env"] == env  # endpoints carried in the env block, not a cwd .env


def test_register_mcp_server_preserves_other_servers_and_keys(tmp_path: Path) -> None:
    mcp_path = tmp_path / ".mcp.json"
    mcp_path.write_text(
        json.dumps(
            {
                "someTopKey": {"keep": True},
                "mcpServers": {"other": {"type": "stdio", "command": "other-mcp"}},
            }
        ),
        encoding="utf-8",
    )

    result = claude_code.register_mcp_server(mcp_path, env={"MU_X": "1"})

    assert result.backup_path == mcp_path.with_name(".mcp.json.bak")
    doc = json.loads(mcp_path.read_text())
    assert doc["someTopKey"] == {"keep": True}  # unrelated top-level key survives
    assert doc["mcpServers"]["other"] == {"type": "stdio", "command": "other-mcp"}  # other server
    assert doc["mcpServers"][claude_code.DEFAULT_MCP_SERVER_NAME]["env"] == {"MU_X": "1"}


def test_register_mcp_server_is_idempotent_overwrites_own_entry(tmp_path: Path) -> None:
    mcp_path = tmp_path / ".mcp.json"
    claude_code.register_mcp_server(mcp_path, env={"MU_X": "1"})
    claude_code.register_mcp_server(mcp_path, env={"MU_X": "2"})

    doc = json.loads(mcp_path.read_text())
    servers = doc["mcpServers"]
    assert servers[claude_code.DEFAULT_MCP_SERVER_NAME]["env"] == {"MU_X": "2"}
    # exactly one memory-universe entry — no duplication
    assert list(servers).count(claude_code.DEFAULT_MCP_SERVER_NAME) == 1


# ── gap B: post-install trust guidance ────────────────────────────────────────────────────────


def test_post_install_guidance_names_trust_and_headless_steps(tmp_path: Path) -> None:
    settings_path = tmp_path / "settings.json"
    mcp_path = tmp_path / ".mcp.json"

    text = claude_code.post_install_guidance(settings_path=settings_path, mcp_json_path=mcp_path)

    assert str(settings_path) in text
    assert str(mcp_path) in text
    assert "/hooks" in text  # how to review trusted hooks interactively
    assert "--settings" in text  # the headless-testing escape hatch
    assert "trust" in text.lower() or "approve" in text.lower()
