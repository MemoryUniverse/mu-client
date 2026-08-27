"""``mu install codex`` / ``mu uninstall codex`` — wire the mu-client capture spine into a real
Codex CLI config (``$CODEX_HOME/config.toml``, default ``~/.codex/config.toml``), per
AGENT-INTEGRATION-AUDIT-AND-PLAN.md §4 Phase 4.

Codex's config is **TOML** (not Claude Code's JSON ``settings.json``), so this is a sibling of
:mod:`mu_client.install.claude_code` with a TOML round-trip (via ``tomlkit``, which preserves the
owner's existing comments/formatting) instead of ``json.dump``. It writes two things codex actually
reads (both verified against codex-cli 0.146.0):

1. **The ``notify`` program hook** — top-level ``notify = ["<mu_codex_notify.sh>"]``. On
   ``agent-turn-complete`` codex spawns this program with the turn's JSON as argv[1]; the shipped
   shim pipes it into ``mu capture-once --host codex`` (the LIVE push path). Codex supports exactly
   ONE ``notify`` program, so if the config already has a DIFFERENT ``notify``, this installer
   **does not clobber it** — it leaves the existing one intact and reports ``notify_conflict=True``
   (the owner then chains both programs themselves). An existing ``notify`` that already IS ours is
   a no-op (idempotent).

2. **The MU MCP server** — ``[mcp_servers.memory-universe]`` with ``command``/``args`` and an
   ``[mcp_servers.memory-universe.env]`` block carrying the RESOLVED store endpoints (so a
   ``mu-mcp`` codex spawns from any directory reaches the real stores — the same self-contained
   pattern :func:`mu_client.install.claude_code.register_mcp_server` writes into ``.mcp.json``).
   Only this ONE named server entry is (re)written; every OTHER ``mcp_servers.*`` entry and every
   other top-level key survives verbatim.

The same three hard requirements as the Claude installer, enforced here (never left to the caller):
**idempotent** (re-running is a no-op for anything already present), **backup-first** (an EXISTING
file is copied to ``<path>.bak`` once before any write), **never clobbers** an unrelated config key,
an unrelated MCP server, or a foreign ``notify`` program.

**Safety.** The CLI defaults ``--config-path`` to ``$CODEX_HOME/config.toml`` but the Phase-4
brief targets a TEST path — pass ``--config-path`` (or set ``CODEX_HOME``) so a validation run never
touches the owner's real ``~/.codex/config.toml``.
"""

from __future__ import annotations

import shutil
from collections.abc import Mapping, MutableMapping
from pathlib import Path

import tomlkit
from pydantic import BaseModel, ConfigDict
from tomlkit import TOMLDocument

__all__ = [
    "DEFAULT_MCP_COMMAND",
    "DEFAULT_MCP_SERVER_NAME",
    "DEFAULT_NOTIFY_SCRIPT",
    "CodexInstallResult",
    "CodexUninstallResult",
    "install",
    "post_install_guidance",
    "uninstall",
]

# scripts/hooks/mu_codex_notify.sh ships from the repo root this package lives under
# (mu-client/src/mu_client/install/codex.py -> mu-client/scripts/hooks/...). A packaged install
# passes an explicit ``notify_script_path`` instead.
DEFAULT_NOTIFY_SCRIPT: Path = (
    Path(__file__).resolve().parents[3] / "scripts" / "hooks" / "mu_codex_notify.sh"
)

# Reuse the Claude installer's server name/command so ONE MU MCP server identity spans both hosts.
DEFAULT_MCP_SERVER_NAME = "memory-universe"
DEFAULT_MCP_COMMAND = "mu-mcp"

_NOTIFY_KEY = "notify"
_MCP_TABLE = "mcp_servers"


class CodexInstallResult(BaseModel, frozen=True):
    """What ``install()`` did, for the CLI to render and for tests to assert on."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    config_path: Path
    backup_path: Path | None  # None only when config_path did not exist before this call
    notify_script_path: Path
    notify_written: bool  # a NEW/updated managed ``notify`` was written this call
    notify_already_present: bool  # ``notify`` already WAS ours (idempotent no-op)
    notify_conflict: bool  # a DIFFERENT ``notify`` existed and was LEFT intact (never clobbered)
    mcp_server_registered: bool  # the memory-universe MCP server entry was (re)written
    server_name: str
    endpoint_vars_written: int  # count of MU_* env vars baked into the server's env block
    mcp_servers_preserved: int  # count of OTHER mcp_servers.* entries left intact


class CodexUninstallResult(BaseModel, frozen=True):
    """What ``uninstall()`` did."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    config_path: Path
    backup_path: Path | None
    notify_removed: bool  # our managed ``notify`` was found + removed
    notify_foreign_left: bool  # a NON-managed ``notify`` existed and was LEFT intact
    mcp_server_removed: bool
    mcp_servers_preserved: int


def _backup(config_path: Path) -> Path | None:
    if not config_path.exists():
        return None
    backup_path = config_path.with_name(config_path.name + ".bak")
    shutil.copy2(config_path, backup_path)
    return backup_path


def _load_doc(config_path: Path) -> TOMLDocument:
    if not config_path.exists():
        return tomlkit.document()
    raw = config_path.read_text(encoding="utf-8")
    if not raw.strip():
        return tomlkit.document()
    return tomlkit.parse(raw)


def _write_doc(config_path: Path, doc: TOMLDocument) -> None:
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(tomlkit.dumps(doc), encoding="utf-8")


def _existing_notify(doc: TOMLDocument) -> list[str] | None:
    value = doc.get(_NOTIFY_KEY)
    if value is None:
        return None
    if isinstance(value, list):
        return [str(v) for v in value]
    return [str(value)]


def install(
    config_path: Path,
    *,
    notify_script_path: Path = DEFAULT_NOTIFY_SCRIPT,
    with_mcp: bool = True,
    mcp_command: str = DEFAULT_MCP_COMMAND,
    mcp_args: tuple[str, ...] = (),
    mcp_env: Mapping[str, str] | None = None,
    server_name: str = DEFAULT_MCP_SERVER_NAME,
) -> CodexInstallResult:
    """Idempotently wire the ``notify`` hook (+ optionally the MU MCP server) into ``config_path``.
    Backs up an EXISTING file first (``.bak``, once), never clobbers a foreign ``notify`` or an
    unrelated ``mcp_servers.*`` entry / top-level key."""
    backup_path = _backup(config_path)
    doc = _load_doc(config_path)

    managed_notify = [str(notify_script_path)]
    existing = _existing_notify(doc)
    notify_written = False
    notify_already_present = False
    notify_conflict = False
    if existing is None:
        doc[_NOTIFY_KEY] = managed_notify
        notify_written = True
    elif existing == managed_notify:
        notify_already_present = True  # idempotent — leave verbatim
    else:
        notify_conflict = True  # a foreign notify — never clobber it

    endpoint_vars_written = 0
    mcp_registered = False
    mcp_preserved = 0
    if with_mcp:
        servers = doc.get(_MCP_TABLE)
        if servers is None:
            servers = tomlkit.table(is_super_table=True)
            doc[_MCP_TABLE] = servers
        if not _is_table_like(servers):
            raise ValueError(
                f"{config_path}: {_MCP_TABLE} is not a table "
                f"(got {type(servers).__name__}) — refusing to clobber"
            )
        mcp_preserved = sum(1 for name in servers if name != server_name)
        entry = tomlkit.table()
        entry["command"] = mcp_command
        entry["args"] = list(mcp_args)
        env_map = dict(mcp_env or {})
        if env_map:
            env_table = tomlkit.table()
            for key, value in env_map.items():
                env_table[key] = value
            entry["env"] = env_table
        servers[server_name] = entry
        endpoint_vars_written = len(env_map)
        mcp_registered = True

    _write_doc(config_path, doc)
    return CodexInstallResult(
        config_path=config_path,
        backup_path=backup_path,
        notify_script_path=notify_script_path,
        notify_written=notify_written,
        notify_already_present=notify_already_present,
        notify_conflict=notify_conflict,
        mcp_server_registered=mcp_registered,
        server_name=server_name,
        endpoint_vars_written=endpoint_vars_written,
        mcp_servers_preserved=mcp_preserved,
    )


def uninstall(
    config_path: Path,
    *,
    notify_script_path: Path = DEFAULT_NOTIFY_SCRIPT,
    server_name: str = DEFAULT_MCP_SERVER_NAME,
) -> CodexUninstallResult:
    """Remove ONLY the entries ``install()`` would add: our managed ``notify`` (left intact if the
    current ``notify`` is a foreign one) and our ``mcp_servers.<server_name>`` entry — every other
    key/server survives. An emptied ``mcp_servers`` table is pruned."""
    if not config_path.exists():
        return CodexUninstallResult(
            config_path=config_path,
            backup_path=None,
            notify_removed=False,
            notify_foreign_left=False,
            mcp_server_removed=False,
            mcp_servers_preserved=0,
        )

    backup_path = _backup(config_path)
    doc = _load_doc(config_path)

    managed_notify = [str(notify_script_path)]
    existing = _existing_notify(doc)
    notify_removed = False
    notify_foreign_left = False
    if existing is not None:
        if existing == managed_notify:
            del doc[_NOTIFY_KEY]
            notify_removed = True
        else:
            notify_foreign_left = True

    mcp_removed = False
    mcp_preserved = 0
    servers = doc.get(_MCP_TABLE)
    if isinstance(servers, MutableMapping):
        if server_name in servers:
            del servers[server_name]
            mcp_removed = True
        mcp_preserved = len(servers)
        if mcp_preserved == 0:
            del doc[_MCP_TABLE]

    _write_doc(config_path, doc)
    return CodexUninstallResult(
        config_path=config_path,
        backup_path=backup_path,
        notify_removed=notify_removed,
        notify_foreign_left=notify_foreign_left,
        mcp_server_removed=mcp_removed,
        mcp_servers_preserved=mcp_preserved,
    )


def _is_table_like(value: object) -> bool:
    """A tomlkit table or an inline table both behave as a Mapping for our merge — ``dict`` covers
    the parsed-back shape too (a plain dict never appears from tomlkit.parse but keeps callers
    honest)."""
    return isinstance(value, Mapping)


def post_install_guidance(
    *,
    config_path: Path,
    with_mcp: bool,
    server_name: str = DEFAULT_MCP_SERVER_NAME,
) -> str:
    """The guidance ``mu install codex`` prints after a successful install — how the notify hook and
    the rollout tailer complement each other, and how to point codex at a test config."""
    mcp_line = (
        f"  MCP server '{server_name}' registered under [mcp_servers.{server_name}]\n"
        if with_mcp
        else "  (MCP server registration skipped: --no-mcp)\n"
    )
    return (
        "\n"
        "Memory Universe — codex capture installed.\n"
        "\n"
        f"  Config written to : {config_path}\n"
        f"  notify hook       : agent-turn-complete -> mu_codex_notify.sh -> "
        "mu capture-once --host codex\n"
        f"{mcp_line}"
        "\n"
        "TWO capture channels are now live:\n"
        "  1. LIVE  — the notify hook pushes each turn's final answer as it completes.\n"
        "  2. FULL  — `mu backfill-codex --rollout <~/.codex/sessions/.../rollout-*.jsonl>`\n"
        "             tails the complete rollout (user prompts, assistant turns, tool calls) into\n"
        "             the SAME outbox; run it (or a daemon sweep) to capture the whole session.\n"
        "\n"
        "Then `mu flush` (or a running `mu daemon run`) drives the outbox into the real stores.\n"
        "If codex already had its own `notify` program, it was LEFT intact — chain both yourself.\n"
    )
