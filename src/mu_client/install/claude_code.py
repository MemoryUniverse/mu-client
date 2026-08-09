"""``mu install claude-code`` / ``mu uninstall claude-code`` — the installer that flips the
dogfood-only capture path (capture-spec.md §5.1) into something live in the owner's REAL Claude
Code, per AGENT-INTEGRATION-AUDIT-AND-PLAN.md §4 Phase 0 / §5 first-milestone.

Writes the SAME managed hook block ``scripts/hooks/mu_capture_once.settings.example.json``
already documents as the target shape into a target ``settings.json`` — one matcher entry per
Claude Code hook event, each pointing at ``scripts/hooks/mu_capture_once.sh`` (absolute path).

Three hard requirements from the plan (all enforced here, not left to the caller):

1. **Idempotent.** Re-running ``install()`` against a settings file that already carries the
   managed block is a no-op for every event that already has it — never a duplicate matcher entry.
   Detected by exact ``(matcher, command)`` match, not by a sentinel comment (Claude Code's own
   settings schema has no comment field once round-tripped through ``json.dump``).
2. **Backup-first.** Any file this module is about to MODIFY is copied to ``<path>.bak`` before
   the write (a plain ``shutil.copy2``, once per call — re-running ``install()`` twice overwrites
   the SAME ``.bak`` with the pre-second-run state, which is the correct "last backup before last
   change" semantics, not a growing chain). A settings file that does not exist yet has nothing to
   back up — created fresh, no ``.bak`` written.
3. **Never clobbers unrelated hooks.** Every existing top-level key, every existing hook event's
   existing matcher entries, and every existing entry's own ``hooks`` list are preserved verbatim.
   The managed entries for a given event are APPENDED to that event's existing list (or the list
   is created if the event has none yet) — this module never removes or rewrites an entry it did
   not itself add in a prior ``install()`` call.

``uninstall()`` removes ONLY the exact managed entries this module would itself add (verbatim
``(matcher="*", hooks=[{"type":"command","command":<script>}])`` match) — any hand-added or
third-party entry for the SAME event survives untouched, and an event/the whole ``hooks`` key is
pruned only once it is left empty by the removal.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict

__all__ = [
    "CLAUDE_CODE_HOOK_EVENTS",
    "DEFAULT_HOOK_SCRIPT",
    "InstallResult",
    "UninstallResult",
    "install",
    "uninstall",
]

# The managed hook events, verbatim from scripts/hooks/mu_capture_once.settings.example.json
# (capture-spec.md §5.1: "every event funnels through the SAME script").
CLAUDE_CODE_HOOK_EVENTS: tuple[str, ...] = (
    "UserPromptSubmit",
    "PostToolUse",
    "PostToolUseFailure",
    "SubagentStop",
    "Stop",
    "SessionStart",
    "SessionEnd",
    "PreCompact",
)

# scripts/hooks/mu_capture_once.sh lives at the repo root this package ships from
# (mu-client/src/mu_client/install/claude_code.py -> mu-client/scripts/hooks/...). This is a
# dev/source-checkout default; a caller targeting a packaged install passes an explicit
# ``hook_script_path`` instead (both the CLI subcommand and install()/uninstall() accept one).
DEFAULT_HOOK_SCRIPT: Path = (
    Path(__file__).resolve().parents[3] / "scripts" / "hooks" / "mu_capture_once.sh"
)

_MATCHER = "*"


class InstallResult(BaseModel, frozen=True):
    """What ``install()`` did, for the CLI to render and for tests to assert on."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    settings_path: Path
    hook_script_path: Path
    backup_path: Path | None  # None only when settings_path did not exist before this call
    events_added: tuple[str, ...]  # events that gained a NEW managed entry this call
    events_already_present: tuple[str, ...]  # events whose managed entry already existed
    unrelated_hooks_preserved: int  # count of pre-existing, non-managed hook entries left intact


class UninstallResult(BaseModel, frozen=True):
    """What ``uninstall()`` did."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    settings_path: Path
    backup_path: Path | None
    events_removed: tuple[str, ...]  # events whose managed entry was found + removed
    events_not_present: tuple[str, ...]  # events that had no managed entry to remove
    unrelated_hooks_preserved: int


def _managed_entry(hook_script_path: Path) -> dict[str, Any]:
    return {
        "matcher": _MATCHER,
        "hooks": [{"type": "command", "command": str(hook_script_path)}],
    }


def _load_settings(settings_path: Path) -> dict[str, Any]:
    if not settings_path.exists():
        return {}
    raw = settings_path.read_text(encoding="utf-8")
    if not raw.strip():
        return {}
    loaded = json.loads(raw)
    if not isinstance(loaded, dict):
        raise ValueError(
            f"{settings_path}: top-level JSON must be an object, got {type(loaded).__name__}"
        )
    return loaded


def _backup(settings_path: Path) -> Path | None:
    if not settings_path.exists():
        return None
    backup_path = settings_path.with_name(settings_path.name + ".bak")
    shutil.copy2(settings_path, backup_path)
    return backup_path


def _write_settings(settings_path: Path, settings: dict[str, Any]) -> None:
    settings_path.parent.mkdir(parents=True, exist_ok=True)
    settings_path.write_text(json.dumps(settings, indent=2) + "\n", encoding="utf-8")


def install(
    settings_path: Path,
    *,
    hook_script_path: Path = DEFAULT_HOOK_SCRIPT,
    events: tuple[str, ...] = CLAUDE_CODE_HOOK_EVENTS,
) -> InstallResult:
    """Idempotently write the managed hook block into ``settings_path``. Backs up an EXISTING
    file first (``.bak``, once), then appends one managed matcher entry per event in ``events``
    that does not already carry one — every other key/event/entry in the file survives verbatim.
    """
    backup_path = _backup(settings_path)
    settings = _load_settings(settings_path)
    hooks_block: dict[str, Any] = settings.setdefault("hooks", {})

    entry = _managed_entry(hook_script_path)
    added: list[str] = []
    already: list[str] = []
    for event in events:
        event_entries: list[Any] = hooks_block.setdefault(event, [])
        if not isinstance(event_entries, list):
            raise ValueError(
                f"{settings_path}: hooks.{event} is not a list "
                f"(got {type(event_entries).__name__}) — refusing to clobber"
            )
        if entry in event_entries:
            already.append(event)
            continue
        event_entries.append(entry)
        added.append(event)

    total_entries = sum(len(v) for v in hooks_block.values() if isinstance(v, list))
    managed_entries = len(added) + len(already)  # exactly one managed entry per event, idempotent
    unrelated = total_entries - managed_entries

    _write_settings(settings_path, settings)
    return InstallResult(
        settings_path=settings_path,
        hook_script_path=hook_script_path,
        backup_path=backup_path,
        events_added=tuple(added),
        events_already_present=tuple(already),
        unrelated_hooks_preserved=unrelated,
    )


def uninstall(
    settings_path: Path,
    *,
    hook_script_path: Path = DEFAULT_HOOK_SCRIPT,
    events: tuple[str, ...] = CLAUDE_CODE_HOOK_EVENTS,
) -> UninstallResult:
    """Remove ONLY the exact managed entries ``install()`` would add for ``events`` — any other
    entry (hand-added, third-party, or a managed entry for an event NOT in ``events``) is left
    untouched. An event key emptied by the removal is pruned; ``hooks`` itself is pruned if it
    ends up empty too."""
    if not settings_path.exists():
        return UninstallResult(
            settings_path=settings_path,
            backup_path=None,
            events_removed=(),
            events_not_present=tuple(events),
            unrelated_hooks_preserved=0,
        )

    backup_path = _backup(settings_path)
    settings = _load_settings(settings_path)
    hooks_block: dict[str, Any] = settings.get("hooks", {})

    entry = _managed_entry(hook_script_path)
    removed: list[str] = []
    not_present: list[str] = []
    for event in events:
        event_entries = hooks_block.get(event)
        if not isinstance(event_entries, list) or entry not in event_entries:
            not_present.append(event)
            continue
        event_entries.remove(entry)
        removed.append(event)
        if not event_entries:
            del hooks_block[event]

    unrelated = sum(
        1
        for event_entries in hooks_block.values()
        if isinstance(event_entries, list)
        for _ in event_entries
    )
    if not hooks_block and "hooks" in settings:
        del settings["hooks"]

    _write_settings(settings_path, settings)
    return UninstallResult(
        settings_path=settings_path,
        backup_path=backup_path,
        events_removed=tuple(removed),
        events_not_present=tuple(not_present),
        unrelated_hooks_preserved=unrelated,
    )
