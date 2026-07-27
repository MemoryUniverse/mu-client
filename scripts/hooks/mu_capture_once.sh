#!/usr/bin/env bash
# The ACTUAL command a Claude Code hook invokes (capture-spec.md §3, host-capture-integration-
# devdoc.md §2.1). Claude Code spawns this as a command hook, pipes the hook-event JSON to its
# stdin, and reads stdout back (exit 0 + optional `{"hookSpecificOutput": {...}}` for
# UserPromptSubmit/SessionStart's additionalContext — capture-spec.md never blocks/fails the host
# turn, so this script always exits 0 even if `mu` itself is misconfigured: a broken hook must
# never break the user's Claude Code session).
#
# Install: reference this script (absolute path) from a managed block in ~/.claude/settings.json —
# see mu_capture_once.settings.example.json in this same directory for the exact hook wiring.
set -uo pipefail

# Resolve `mu` the same way this repo's own dev shell would: prefer an active venv's `mu`, else
# whatever `mu` is on PATH (a `pip install -e .`'d checkout, or a future packaged install).
if [ -n "${MU_CLIENT_VENV:-}" ] && [ -x "${MU_CLIENT_VENV}/bin/mu" ]; then
  MU_BIN="${MU_CLIENT_VENV}/bin/mu"
elif command -v mu >/dev/null 2>&1; then
  MU_BIN="$(command -v mu)"
else
  # `mu` is not installed/reachable — degrade to a silent no-op rather than fail the host turn
  # (capture-spec.md §3: "on socket-down: spool ... exit 0; NEVER block/fail host" — the same
  # honest-degrade discipline applies one layer up, at "mu itself is unreachable").
  echo '{}'
  exit 0
fi

"${MU_BIN}" capture-once --host claude_code
exit 0
