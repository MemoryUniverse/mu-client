#!/usr/bin/env bash
# Codex `notify` program shim (AGENT-INTEGRATION-AUDIT-AND-PLAN.md §4 Phase 4).
#
# Codex invokes its configured `notify` program with the turn's event as a SINGLE argv:
#     mu_codex_notify.sh '{"type":"agent-turn-complete","thread-id":..,"last-assistant-message":..}'
# (verified against codex-cli 0.146.0). We pipe that JSON straight into `mu capture-once --host
# codex`, which appends it to the durable outbox (daemon fast-path if a `mu daemon run` is up, else
# a direct SQLite-WAL append). Capture NEVER blocks or fails the codex turn — we swallow every
# error and always exit 0, exactly like the Claude Code hook shim.
#
# `mu` must be on PATH (the console-script from mu-client's pyproject). Override with MU_BIN.
set -u
MU_BIN="${MU_BIN:-mu}"
PAYLOAD="${1:-}"
if [ -n "$PAYLOAD" ]; then
  printf '%s' "$PAYLOAD" | "$MU_BIN" capture-once --host codex >/dev/null 2>&1 || true
fi
exit 0
