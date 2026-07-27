# mu-client

Part of **Memory Universe**. See `CLAUDE.md` in this repo and `../CLAUDE.md` for the rules.
Design authority: `../docs/superpowers/design/` (`daemon-app-skeleton-spec.md`, `capture-spec.md`,
`host-capture-integration-devdoc.md`).

## Status

**Foundation + daemonless + capture/outbox/inject/daemon stage built.** Scope note: this stage
wires Claude Code capture only (Codex/Desktop parsers are a later stage) and the device-sync /
cross-plane (Centrifugo) parts of `daemon-app-skeleton-spec.md` §8/§9 are SHARED-plane features
out of scope for a `mu-server`-free repo (`client-has-no-server`).

## Public surface (this stage)

### Foundation (host + config)
- `mu_client.config.ClientSettings` — the one env boundary (`MU_` prefix, shared with mu-core's
  `.env`/`.env.test`); nested `capture`/`outbox`/`inject`/`ipc` subtrees for this stage.
- `mu_client.host.LocalMemoryHost` / `mu_client.host.daemonless_host()` — hosts mu-local's
  `LocalMemory` with a clean async lifecycle; `add`/`recall`/`search` verb proxies.

### Capture (`mu_client.capture`)
- `capture.model` — `RawActivity`/`ActivityKind`/`HostKind` (capture-spec.md §4.1, verbatim shape).
- `capture.parsers` — `HostSchemaParser`/`ParserRegistry`/`ClaudeCodeParserV1`: the real
  `hook_event_name` → `RawActivity` mapping (`UserPromptSubmit`, `PostToolUse(Failure)`,
  `SubagentStop`, `Stop` — F2 KEEPs the final answer, `SessionStart`/`SessionEnd`/`PreCompact`).
  Fails loud (`CaptureSchemaDriftError`) on an unrecognized event, never guesses.
- `capture.hook.capture_once()` — the ONE hook entrypoint: daemon fast-path if the IPC socket is
  reachable, else a DIRECT SQLite-WAL outbox append (daemonless); on outbox-unreachable, spools to
  `~/.memory-universe/spool/*.json` and always exits 0 (never blocks/fails the host turn).
- `scripts/hooks/mu_capture_once.sh` + `mu_capture_once.settings.example.json` — the actual
  script + `~/.claude/settings.json` managed-block wiring a real Claude Code hook invokes.

### Outbox (`mu_client.outbox`)
- `outbox.sqlite_outbox.SqliteOutbox` — REAL WAL-mode SQLite (`synchronous=FULL`,
  `UNIQUE(activity_id)` idempotent redelivery); `append`/`drain`/`ack`/`retry_later`/
  `dead_letter`/`redrive_dead`/`quarantine_raw`/`undelivered_count`/`outbox_depth`. Recovers any
  `INFLIGHT` row back to `PENDING` on `open()` (crash between drain and ack never loses/dupes).

### Workers + inject
- `workers.ingest_client.InProcessLocalIngest` — outbox record → `LocalMemory.add`; a control kind
  or a filtered-slice `text=None` raises `ExtractionSkippedError` (ack, never a `MemoryItem`).
- `workers.pool.OutboxWorker` / `WorkerPool` — drain → ingest → ack, with retry/dead-letter.
- `inject.recall_bridge.RecallInjectBridge` — recalls from mu-local and renders the
  `additionalContext` payload (fresh/stale/cold staleness contract; F4 10k-char budget with
  named file-spill degrade, never a silent truncate).

### Daemon (`mu_client.daemon`)
- `daemon.ipc.IpcServer` — unix-socket (`SO_PEERCRED`-checked) front door; `capture`/`recall`/
  `healthz` routes over newline-delimited JSON (an HTTP-route-table-compatible MVP transport, see
  module docstring for the recorded deviation).
- `daemon.app.LocalDaemon` — the composition root: hosts the engine, the outbox, the worker pool,
  and the IPC server under one `asyncio.TaskGroup`; ordered shutdown (stop inbound → drain outbox
  → release engine).

### CLI (`mu` console script, `mu_client.cli`)
- `mu add|recall|search` — daemonless one-shot (unchanged from the foundation stage).
- `mu capture-once --host claude_code` — the hook entrypoint (reads stdin JSON).
- `mu flush` — drains the spool + outbox into mu-local; no daemon required.
- `mu daemon run` — the resident daemon (graceful `SIGINT`/`SIGTERM` shutdown).

## Dev

```bash
uv sync --extra dev
uv run ruff check . && uv run ruff format --check .
uv run mypy --strict src tests
uv run lint-imports
uv run pytest -m unit
uv run pytest -m integration   # needs the mu-dev-* containers up (mu-core/docker-compose.dev.yml)
```
