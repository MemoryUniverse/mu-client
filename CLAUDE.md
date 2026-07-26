# mu-client

**Open (Apache-2.0). Depends on `mu-core` only.** The local daemon. See `../CLAUDE.md` for the
project-wide rules — they bind here.

## Contains

- Host capture / inject / bridge adapters — Claude Code + Codex **hooks** (maximal reasoning capture:
  Codex reasoning-tail + `ClaudeCodeTranscriptTailer`; `Stop → ASSISTANT_MSG`), the tiny **Go
  hook-client** binary, the IPC server, the injector.
- Local store adapters (Valkey / Qdrant / **LadybugDB embedded graph**) and the on-device engine
  host — runs `mu-engine` from `mu-core` for full on-device memory.
- Client durable execution = **InlineRunner + SQLite-WAL outbox** (NO Temporal on the laptop).
- Warm-in-process local models (startup-loaded singletons; the MemOS `HFSingleton` pattern) via the
  `mu-core` model router.

## Rules specific to this repo

- **FULL-LOCAL must be a complete, good memory system** with no server required — capture, all
  tiers, promotion/demotion, conflict, good recall, persona, local agent-to-agent rooms.
- Never import `mu-server`. All server interaction is via `mu-sdk` over the wire contract.
- Metering/observability still emit content-free usage locally where relevant.
- The hook-client is Go; everything else is Python (per ADR 0021).
