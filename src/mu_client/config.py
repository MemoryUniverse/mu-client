"""``ClientSettings`` — the ONE mu-client env boundary (DEV-STANDARDS rule 3: no hardcoded
literals; everything flows from a central config).

Deliberately reuses mu-core's shapes rather than re-inventing them:

* ``storage`` is ``mu_contracts.config.settings.StorageSettings`` VERBATIM (the same class the
  ``mu-dev-*`` container stack's endpoints already validate against, ``.env.test``) — "store
  endpoints (reuse mu-core's ``StorageSettings`` shape)" from the brief, read literally: it is the
  identical class, not a re-shaped copy. mu-client shares mu-core's ``MU_`` env-var namespace (same
  prefix, same nested delimiter, same ``.env``/``.env.test`` files) so ONE env file configures both
  the engine's connection endpoints *and* mu-client's own subtrees below — no translation layer.
* ``model`` (env: ``MU_MODEL_PROFILE__*`` — NOT ``MU_MODEL__*``, which the ENGINE owns; see
  the field's own comment for the two live-reproduced failures that collision caused) is the
  client's LLM/SLM profile slot. ``mu_local``'s composition
  root (``mu_local/composition.py``) closed the ``StorageSettings.llm=None`` seam on 2026-07-27
  (``mu-core`` commit ``e8fdaeb``): a configured ``mu_local.config.ModelProfileSettings`` now builds
  a REAL ``ModelRouter``. ``LocalMemoryHost.start()`` (``host.py``) maps THIS profile into that
  shape, so it defaults at the real mu-dev-slm sidecar (``127.0.0.1:11435/v1``, an OpenAI-compatible
  Ollama endpoint) and a bare ``ClientSettings()`` wires the daemon straight to the real SLM — no
  code change needed, only env (or the ``model=None`` opt-out for heuristic mode).
* ``daemon_socket_path`` / ``outbox_db_path`` are named EXACTLY as the daemon-app-skeleton-spec's
  ``IpcSettings.socket_path`` / ``OutboxSettings.outbox_path`` (same default paths,
  ``~/.memory-universe/*``) so the daemon stage can promote them into nested ``DaemonSettings``
  subtrees later with no behavioural change — this stage does not read them (no daemon, no outbox
  yet); they exist now so `ClientSettings` is the one place a later stage adds to, never a second
  settings tree.

Backend SELECTION (which adapter binds each role — ``sqlite``/``redis``/``qdrant``/``falkordb``/
``minilm_local``) is a SEPARATE concern owned by ``mu_local.config.StorageSettings`` (the
``{backend, config}`` per-role choice `LocalMemoryHost` passes straight through to
``mu_local.LocalContainer``, unmodified) — see ``host.py``. This class owns only the CLIENT's
env-configurable surface: store *endpoints*, the model profile, and the two forward-declared paths.
"""

from __future__ import annotations

import os
import socket
from collections.abc import Mapping
from functools import lru_cache
from pathlib import Path

from mu_contracts.config.settings import StorageSettings
from pydantic import BaseModel, ConfigDict, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

# ── CWD-INDEPENDENT ENDPOINT RESOLUTION (deployment hardening, gap A) ──────────────────────────
# ``mu-mcp`` / the daemon are spawned by a Claude Code / Codex client from an ARBITRARY project
# directory (wherever the user is working), NOT from this repo's checkout. pydantic-settings'
# ``env_file`` is resolved relative to the process CWD, so a bare ``.env``/``.env.test`` reference
# finds NOTHING when spawned elsewhere — every store endpoint then silently falls back to its
# in-container default (redis ``localhost:6379``, qdrant ``localhost:6333`` …) and the engine
# crashes with a connection error against a port nothing is listening on. The fix is to ALSO read
# from a FIXED absolute location that does not move with the CWD:
#
#   * ``MU_ENV_FILE`` — an explicit absolute env-file path (highest-priority file), and
#   * ``~/.memory-universe/config.env`` — the default user-config home the installer writes.
#
# OS environment variables still beat every file (pydantic default), so the installer-written MCP
# ``env`` block (real ``MU_STORAGE__*`` vars in ``.mcp.json``) is the top-priority, self-contained
# path — see :func:`render_endpoint_env` and ``mu_client.install.claude_code``.

#: Env var naming an explicit absolute env file (an operator/installer override).
MU_ENV_FILE_VAR = "MU_ENV_FILE"

#: The fixed user-config env file the installer writes and a bare ``mu-mcp`` falls back to.
USER_CONFIG_ENV_FILE = Path("~/.memory-universe/config.env")


def _default_device_id() -> str:
    """A stable-enough per-device string for the lease-naming convention (CANONICAL §7.5,
    ``lifecycle-sweep-lease:local:{device_id}:{...}``) with zero required setup — the local
    hostname. Override via ``MU_DEVICE_ID`` for a multi-daemon-on-one-host test rig."""
    return socket.gethostname()


def resolve_env_files(
    *,
    cwd: Path | None = None,
    environ: Mapping[str, str] | None = None,
    user_config: Path | None = None,
) -> tuple[Path, ...]:
    """The env files :func:`get_client_settings` feeds pydantic-settings, in ASCENDING precedence
    (pydantic reads the tuple left-to-right and the LAST-read file wins — verified behaviour).

    Order (lowest → highest priority among *files*; a real OS env var still beats them all):

    1. ``~/.memory-universe/config.env`` — the fixed user-config home (CWD-independent fallback the
       installer writes; the whole point of gap-A: a bare ``mu-mcp`` from any dir finds it here).
    2. ``$MU_ENV_FILE`` — an explicit absolute override, if set.
    3. ``<cwd>/.env`` then ``<cwd>/.env.test`` — the project-local dev files, HIGHEST so a
       developer working inside the repo keeps the old behaviour (``.env.test`` on top).

    Missing files are harmless — pydantic-settings' dotenv source silently skips any path that does
    not exist, so all four are always offered and only the present ones contribute.
    """
    env = os.environ if environ is None else environ
    base = Path.cwd() if cwd is None else cwd
    user_cfg = (USER_CONFIG_ENV_FILE if user_config is None else user_config).expanduser()

    files: list[Path] = [user_cfg]
    explicit = env.get(MU_ENV_FILE_VAR)
    if explicit:
        files.append(Path(explicit).expanduser())
    files.append(base / ".env")
    files.append(base / ".env.test")
    return tuple(files)


def _flatten_env(prefix: str, model: BaseModel, out: dict[str, str]) -> None:
    """Serialise a pydantic model's leaf fields back into flat ``PREFIX__FIELD`` env vars (the
    nested ``MU_`` convention, uppercased). Recurses into nested models; ``SecretStr`` is
    unwrapped; ``None`` fields are skipped. Used to materialise the resolved store endpoints into
    a self-contained MCP ``env`` block."""
    for name in type(model).model_fields:
        value = getattr(model, name)
        key = f"{prefix}__{name.upper()}"
        if isinstance(value, BaseModel):
            _flatten_env(key, value, out)
        elif isinstance(value, SecretStr):
            out[key] = value.get_secret_value()
        elif isinstance(value, bool):
            out[key] = "true" if value else "false"
        elif value is None:
            continue
        else:
            out[key] = str(value)


def render_endpoint_env(settings: ClientSettings) -> dict[str, str]:
    """Flatten the RESOLVED store endpoints (+ model profile) of ``settings`` into a ``MU_*`` env
    mapping suitable for an MCP server's ``env`` block. This is what makes a registered
    ``.mcp.json`` self-contained and CWD-independent: the client inherits the real endpoints from
    the spawn environment, never from a ``.env`` that may not be next to it (gap A)."""
    out: dict[str, str] = {"MU_RUNTIME_MODE": "local"}
    _flatten_env("MU_STORAGE", settings.storage, out)
    if settings.model is not None:
        # MUST match the field's own env alias (see ClientSettings.model): emitting the old
        # ``MU_MODEL`` prefix here is precisely what made every generated .mcp.json / codex
        # config.toml start a server that crashed on EngineSettings construction.
        _flatten_env("MU_MODEL_PROFILE", settings.model, out)
    return out


__all__ = [
    "MU_ENV_FILE_VAR",
    "USER_CONFIG_ENV_FILE",
    "CaptureSettings",
    "ClientSettings",
    "DaemonIpcSettings",
    "InjectSettings",
    "ModelProfileSettings",
    "OutboxSettings",
    "get_client_settings",
    "render_endpoint_env",
    "resolve_env_files",
]


class CaptureSettings(BaseModel):
    """Capture-cluster knobs (capture-spec.md §10, scoped to the ONE host this stage wires).
    ``env: MU_CAPTURE__*``."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    tool_outcome_max_chars: int = 500  # salient-slice truncation budget (capture-spec.md §7.1)
    spool_dir: Path = Path("~/.memory-universe/spool")  # on outbox-unreachable (§2.2)

    # ---- Phase 0B: Claude Code background-thinking transcript backfill (capture-spec.md §6,
    # AGENT-INTEGRATION-AUDIT-AND-PLAN.md §4 Phase 0B). OFF unless explicitly enabled — the tailer
    # reads the session transcript JSONL (reasoning the hook fleet cannot see) and is gated behind
    # THIS flag so a default install captures via hooks only, exactly as before.
    thinking_backfill_enabled: bool = False  # env: MU_CAPTURE__THINKING_BACKFILL_ENABLED
    # Importance bands the tailer stamps on a candidate → the engine's ONE promotion gate
    # (DeterministicPromoteStage: importance >= IngestSettings.importance_promote, default 0.6).
    # A DECISION is stamped ≥ 0.6 so it promotes STM→MTM ("worth keeping"); a FINDING is stamped
    # < 0.6 so it stays STM-only. These are the client's lever on the SAME gate LocalMemory.add
    # already exposes — never a second, competing threshold.
    thinking_decision_importance: float = 0.7
    thinking_finding_importance: float = 0.55

    # ---- Phase 1.5: subagent agent-scoped memory partitions (AGENT-INTEGRATION-AUDIT-AND-PLAN.md
    # §6; mu_contracts.domain.model.agent). ON by default — a ``SubagentStop`` capture writes to a
    # distinct agent-scoped ``η`` partition (deterministic ``agent_principal_id``) instead of the
    # Phase-0 ``[subagent:...]`` text prefix alone. OFF reverts to Phase-0 (prefix only, same
    # partition as a top-level turn). Human/top-level captures are unchanged either way.
    subagent_partition_enabled: bool = True  # env: MU_CAPTURE__SUBAGENT_PARTITION_ENABLED

    # ---- Phase 3: PreCompact promote-before-delete (AGENT-INTEGRATION-AUDIT-AND-PLAN.md §4 Phase
    # 3). ON by default — a ``PreCompact`` control event (the host is about to compact/delete the
    # session's turns) drives ``PreCompactPromoter`` to force-promote the session's at-risk STM
    # turns into a durable tier (MTM/LTM) BEFORE they are dropped, instead of the pre-Phase-3 no-op
    # (``PreCompact`` was parsed then silently swallowed via ``ExtractionSkippedError``). This is
    # the clearly-correct fix — it is the whole point of the PreCompact trigger — so it defaults ON;
    # OFF is the backward-compatible escape hatch that reverts to the old skip-and-drop behaviour.
    # Non-PreCompact events are unaffected either way.
    precompact_promote_enabled: bool = True  # env: MU_CAPTURE__PRECOMPACT_PROMOTE_ENABLED

    # THE EVENT-DRIVEN CONSOLIDATION TRIGGERS (lifecycle/session_save.py). STM is a BUFFER, and a
    # buffer drains on PRESSURE and on CLOSE — not only on a slow salience-gated timer. Before
    # these existed the ONLY routine route out of STM was the periodic sweep's
    # ``S(m) >= promote_stm_mtm`` gate, which an auto-captured turn can NEVER clear: at the default
    # importance its best possible score is 0.5(1.0)+0.2(0.0)+0.3(0.5) = 0.650 against a 0.700 gate
    # (recency is already maxed at capture, usage is 0 by definition, and recency only decays from
    # there). So captured memory never promoted, never distilled, and stayed session-trapped —
    # live-reproduced end-to-end: a fact stated in one Claude session was UNKNOWN in the next.
    # Defaults ON because a memory system whose capture path can never become durable is not doing
    # its job; OFF reverts to the pre-existing periodic-sweep-only behaviour.
    session_end_consolidate_enabled: bool = True  # env: MU_CAPTURE__SESSION_END_CONSOLIDATE_ENABLED
    # Captured turns in ONE session before its STM buffer is drained upward (the "STM limit full"
    # trigger). Counted in-process — the alternative is a store COUNT on every ingest, i.e. a
    # network round-trip on the capture hot path to answer what a local integer answers exactly as
    # well. Consolidation is idempotent (distill NOOPs an identical active fact), so an extra drain
    # costs a bounded extraction; too high a value leaves memory session-trapped for longer.
    # env: MU_CAPTURE__CONSOLIDATE_EVERY_N_CAPTURES
    consolidate_every_n_captures: int = Field(default=20, ge=1)


class McpSettings(BaseModel):
    """The AGENT-FACING MCP surface.

    **Division of labour (the product rule this models):** the HOOKS own the routine path —
    capture/ingest, tier promotion/consolidation, and general context injection all happen
    AUTOMATICALLY, with no tool call and no tokens spent by the model. MCP is the DELIBERATE
    ESCAPE HATCH: the agent reaches for it only when it wants to go deeper than the auto-injected
    context, or genuinely cannot find a fact there.

    Exposing the automatic-duty verbs as tools actively harms that: an agent that CAN call ``add``
    will re-add what the hook already captured (duplicate writes, wasted tokens, and a second
    capture policy competing with the real one), and an agent that CAN call ``promote``/
    ``consolidate`` will second-guess a lifecycle it has no visibility into. Steering with prose
    alone is unreliable — the tool simply is not offered.
    """

    model_config = ConfigDict(extra="forbid")

    # env: MU_MCP__EXPOSE_AUTOMATIC_TOOLS — default OFF. ON re-exposes `add`/`consolidate`/
    # `promote`/`demote` for debugging, a headless SDK-style caller, or a host with no hooks
    # installed (where nothing else would ever capture).
    expose_automatic_tools: bool = False

    # ---- memory-health + pinning (memory-health-pinning-spec.md §7) ------------------------
    # TWO flags, not one, because the two verbs carry genuinely different risk. `health` is
    # read-pure (mu-core's `MemoryHealthService` takes no write port at all —
    # mu-core/packages/mu-engine/src/mu_engine/services/health/service.py:86-98); `pin`/`unpin`
    # are a LIFECYCLE OVERRIDE. A single flag would force the read lens to inherit the override's
    # risk profile and would make the most likely owner ruling — "health yes, pin no" —
    # inexpressible in configuration.
    #
    # WHAT THE SPEC ACTUALLY SAYS, quoted rather than paraphrased. §7.1 line 332 is:
    #     | **MCP tool** | `memory.local.health` | Reachable from inside the agent host where the
    #     user works, alongside `memory.local.status` (§7.15). |
    # That is a REACHABILITY PURPOSE CLAUSE, not silence, and `remove_tool` below defeats it: in
    # an MCP host the model is the only caller, so "not on the model's tool list" and "not
    # reachable inside the agent host" are the same state. §7.2 line 340's MCP row, by contrast,
    # has an EMPTY Notes column — it asks for `memory.pin`/`memory.unpin` and says nothing more.
    #
    # Both are nevertheless OFF by default, and this is an OVERRIDE of §7.1:332 pending an owner
    # ruling, not a reading of it:
    #   1. `tests/unit/test_mcp_surface_policy_unit.py:47` — pre-existing and owner-ratified —
    #      asserts the default surface is EXACTLY the seven deep-dive names. Adding `health` to it
    #      breaks that test, and that test is the owner's ratified statement of this surface.
    #   2. §7.1:332's own companion does not exist here: mu-client registers NO `status` tool
    #      (`mcp/server.py` has add/recall/get/consolidate/search/build_context/ask/promote/
    #      demote/update/delete only). The row is describing the design-set-wide MCP surface —
    #      api-sdk-mcp-surface-design.md:904 puts "the 7+ MCP tools + `memory.local.status`" on the
    #      frozen contract list — not this host's deliberately-narrowed hook-aware local surface.
    #      Honouring it here means registering `status` too, which is another subsystem's work.
    #   3. Flipping a flag on is reversible; retracting a tool a model has started calling is not.
    #
    # NOT COMPENSATED ELSEWHERE — stated plainly because the first version of this comment claimed
    # otherwise. `mu health`/`mu pin`/`mu unpin` and the `/health` `/pin` `/unpin` IPC routes are
    # ALSO unable to answer on a real host today: both engine services require a
    # `MemoryRepository` and mu-core ships no implementation of it (only the Protocol at
    # mu-core/packages/mu-contracts/src/mu_contracts/ports/memory.py:142), so every surface
    # answers its named "not wired" degrade. The exposure question is therefore about what happens
    # the day that façade lands, and it is open for the owner either way.

    # env: MU_MCP__EXPOSE_HEALTH_TOOL — default OFF. ON puts the read-only `health` lens on the
    # agent-facing surface, which is what §7.1:332 asks for.
    expose_health_tool: bool = False

    # env: MU_MCP__EXPOSE_PIN_TOOLS — default OFF. ON puts `pin`/`unpin` — the lifecycle override
    # — on the agent-facing surface. Independent of `expose_health_tool` on purpose.
    expose_pin_tools: bool = False


class OutboxSettings(BaseModel):
    """capture-spec.md §10/§8.3, same literal default path as ``ClientSettings.outbox_db_path``
    (kept as a plain field there for backward compat with the foundation stage's tests) so a later
    promotion of that flat field into this subtree is a no-behaviour-change rename.
    ``env: MU_OUTBOX__*``."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    outbox_path: Path = Path("~/.memory-universe/outbox.sqlite")
    fsync: bool = True  # synchronous=FULL on the append txn (non-negotiable; not read as a knob
    #                      by SqliteOutbox this stage — kept for parity with the pinned shape)
    worker_concurrency: int = 4
    batch_size: int = 64
    max_attempts: int = 8
    base_backoff_s: float = 0.5
    poll_interval_s: float = 0.5


class InjectSettings(BaseModel):
    """capture-spec.md §10/§7.2 (the F4 10k budget). ``env: MU_INJECT__*``."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    top_k: int = 8
    hot_session_ttl_s: int = 1800
    stale_after_s: int = 120
    body_budget_chars: int = 10_000  # Claude Code additionalContext cap (F4)
    recall_dir: Path = Path("~/.memory-universe/recall")  # F4 over-budget spill dir


class DaemonIpcSettings(BaseModel):
    """daemon-app-skeleton-spec.md §9 ``IpcSettings``, same literal default path as
    ``ClientSettings.daemon_socket_path``. ``env: MU_IPC__*``."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    socket_path: Path = Path("~/.memory-universe/daemon.sock")
    socket_peer_check: bool = True
    socket_timeout_s: float = 2.0  # capture-spec.md §3: "Timeout <= 2s on the socket call"

    # ---- FRAMING BUDGET for ONE newline-delimited-JSON request (both ends of the socket).
    # asyncio's ``StreamReader`` default is 64 KiB; a line longer than the limit makes
    # ``readline()`` raise, which — before this became a configured, ANSWERED condition — closed
    # the connection with no reply and silently DROPPED the capture (the hook could not tell the
    # empty reply apart from a success). A real Claude Code ``PostToolUse`` record carries the
    # untruncated ``tool_response`` (the ``capture.tool_outcome_max_chars`` slice is taken
    # daemon-side, AFTER parsing), and a single Read/Grep/MCP tool result in the hundreds of KiB
    # is routine — 64 KiB is exceeded by ordinary work, not by abuse. 4 MiB is ~10x the largest
    # record any host tool can plausibly emit, while still BOUNDING the per-connection read
    # buffer, so one runaway hook cannot grow the daemon's RSS without limit. It is a bound on a
    # uid-checked local peer (``socket_peer_check``/``SO_PEERCRED``), i.e. on our own runaway
    # process, never on an attacker. A record past this bound gets an explicit 413 and the client
    # spools it — refused, never silently lost. env: MU_IPC__MAX_REQUEST_BYTES
    max_request_bytes: int = Field(default=4 * 1024 * 1024, ge=64 * 1024)
    # Server-side bound on EACH blocking step of one request/response exchange (reading the
    # request line; draining the reply). Without it a peer that connects and never sends ``\n`` —
    # or that hangs up without reading its reply — pins its handler forever, and ordered shutdown
    # (``stop_accepting()`` -> ``wait_closed()``, which in 3.12 waits for live handlers) then
    # never returns. Deliberately ABOVE the client's own end-to-end ``socket_timeout_s`` budget so
    # it never cuts off a peer that is genuinely mid-transfer (that peer would abort its own call
    # first anyway) — it only reaps one that has gone silent. env: MU_IPC__REQUEST_IO_TIMEOUT_S
    request_io_timeout_s: float = Field(default=5.0, gt=0)


class ModelProfileSettings(BaseModel):
    """Where the client's model slot points. ``mu_local``'s composition root (``mu_local/
    composition.py:_build_llm_catalog``) closed the ``StorageSettings.llm=None`` seam on
    2026-07-27 (``mu-core`` commit ``e8fdaeb``) and now builds a REAL ``ModelRouter`` whenever a
    ``mu_local.config.ModelProfileSettings`` is supplied — ``LocalMemoryHost.start()`` maps THIS
    profile into that shape (see ``host.py::_local_llm_profile``), so a bare ``ClientSettings()``
    wires the daemon's ``LocalMemory`` to the REAL SLM with zero required env vars.

    Defaults at the real ``mu-dev-slm`` sidecar container (``qwen2.5:0.5b`` over its OpenAI-
    compatible ``/v1`` shim on the shared dev box) — the SAME shape the reference integration test
    proved against the real docker SLM (``mu-core/packages/mu-engine/tests/pipelines/
    test_distill_llm_slm_int.py::SlmTestSettings`` — ``litellm_provider="openai"``,
    ``api_base=".../v1"``): ``provider`` is litellm's OpenAI-compatible provider prefix (NOT the
    raw ``ollama`` client), so ``mu_local``'s catalog builds the litellm model id
    ``"{provider}/{model}"`` (``openai/qwen2.5:0.5b``) against Ollama's OpenAI-compat endpoint.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    provider: str = "openai"  # litellm's OpenAI-compatible provider prefix (catalog.py LOCAL_HTTP)
    base_url: str = "http://127.0.0.1:11435/v1"  # mu-dev-slm host port, OpenAI-compat /v1 shim
    model_name: str = "qwen2.5:0.5b"
    api_key: SecretStr = SecretStr("sk-mu-local-placeholder")  # NOT a secret — Ollama's OpenAI-
    #   compat shim never validates it (same placeholder mu_local.config.ModelProfileSettings
    #   defaults to); a real default (never None) so mapping never needs a second fallback literal


class ClientSettings(BaseSettings):
    """The one mu-client env boundary (mirrors ``mu_contracts.config.Settings``'s shape/discipline
    — ``@lru_cache``'d access via :func:`get_client_settings`, never bare ``ClientSettings()``
    scattered through call sites)."""

    model_config = SettingsConfigDict(
        env_prefix="MU_",
        env_nested_delimiter="__",
        env_file=(".env", ".env.test"),
        env_file_encoding="utf-8",
        extra="ignore",
        # `model` carries a `validation_alias` (its env name had to move off the engine-owned
        # `MU_MODEL__` prefix — see that field). A validation_alias otherwise REPLACES the field
        # name for population, which would silently break every direct construction call such as
        # the documented heuristic-mode opt-out `ClientSettings(model=None)` (it would fall back
        # to the default profile instead of disabling the LLM). `populate_by_name` keeps the
        # Python-level field name working alongside the env alias.
        populate_by_name=True,
    )

    # Store ENDPOINTS — mu-core's StorageSettings shape, reused verbatim (see module docstring).
    storage: StorageSettings = Field(default_factory=StorageSettings)

    # Model profile (see ModelProfileSettings docstring); env: **MU_MODEL_PROFILE__\***.
    #
    # ENV-NAMESPACE COLLISION FIX (live-reproduced): this slot used to read ``MU_MODEL__*``, but
    # that prefix is ALREADY OWNED by the ENGINE's own ``EngineSettings.model``
    # (``mu_engine.providers.settings.ModelSettings``, ``extra="forbid"``), whose fields are
    # per-task model-GROUP names (``answer_model``/``adjudicate_model``/``embed_backend``/...) —
    # a completely different shape from this client-side endpoint profile
    # (``provider``/``base_url``/``model``/``api_key``). Sharing the prefix produced two real
    # failures, both observed:
    #   1. HARD CRASH — ``MU_MODEL__BASE_URL``/``__MODEL_NAME``/``__API_KEY`` are *extra* on
    #      ``ModelSettings``, so ANY process that constructs ``EngineSettings`` with them set died
    #      with ``ValidationError: 3 validation errors for EngineSettings ... extra_forbidden``.
    #      ``mu install claude-code`` BAKES exactly those vars into the generated ``.mcp.json``
    #      env block, so the shipped MCP server could not start at all in the very configuration
    #      the installer wrote — the whole agent-plugin surface was dead on arrival.
    #   2. SILENT OVERRIDE — ``MU_MODEL__PROVIDER`` *is* a valid ``ModelSettings`` field, so it
    #      quietly repointed the engine's provider-registry key (``azure`` -> ``openai``) instead
    #      of erroring. Worse than the crash, because nothing surfaced it.
    # Own prefix, no overlap, both failures structurally impossible.
    #
    # Defaults to the real mu-dev-slm profile (never None) — an explicit
    # ``ClientSettings(model=None)`` is the opt-out that keeps ``LocalMemoryHost.start()`` in
    # heuristic mode (backward compat; host.py maps None -> None ->
    # mu_local.config.StorageSettings.llm=None, byte-for-byte the prior behaviour).
    model: ModelProfileSettings | None = Field(
        default_factory=ModelProfileSettings,
        validation_alias="MU_MODEL_PROFILE",
    )

    # Daemon-stage seams (kept flat for backward compat with the foundation stage's tests); env:
    # MU_DAEMON_SOCKET_PATH / MU_OUTBOX_DB_PATH. Same literal defaults as daemon-app-skeleton-
    # spec.md §9's IpcSettings.socket_path / OutboxSettings.outbox_path, and as the nested
    # ``ipc``/``outbox`` subtrees below (THIS stage reads the nested subtrees; these two flat
    # fields are the pre-daemon-stage seam, unused by mu_client.daemon/outbox/capture code).
    daemon_socket_path: Path = Path("~/.memory-universe/daemon.sock")
    outbox_db_path: Path = Path("~/.memory-universe/outbox.sqlite")

    # THIS stage's nested subtrees (daemon-app-skeleton-spec.md §9, capture-spec.md §10).
    capture: CaptureSettings = Field(default_factory=CaptureSettings)
    mcp: McpSettings = Field(default_factory=McpSettings)
    outbox: OutboxSettings = Field(default_factory=OutboxSettings)
    inject: InjectSettings = Field(default_factory=InjectSettings)
    ipc: DaemonIpcSettings = Field(default_factory=DaemonIpcSettings)

    # The η defaults LocalMemoryHost/CLI fall back to when a caller doesn't name one (DEV-STANDARDS
    # rule 3: even a "default" string is config, never an inline literal at the call site).
    default_workspace: str = "local"
    default_namespace: str = "default"
    default_user: str = "default"

    # This device's identity for the plane-qualified lease-naming convention (CANONICAL §7.5) —
    # threaded into ``SqliteWalLeaseAdapter`` (S1-06) by the daemon composition root
    # (``daemon/app.py``). env: ``MU_DEVICE_ID``.
    device_id: str = Field(default_factory=_default_device_id)


@lru_cache
def get_client_settings() -> ClientSettings:
    """The single, cached read of the client's env boundary (mirrors
    ``mu_contracts.config.settings.get_settings``). Never construct ``ClientSettings()`` bare at a
    production call site — route through here so the boundary is read exactly once per process.
    Callers that need an explicit override (tests, per-run η isolation) still construct
    ``ClientSettings(...)`` directly; this accessor is only for the "no override, use env" path.

    Reads the CWD-INDEPENDENT env-file set (:func:`resolve_env_files`) rather than the plain CWD
    ``.env``/``.env.test`` in ``model_config`` — so a ``mu-mcp``/daemon spawned from an arbitrary
    project directory still finds the real store endpoints (gap A). ``_env_file`` overrides
    ``model_config.env_file``; OS env vars still win over every file."""
    # ``_env_file`` is a real pydantic-settings BaseSettings init kwarg, but the pydantic mypy
    # plugin synthesises ``__init__`` from model FIELDS only and omits it — hence the ignore.
    return ClientSettings(_env_file=resolve_env_files())  # type: ignore[call-arg]
