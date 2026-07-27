"""``ClientSettings`` — the ONE mu-client env boundary (DEV-STANDARDS rule 3: no hardcoded
literals; everything flows from a central config).

Deliberately reuses mu-core's shapes rather than re-inventing them:

* ``storage`` is ``mu_contracts.config.settings.StorageSettings`` VERBATIM (the same class the
  ``mu-dev-*`` container stack's endpoints already validate against, ``.env.test``) — "store
  endpoints (reuse mu-core's ``StorageSettings`` shape)" from the brief, read literally: it is the
  identical class, not a re-shaped copy. mu-client shares mu-core's ``MU_`` env-var namespace (same
  prefix, same nested delimiter, same ``.env``/``.env.test`` files) so ONE env file configures both
  the engine's connection endpoints *and* mu-client's own subtrees below — no translation layer.
* ``model`` is new here (mu-local's ``StorageSettings.llm`` is deliberately ``None`` this phase —
  Azure PARKED, ``mu_local/config.py``) — a forward-looking config surface a LATER phase's LLM-aware
  ``LocalContainer`` wiring will consume; it already defaults at the mu-dev-slm sidecar
  (``127.0.0.1:11435``, an Ollama-compatible endpoint) so pointing the client at a real local model
  is a config change, never a code change, the day that wiring lands.
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

from functools import lru_cache
from pathlib import Path

from mu_contracts.config.settings import StorageSettings
from pydantic import BaseModel, ConfigDict, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = [
    "CaptureSettings",
    "ClientSettings",
    "DaemonIpcSettings",
    "InjectSettings",
    "ModelProfileSettings",
    "OutboxSettings",
    "get_client_settings",
]


class CaptureSettings(BaseModel):
    """Capture-cluster knobs (capture-spec.md §10, scoped to the ONE host this stage wires).
    ``env: MU_CAPTURE__*``."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    tool_outcome_max_chars: int = 500  # salient-slice truncation budget (capture-spec.md §7.1)
    spool_dir: Path = Path("~/.memory-universe/spool")  # on outbox-unreachable (§2.2)


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


class ModelProfileSettings(BaseModel):
    """Where the client's model slot points (reserved seam — mu-local's LLM is PARKED this
    phase, ``mu_local/config.py`` ``StorageSettings.llm: BackendChoice | None = None``).

    Defaults at the real ``mu-dev-slm`` sidecar container (``qwen2.5:0.5b``, an Ollama-compatible
    HTTP API on the shared dev box) so a LATER LLM-aware wiring needs only a config read here, never
    a hardcoded literal in that wiring's construction code (DEV-STANDARDS rule 3).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    provider: str = "ollama"  # LiteLLM provider key (ProviderRegistry, mu-engine providers)
    base_url: str = "http://127.0.0.1:11435"  # mu-dev-slm host port -> container 11434
    model_name: str = "qwen2.5:0.5b"
    api_key: SecretStr | None = None  # local Ollama-compatible sidecar needs none; kept for parity


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
    )

    # Store ENDPOINTS — mu-core's StorageSettings shape, reused verbatim (see module docstring).
    storage: StorageSettings = Field(default_factory=StorageSettings)

    # Model profile — reserved seam (see ModelProfileSettings docstring); env: MU_MODEL__*.
    model: ModelProfileSettings = Field(default_factory=ModelProfileSettings)

    # Daemon-stage seams (kept flat for backward compat with the foundation stage's tests); env:
    # MU_DAEMON_SOCKET_PATH / MU_OUTBOX_DB_PATH. Same literal defaults as daemon-app-skeleton-
    # spec.md §9's IpcSettings.socket_path / OutboxSettings.outbox_path, and as the nested
    # ``ipc``/``outbox`` subtrees below (THIS stage reads the nested subtrees; these two flat
    # fields are the pre-daemon-stage seam, unused by mu_client.daemon/outbox/capture code).
    daemon_socket_path: Path = Path("~/.memory-universe/daemon.sock")
    outbox_db_path: Path = Path("~/.memory-universe/outbox.sqlite")

    # THIS stage's nested subtrees (daemon-app-skeleton-spec.md §9, capture-spec.md §10).
    capture: CaptureSettings = Field(default_factory=CaptureSettings)
    outbox: OutboxSettings = Field(default_factory=OutboxSettings)
    inject: InjectSettings = Field(default_factory=InjectSettings)
    ipc: DaemonIpcSettings = Field(default_factory=DaemonIpcSettings)

    # The η defaults LocalMemoryHost/CLI fall back to when a caller doesn't name one (DEV-STANDARDS
    # rule 3: even a "default" string is config, never an inline literal at the call site).
    default_workspace: str = "local"
    default_namespace: str = "default"
    default_user: str = "default"


@lru_cache
def get_client_settings() -> ClientSettings:
    """The single, cached read of the client's env boundary (mirrors
    ``mu_contracts.config.settings.get_settings``). Never construct ``ClientSettings()`` bare at a
    production call site — route through here so the boundary is read exactly once per process.
    Callers that need an explicit override (tests, per-run η isolation) still construct
    ``ClientSettings(...)`` directly; this accessor is only for the "no override, use env" path."""
    return ClientSettings()
