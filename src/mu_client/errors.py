"""``mu-client`` error hierarchy (DEV-STANDARDS rule 8: one typed hierarchy, fail-loud, no silent
fallback) + the CLI's global exception boundary.

Extends the frozen ``mu_contracts.domain.errors.MemoryUniverseError`` root (the shared wire
vocabulary every mu-core/mu-client/mu-server plane maps) — additive, no new envelope.
"""

from __future__ import annotations

import asyncio
import functools
import sys
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any, ParamSpec, TypeVar

from mu_contracts.domain.errors import MemoryUniverseError, SchemaDriftError

__all__ = [
    "CaptureSchemaDriftError",
    "ClientError",
    "ClientNotStartedError",
    "ConsentStoreCorruptionError",
    "DaemonReplyInvalidError",
    "DaemonUnreachableError",
    "OutboxCorruptionError",
    "ServiceNotWiredError",
    "SharedPlaneNotConfiguredError",
    "SharedPlaneUnreachableError",
    "cli_error_boundary",
]

P = ParamSpec("P")
R = TypeVar("R")


class ClientError(MemoryUniverseError):
    """Root of mu-client's OWN error subtree — never raised directly, only its subclasses."""


class ClientNotStartedError(ClientError):
    """A verb was called on a :class:`~mu_client.host.LocalMemoryHost` before ``start()`` (or after
    ``aclose()``) — fail loud rather than silently constructing a second, unmanaged engine."""


class OutboxCorruptionError(ClientError):
    """The outbox WAL is unreadable at open — the daemon/CLI refuses to start (loud) rather than
    running healthy with a silently lost backlog (daemon-app-skeleton-spec.md §10)."""


class ServiceNotWiredError(ClientError):
    """A surface was asked for a capability whose engine service the composition root could not
    build — REFUSED LOUD, never a fabricated answer (the same stance ``LocalMemory.ask`` takes in
    heuristic mode, surfaced to an MCP caller as a tool error rather than a silent empty result).

    Today this is the memory-health + pinning pair: ``MemoryHealthService``/``PinService`` are
    built by ``mu_local.composition.LocalContainer`` over mu-core's ``MemoryRepository`` façade
    and reach this host through ``LocalMemory.health``/``.pin``, so on a normal FULL-LOCAL binding
    they ARE wired. What is left is the real binding case: three of the five vector backends
    (``pgvector``/``chroma``/``faiss``) expose no point-get and no partition-walk primitive, so on
    those the container builds neither service and these surfaces refuse instead of fabricating a
    health view or acking a pin no store would persist (see :mod:`mu_client.memory_health`).
    Content-free — it names the SERVICE, never a namespace, an id or any memory text.
    """

    def __init__(self, service: str) -> None:
        self.service = service
        super().__init__(
            f"{service} is not available on this host: the bound stores cannot serve the "
            "partition walk and id-stable pin upsert it reads/writes through, so there is "
            "nothing to answer with"
        )


class ConsentStoreCorruptionError(ClientError):
    """The local agent-share tombstone store is unreadable at open.

    A consent surface REFUSES rather than degrades: a "your agent is shared here" screen backed by
    a store it could not read cannot tell a live share from a withdrawn one, and guessing either
    way is a privacy misstatement (D4 §4.2-A). Content-free — the file path only.
    """

    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__(
            f"the local agent-share consent store at {path} could not be opened; this device "
            "cannot say which shares it has withdrawn, so it refuses to answer at all"
        )


class NaiveConsentTimestampError(ClientError):
    """A consent timestamp arrived without a timezone, so this device refuses to interpret it.

    ``datetime.astimezone`` reads a naive value as LOCAL time. Applied to a grant's server-authored
    ``issued_at``, that silently shifted the instant a blanket withdrawal is compared against — on
    any host west of UTC it made a covering cut stop covering, i.e. re-presented a withdrawn share
    as live with no error anywhere. A refusal is the only safe reading. Content-free: the offending
    timestamp only, which is not memory content.
    """

    def __init__(self, value: str) -> None:
        self.value = value
        super().__init__(
            f"consent timestamp {value!r} has no timezone; this device will not guess one, "
            "because guessing it silently changes which shares read as withdrawn"
        )


class InvalidRevokeReasonError(ClientError):
    """``--reason`` was not a NAMED reason.

    ``wire.py`` and ``tombstone.py`` both call this field *"a NAMED reason for an operator
    (``user_revoked``, ``policy_change``), never prose about the conversation"*, and the value lands
    on a Trust Ledger row that ``trust-ledger-spec.md`` §2 rule 3 requires to carry *"only ids,
    content hashes, principal ids, enums, timestamps, and counts"*. A 64-character cap does not make
    prose into a name — 62 characters of conversation content fitted, and was forwarded. This
    refusal is what actually enforces the sentence those docstrings assert.

    Raised BEFORE the revoke's local cut, never during it: a validation error thrown between the
    server read and the durable write would defeat the consent-first ordering the whole verb is
    built on. Content-free — it names the RULE, and never echoes the rejected value.
    """

    def __init__(self, rule: str) -> None:
        self.rule = rule
        super().__init__(
            f"--reason must be a named reason ({rule}), e.g. 'user_revoked' or 'policy_change'; "
            "it lands on a content-free trust-ledger row, so it may not carry prose"
        )


class SharedPlaneNotConfiguredError(ClientError):
    """A shared-plane consent verb was called on a device with no server configured.

    ``ConsentSettings.server_base_url`` is ``None`` by default: FULL-LOCAL is the norm, and a
    laptop with no team server has no agent share to inspect. Named and loud rather than an empty
    result, because "no share found" and "this device cannot look" are different answers and only
    one of them means the agent is not shared.
    """

    def __init__(self) -> None:
        super().__init__(
            "no shared plane is configured on this device (MU_CONSENT__SERVER_BASE_URL is unset), "
            "so there is no agent-share grant to read or revoke here"
        )


class SharedPlaneUnreachableError(ClientError):
    """The configured server did not answer an agent-share request.

    Carries the HTTP status when there was one (``None`` for a transport failure) and NOTHING else:
    no response body, no URL with ids in it. A revoke that raises this has still performed its
    LOCAL cut — see :mod:`mu_client.consent.service` for the consent-first ordering that makes that
    true.
    """

    def __init__(self, *, operation: str, status_code: int | None = None) -> None:
        self.operation = operation
        self.status_code = status_code
        detail = "no response" if status_code is None else f"HTTP {status_code}"
        super().__init__(f"the shared plane did not answer {operation} ({detail})")


class DaemonUnreachableError(ClientError):
    """A CLI verb that speaks ONLY to the resident daemon could not reach its socket.

    Distinct from the capture path's tolerance of the same condition: ``capture_once`` treats an
    unreachable daemon as "spool it myself" because it holds a record it must not lose, whereas
    ``mu health``/``mu pin`` hold nothing and have no second front door — the honest answer is to
    say the daemon is not running, not to half-succeed. Content-free (the socket path only).
    """


class DaemonReplyInvalidError(ClientError):
    """A 200 reply from the daemon did not validate against the mu-core contract it claims to be.

    The CLI renders health/pin replies through ``MemoryHealthView``/``PinResult`` rather than
    indexing the raw dict, so a daemon that answered the wrong SHAPE is a named, content-free
    refusal here instead of a ``KeyError`` traceback escaping
    :func:`cli_error_boundary` (which re-raises anything outside this hierarchy). Names the
    expected CONTRACT only — never a field value, an id, or any memory text.
    """

    def __init__(self, contract: str) -> None:
        self.contract = contract
        super().__init__(
            f"the daemon's reply did not match the {contract} contract — this client and the "
            "running daemon are not the same build"
        )


class CaptureSchemaDriftError(SchemaDriftError):
    """A capture parser saw an unexpected schema (capture-spec.md §4.2). Subclasses mu-core's
    :class:`~mu_contracts.domain.errors.SchemaDriftError` root (so it is caught by any handler
    written against that type) but carries the structured, content-free fields the spec pins:
    ``host``, ``source_id``, ``detected_keys``, ``expected_schema``, ``raw_sample_sha256`` — never
    the raw record itself (only a hash of it may cross this boundary)."""

    def __init__(
        self,
        *,
        host: str,
        source_id: str,
        detected_keys: list[str],
        expected_schema: list[str],
        raw_sample_sha256: str,
    ) -> None:
        self.host = host
        self.source_id = source_id
        self.detected_keys = detected_keys
        self.expected_schema = expected_schema
        self.raw_sample_sha256 = raw_sample_sha256
        super().__init__(
            f"schema drift on host={host!r} source={source_id!r}: "
            f"detected_keys={sorted(detected_keys)} not matched by any of "
            f"expected_schema={expected_schema!r} (raw_sample_sha256={raw_sample_sha256})"
        )


def cli_error_boundary(
    func: Callable[P, Awaitable[int]],
) -> Callable[P, Coroutine[Any, Any, int]]:
    """The CLI's ONE global exception→exit-code mapping (DEV-STANDARDS rule 8).

    * ``asyncio.CancelledError`` / ``KeyboardInterrupt`` propagate/exit as SIGINT convention (130) —
      never counted as an application error.
    * A ``MemoryUniverseError`` (this hierarchy, or an engine/local one it wraps) prints a single
      content-free line — the error's ``type`` + a bounded, already-safe message (never a raw
      traceback of internal state) — to stderr and returns exit code 1.
    * Any other unexpected exception is NEVER swallowed: it re-raises after being logged, so a
      genuine bug surfaces loudly (never a silent wrong "success").
    """

    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> int:
        try:
            return await func(*args, **kwargs)
        except asyncio.CancelledError:
            raise
        except KeyboardInterrupt:
            return 130
        except MemoryUniverseError as exc:
            print(f"mu: {type(exc).__name__}: {exc}", file=sys.stderr)  # noqa: T201
            return 1

    return wrapper
