"""The mu-client half of MEMORY-HEALTH + PINNING — ``memory-health-pinning-spec.md`` §7.

§7's preamble is the rule this module exists to keep: *"Three surfaces onto each capability; all
read the one projector / call the one service (none computes its own)."* The three client surfaces
are the daemon IPC routes (:mod:`mu_client.daemon.ipc`), the MCP tools
(:mod:`mu_client.mcp.tools`) and the ``mu health|pin|unpin`` CLI (:mod:`mu_client.cli`). Every one
of them calls ``mu_engine.services.health.MemoryHealthService`` / ``.pin.PinService`` — mu-client
holds NO health rule, NO flag logic and NO pin bound of its own. This module is the shared *shape*
of those surfaces (η, scope, error mapping, argument codecs) and nothing else.

WHAT IS CONTENT-FREE HERE, AND WHY IT IS FREE
---------------------------------------------
``MemoryHealthEntry`` deliberately ships with **no** ``preview`` field: mu-core resolved spec §0
line 17 against CANONICAL §7.26 and did not build it (``mu_contracts/domain/model/health.py``
lines 11-19). Every field of ``MemoryHealthView`` is therefore an id, an enum, a number or a
timestamp, so ``view.model_dump(mode="json")`` is content-free *by construction* on all three
surfaces — the client never has to redact anything, and must never add a snippet back.
``PinResult`` is likewise ids/booleans/timestamps, and ``PinRequest.reason`` is a short NAMED
classification (mu-core bounds it at 200 chars), never memory text — so it is accepted from the
caller but never logged here.

WHEN THESE SURFACES ANSWER, AND WHEN THEY DO NOT
------------------------------------------------
Both engine services take ``repo: MemoryRepository``
(``mu-core/.../services/health/service.py:89`` and ``.../services/pin/service.py:94``). mu-core
now implements that facade -- ``mu_engine.services.memory.repository.TieredMemoryRepository`` over
a ``TierRouter`` (CANONICAL §6-P2) -- and ``mu_local.composition.LocalContainer`` builds it plus
both services over the SAME stm/mtm/ltm adapters every other local verb uses. ``LocalMemory``
exposes them (``.health`` / ``.pin``), ``LocalMemoryHost`` passes them through, and the daemon and
the MCP server hand them to ``IpcServer(health=..., pin=...)`` / the MCP engine holder. So on a
normal FULL-LOCAL binding these surfaces serve real answers over real stores.

They stay ``| None``-shaped because ABSENCE is still a real binding state, not a placeholder:
three of the five vector backends (``pgvector``/``chroma``/``faiss``) expose no point-get and no
partition-walk primitive at all, so ``LocalContainer`` builds NEITHER service on such a binding
and every surface answers a NAMED, content-free "not wired" degrade -- the identical discipline
``IpcServer`` already applies to ``lifecycle_manager`` (``daemon/ipc.py:29-33``,
``lifecycle_manager_not_wired``). No stub, no ``NotImplementedError``, no fabricated view.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from mu_contracts.domain.errors import (
    NamespaceIsolationError,
    PinAuthorizationError,
    PinLimitExceededError,
    PinnedTransitionBlocked,
    PinPartiallyAppliedError,
    PinTargetNotFoundError,
    PinTargetNotPinnableError,
    TierCapabilityUnavailableError,
    TierRepositoryUnavailableError,
)
from mu_contracts.domain.model.health import MemoryHealthFlag
from mu_contracts.domain.model.memory import Namespace, Visibility
from mu_contracts.domain.model.pin import PinRequest
from mu_contracts.domain.model.scope import ClientScope

if TYPE_CHECKING:
    from mu_client.config import ClientSettings

__all__ = [
    "DEFAULT_SESSION",
    "HEALTH_ROUTE",
    "HEALTH_SURFACE_ERRORS",
    "HEALTH_UNWIRED",
    "MALFORMED_REQUEST",
    "PIN_PARTIALLY_APPLIED",
    "PIN_ROUTE",
    "PIN_SURFACE_ERRORS",
    "PIN_UNWIRED",
    "SHARED_PLANE_REFUSED",
    "TIER_INCAPABLE",
    "TIER_UNAVAILABLE",
    "UNKNOWN_HEALTH_FLAG",
    "UNPIN_ROUTE",
    "health_failure_response",
    "local_scope",
    "malformed_request_response",
    "namespace_for",
    "namespace_on_the_wire",
    "parse_health_flags",
    "pin_failure_response",
    "pin_request_of",
    "private_plane_refusal",
    "unwired_response",
]

#: The IPC route names (spec §7.1 line 331 / §7.2 line 339 write them ``/health`` ``/pin``
#: ``/unpin``). This socket's routes are bare strings with no leading slash — a recorded deviation
#: of the whole front door, not of these three (``daemon/ipc.py:5-9``).
#:
#: ``health`` sits one character from the EXISTING ``healthz`` route and answers a completely
#: different question: ``healthz`` is daemon liveness (outbox depth / dead letters, an OPS read),
#: ``health`` is the USER's memory-health lens. Dispatch is exact-match so there is no functional
#: collision, but the pair is a documented reading hazard — the same distinction
#: ``S1-local-daemon-host-integration-design.md`` §7.1 draws between ``/healthz`` and
#: ``/syncstatus`` ("``/syncstatus`` is the USER's surface, not ``/healthz``"). Kept as the spec
#: names it rather than renamed unilaterally; flagged for the owner.
HEALTH_ROUTE = "health"
PIN_ROUTE = "pin"
UNPIN_ROUTE = "unpin"

#: The NAMED degrades. ``*_not_wired`` mirrors ``lifecycle_manager_not_wired`` exactly.
HEALTH_UNWIRED = "health_service_not_wired"
PIN_UNWIRED = "pin_service_not_wired"
#: A non-PRIVATE η reached a PRIVATE-plane surface (ADR-0003, the same boundary
#: :class:`~mu_client.mcp.guard.SharedPrivateGuard` refuses at the MCP surface).
SHARED_PLANE_REFUSED = "shared_plane_not_available"
UNKNOWN_HEALTH_FLAG = "unknown_health_flag"
#: The request itself did not parse — a missing/short ``memory_id``, an η that is not five parts,
#: a ``reason`` past ``PinRequest``'s 200-char bound. NAMED and answered, never raised: an
#: unhandled exception in a route handler closes the connection with NO reply, and
#: ``daemon/ipc.py``'s own docstring is explicit that "a close with no reply is indistinguishable
#: from success on the wire". A user who typed ``mu pin --reason <201 chars>`` would otherwise be
#: told the daemon is unreachable, which is a lie about a healthy daemon.
MALFORMED_REQUEST = "malformed_request"
#: A tier STORE is down (``TierRepositoryUnavailableError``). 503, like the other "come back
#: later" answers -- the binding is fine, the infrastructure is not.
TIER_UNAVAILABLE = "tier_store_unavailable"
#: A bound BACKEND has no such primitive and never will (``TierCapabilityUnavailableError``).
#: 501, not 503: retrying cannot help, only rebinding can.
TIER_INCAPABLE = "tier_capability_unavailable"
#: A cross-store pin landed on SOME tiers and failed on others. 409, and -- uniquely in this
#: table -- the payload carries the ``applied``/``failed`` tier sets and the direction, because
#: those are the whole reason the error type exists: without them the caller cannot tell a
#: conservative leftover pin from an UNPIN that stranded a row as permanently GC-ineligible. Tier
#: enum values and a boolean are configuration, not memory content, so this stays content-free.
PIN_PARTIALLY_APPLIED = "pin_partially_applied"

#: mu-local's own session default (``mu_local.local_memory._DEFAULT_SESSION``), restated here
#: because a client surface that builds η itself must land on the SAME partition the engine's own
#: verbs do — a different default would silently address a different session's memory.
DEFAULT_SESSION = "default"

#: Every failure the pin/unpin path can raise, mapped ONE-to-one to a transport status + a stable
#: machine-readable name. The exception MESSAGE is never echoed: mu-core writes the pin denials
#: non-enumerating on purpose ("not found", never the id — ``errors.py:186-193``), and this table
#: keeps that true even if a future message becomes chattier.
#:
#: ``PinTargetNotPinnableError`` is NOT in the spec's §9 list (lines 372-378); mu-core added it
#: (``errors.py:195``) and says so. It is mapped here so the surface cannot drop it on the floor,
#: and the spec addition is reported.
_PIN_FAILURES: tuple[tuple[type[Exception], int, str], ...] = (
    (PinAuthorizationError, 403, "pin_not_authorized"),
    (PinTargetNotFoundError, 404, "pin_target_not_found"),
    (PinTargetNotPinnableError, 409, "pin_target_not_pinnable"),
    (PinLimitExceededError, 429, "pin_limit_exceeded"),
    (PinnedTransitionBlocked, 409, "pinned_transition_blocked"),
    # assert_scope's refusal. Unreachable while `local_scope` derives the scope FROM the
    # authorized η (below), but mapped rather than assumed away: it collapses to the same
    # non-enumerating 404 as "absent", which is the whole point of the discipline.
    (NamespaceIsolationError, 404, "not_found"),
    # The three the FACADE introduced. Unmapped they were not "propagated loud" -- an unhandled
    # exception in a route handler closes the IPC connection with NO reply, which this module's
    # own docstring calls indistinguishable from success on the wire. So a half-landed pin -- the
    # single most operationally important outcome this feature has -- would have reached the
    # caller as a dropped connection and lived only in a daemon-side log line.
    #
    # ``TierCapabilityUnavailableError`` is NOT a subclass of ``TierRepositoryUnavailableError``:
    # they are siblings by design (errors.py:133 vs :265), "this store is down, retry" versus
    # "this backend can never answer, rebind". Both are listed; neither shadows the other.
    (PinPartiallyAppliedError, 409, PIN_PARTIALLY_APPLIED),
    (TierCapabilityUnavailableError, 501, TIER_INCAPABLE),
    (TierRepositoryUnavailableError, 503, TIER_UNAVAILABLE),
)

#: The ``except`` clause for a pin/unpin call site. Anything OUTSIDE this tuple is a genuine bug
#: and propagates loud (DEV-STANDARDS rule 8 — no bare catch, no silent fallback).
PIN_SURFACE_ERRORS: tuple[type[Exception], ...] = tuple(exc for exc, _, _ in _PIN_FAILURES)

#: The ``except`` clause for the ``/health`` call site. Health is a READ, so the pin-specific
#: refusals cannot occur on it; what CAN is a tier store going down past the service's ONE
#: modelled degrade, a bound backend that can never walk a partition, and a replayed or malformed
#: ``cursor`` (which ``services/memory/cursor.py`` refuses with ``NamespaceIsolationError`` and is
#: user-reachable through ``mu health --cursor``). All three were previously uncaught, and an
#: uncaught exception on this socket is a close with no reply.
HEALTH_SURFACE_ERRORS: tuple[type[Exception], ...] = (
    NamespaceIsolationError,
    TierCapabilityUnavailableError,
    TierRepositoryUnavailableError,
)


def local_scope(ns: Namespace) -> ClientScope:
    """The acting identity for a LOCAL-plane health/pin call, derived FROM the authorized η.

    Both engine services take a ``ClientScope`` first and hand it to
    ``TenancyGuard.assert_scope(scope, ns, operation)``. On this plane the acting principal is not
    a claim the caller makes — it is the OS peer, already proven before any route is reached: the
    daemon socket is ``SO_PEERCRED``-checked to this uid only (``daemon/ipc.py:132-134``), the MCP
    server is a stdio child of the user's own agent host, and the CLI runs as the user. There is
    exactly ONE principal on a LOCAL device, and η's ``user`` slot is it — the same
    ``(principal_id == agent_principal_id == η.user)`` shape ``LocalMemory._scope`` builds for
    every other local verb (``mu-core/packages/mu-local/src/mu_local/local_memory.py:562-569``).

    ⚠ **Never copy this onto the hosted plane.** There the principal comes from the verified
    credential (``ResolvedPrincipal``) and deriving it from the requested η would let a caller
    name someone else's partition and be handed the authority to touch it. The refusal that makes
    that safe is the credential check, not this function.
    """
    return ClientScope(
        principal_id=ns.user,
        org_id=ns.org,
        workspace_id=ns.workspace,
        session_id=ns.session,
        agent_principal_id=ns.user,
    )


def namespace_for(
    settings: ClientSettings, *, user: str | None = None, session: str | None = None
) -> Namespace:
    """Build the PRIVATE η a ``user``/``session`` pair addresses, from the client's env boundary.

    Deliberately the same slot mapping ``LocalMemory.__init__`` uses — ``ClientSettings
    .default_namespace`` fills ``η.org`` and ``default_workspace`` fills ``η.workspace``
    (``local_memory.py:143-144``: *"the η.org slot (spec §3.2: namespace fixes org)"*) — so a
    health page and a ``recall`` issued by the same caller describe the same partition. Reversing
    the two would silently address an empty one.
    """
    return Namespace(
        org=settings.default_namespace,
        workspace=settings.default_workspace,
        user=user or settings.default_user,
        session=session or DEFAULT_SESSION,
        visibility=Visibility.PRIVATE,
    )


def parse_health_flags(values: Sequence[str] | None) -> frozenset[MemoryHealthFlag] | None:
    """Decode the ``flags=`` filter. ``None``/empty = no filter (the service's own default).

    Fails loud on an unknown flag (``ValueError``), exactly as
    :func:`~mu_client.mcp.tools.resolve_tier` does for ``tier`` — a silently-ignored filter would
    show the caller MORE than they asked for and read as if it had been applied.
    """
    if not values:
        return None
    try:
        return frozenset(MemoryHealthFlag(value) for value in values)
    except ValueError:
        raise ValueError(
            f"unknown health flag; expected one of {sorted(f.value for f in MemoryHealthFlag)}"
        ) from None


def private_plane_refusal(ns: Namespace) -> dict[str, Any] | None:
    """``None`` when ``ns`` is PRIVATE; otherwise the refusal payload.

    mu-client is a PRIVATE-plane host (ADR-0003): there is no shared room here to pin in or to
    assess. ``PinService`` would refuse a SHARED η itself (``PinAuthorizationError``, service.py
    lines 168-198), but health would NOT — it would happily assess a partition this plane has no
    business serving. Refused at the surface, once, for both.
    """
    if ns.visibility is Visibility.PRIVATE:
        return None
    return {"status": 403, "error": SHARED_PLANE_REFUSED}


def unwired_response(error: str) -> dict[str, Any]:
    """The NAMED "the composition root could not build this service" degrade (see module
    docstring). 503 + a stable name, never a raise and never a fabricated empty view."""
    return {"status": 503, "error": error}


def pin_failure_response(exc: Exception) -> dict[str, Any]:
    """Map one caught :data:`PIN_SURFACE_ERRORS` member onto its transport payload.

    Content-free: the status and the stable name only — never ``str(exc)``.

    ONE member carries a body: a half-landed cross-store pin also reports WHICH tiers took
    the write, which did not, and which DIRECTION half-landed. That is the entire reason
    ``PinPartiallyAppliedError`` is a distinct type (errors.py:217) — a leftover PINNED leg
    is merely un-GC-able until reconciled, while a leftover leg after an UNPIN strands the
    row as permanently GC-ineligible, and a bare 409 cannot tell the caller which one they
    are in. Tier enum values and a boolean are configuration, never memory text.
    """
    if isinstance(exc, PinPartiallyAppliedError):
        return {
            "status": 409,
            "error": PIN_PARTIALLY_APPLIED,
            "applied": sorted(exc.applied),
            "failed": sorted(exc.failed),
            "pinned": exc.pinned,
        }
    for exc_type, status, name in _PIN_FAILURES:
        if isinstance(exc, exc_type):
            return {"status": status, "error": name}
    raise AssertionError(  # pragma: no cover — PIN_SURFACE_ERRORS is derived from the same table
        f"unmapped pin failure {type(exc).__name__}"
    )


def health_failure_response(exc: Exception) -> dict[str, Any]:
    """Map one caught :data:`HEALTH_SURFACE_ERRORS` member onto its transport payload.

    Reuses the SAME status/name table the pin path does, so ``/health`` and ``/pin`` cannot
    drift into two different names for one condition. Content-free: status and stable name
    only, never ``str(exc)`` — a cursor refusal in particular must stay non-enumerating
    (``cursor.py`` names neither partition on purpose).
    """
    for exc_type, status, name in _PIN_FAILURES:
        if isinstance(exc, exc_type):
            return {"status": status, "error": name}
    raise AssertionError(  # pragma: no cover — HEALTH_SURFACE_ERRORS is a subset of the table
        f"unmapped health failure {type(exc).__name__}"
    )


def namespace_on_the_wire(request: dict[str, Any]) -> Namespace:
    """Decode the 5-part η every namespaced route carries (``Namespace.parts()``, CANONICAL §7.3).

    Raises ``ValueError``/``KeyError``/``TypeError`` on anything malformed — the caller turns that
    into :func:`malformed_request_response` rather than letting it escape the handler.
    """
    return Namespace.from_parts(tuple(request["namespace"]))


def pin_request_of(request: dict[str, Any]) -> PinRequest:
    """Decode a ``/pin`` (or ``/unpin``) body into mu-core's own validated ``PinRequest``.

    ``memory_id`` is bounded ``min_length=1`` and ``reason`` ``max_length=200`` by the contract
    (``mu_contracts/domain/model/pin.py``) — mu-client re-states neither bound, it just lets the
    contract refuse. ``ValidationError`` is a ``ValueError`` subclass, so one ``except`` at the
    call site covers this and :func:`namespace_on_the_wire` alike.
    """
    reason = request.get("reason")
    return PinRequest(
        memory_id=str(request["memory_id"]),
        reason=str(reason) if reason is not None else None,
    )


def malformed_request_response() -> dict[str, Any]:
    """400 + a stable name. Content-free by construction: it echoes NO part of the request — not
    the offending field, not the value, not the id — so a malformed body can never round-trip
    memory text (or a mistyped one) back out through the refusal."""
    return {"status": 400, "error": MALFORMED_REQUEST}
