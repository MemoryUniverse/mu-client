"""The mu-client half of CONFLICT RESOLUTION — ``conflict-resolution-async-design.md`` §5 (AD-300).

§5's preamble is the rule this module exists to keep, restated from :mod:`mu_client.memory_health`
(the sibling module this one is modelled on byte-for-byte): *"Three surfaces onto each capability;
all read the one projector / call the one service (none computes its own)."* The three client
surfaces named by §5 are the daemon IPC route (:mod:`mu_client.daemon.ipc`), the ``mu conflicts``
CLI (:mod:`mu_client.cli`) and the MCP tool (:mod:`mu_client.mcp.tools`). Every one of them calls
``mu_engine.services.conflict.inbox.ConflictInboxProjector`` /
``mu_engine.services.conflict.resolution.ConflictResolutionService`` — mu-client holds NO conflict
rule, NO FSM edge and NO authorization decision of its own. This module is the shared *shape* of
those surfaces (η, scope, argument/error codecs) and nothing else, exactly as ``memory_health.py``
is for ``/health``/``/pin``/``/unpin``.

**What this closes (AD-300 / ARCHITECTURE-DELTAS.md).** The design doc's own AMENDMENT 2 names the
gap this module (plus the IPC routes / CLI subcommand / MCP tool built on it) closes: *"the three
read/write FACES … still do not exist, so a local user has a working apply path
(``ConflictResolutionService``, wired AD-269) and no verb to reach it."* This is the smallest
vertical slice of that — list the pending conflicts, see both sides of one, resolve it — not the
AUTOMATIC lane (still zero production call sites, unchanged) and not a durable
``ConflictRecordRepository`` (still in-process; both remain OPEN per the design doc).

WHAT IS CONTENT-FREE HERE, AND WHY IT IS FREE
---------------------------------------------
``ConflictInboxView``/``ConflictInboxItem``/``ConflictMemberView`` are frozen ``ContentFreeModel``s
in mu-core EXCEPT for ``ConflictMemberView.content`` — the ONE deliberate exception (spec §5:
"bodies hydrated by id at render time... never on the bus"), which is fine to serve over THIS
request/reply
IPC socket (never the bus) exactly as ``mu health``'s intentionally-empty-today ``content`` field
would be. This binding wires NO ``ConflictMemberHydrator`` yet (AD-300; reported, not fixed here —
it needs STM/MTM/LTM read-by-id, which is engine-internal work outside this lane), so every member
renders with ``content=""`` today: a real gap, visible to the caller, never a silently wrong body.

WHEN THIS SURFACE ANSWERS, AND WHEN IT DOES NOT
------------------------------------------------
Unlike ``health``/``pin`` (``| None``-shaped: absent on a vector backend with no partition-walk
primitive), ``LocalContainer.conflict_inbox``/``.conflict_resolution`` are built UNCONDITIONALLY
(``mu_local/composition.py``, AD-300/AD-269) — they read/write ``ConflictRecordRepository``
directly, never the tier router's ``enumerate``. So this surface has no ``*_UNWIRED`` degrade at
all: on every FULL-LOCAL binding it answers for real.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from mu_contracts.domain.errors import (
    ConflictUnresolvedError,
    IllegalConflictTransitionError,
    NamespaceIsolationError,
)
from mu_contracts.domain.model.memory import Namespace, Visibility
from mu_contracts.domain.model.scope import ClientScope
from pydantic import ValidationError

if TYPE_CHECKING:
    from mu_engine.services.conflict.resolution import ManualDecision

    from mu_client.config import ClientSettings

__all__ = [
    "CONFLICTS_LIST_ROUTE",
    "CONFLICTS_RESOLVE_ROUTE",
    "CONFLICT_INBOX_UNWIRED",
    "CONFLICT_RESOLUTION_UNWIRED",
    "CONFLICT_SURFACE_ERRORS",
    "MALFORMED_REQUEST",
    "SHARED_PLANE_REFUSED",
    "ValidationError",
    "conflict_failure_response",
    "local_scope",
    "malformed_request_response",
    "manual_decision_of",
    "namespace_for",
    "namespace_on_the_wire",
    "private_plane_refusal",
]

#: The IPC route names (spec §5 line 202: ``mu conflicts`` / daemon IPC ``/conflicts``). This
#: socket's routes are bare strings with no leading slash, matching every existing route
#: (``daemon/ipc.py``'s own recorded deviation). ``conflicts`` is the READ (list) route;
#: ``conflicts/resolve`` is the one WRITE action this lane ships (§5's four decision kinds —
#: supersede/keep_both/merge/quarantine, plus dismiss). ``reopen`` and the per-namespace/
#: per-memory policy PUTs (§5 lines 214-217) are NOT built here — reported, not this lane's
#: smallest vertical slice.
CONFLICTS_LIST_ROUTE = "conflicts"
CONFLICTS_RESOLVE_ROUTE = "conflicts/resolve"

#: The NAMED "the composition root could not build this" degrades — mirrors ``HEALTH_UNWIRED``/
#: ``PIN_UNWIRED`` exactly (``memory_health.py``). Named separately per capability (never a single
#: shared constant) so a caller can tell which of the two services is missing when only one is.
CONFLICT_INBOX_UNWIRED = "conflict_inbox_not_wired"
CONFLICT_RESOLUTION_UNWIRED = "conflict_resolution_not_wired"

#: The request itself did not parse: a missing/short ``conflict_id``, an η that is not five parts,
#: an unknown ``kind``, a ``winner_id``/``resolved_by`` bound violation, or a decision shape the
#: engine's own ``ManualDecision`` model refuses (e.g. ``merged_text_ref`` past its bound). NAMED
#: and answered, never raised — an unhandled exception in a route handler closes the IPC
#: connection with NO reply, which ``daemon/ipc.py``'s own module docstring calls
#: "indistinguishable from success on the wire".
MALFORMED_REQUEST = "malformed_request"
#: A non-PRIVATE η reached a PRIVATE-plane surface (ADR-0003) — same refusal
#: :mod:`mu_client.memory_health` and :class:`~mu_client.mcp.guard.SharedPrivateGuard` already
#: apply; restated here rather than imported so this module has no import-time dependency on
#: ``memory_health`` (the two are siblings, not a hierarchy).
SHARED_PLANE_REFUSED = "shared_plane_not_available"
#: Both ``ConflictInboxProjector.view`` and ``ConflictResolutionService.resolve`` raise this via
#: ``TenancyGuard.assert_scope`` for a caller outside the partition. Mapped to the SAME name and
#: status as ``ConflictUnresolvedError`` below, deliberately: §5's own docstring calls an unknown
#: ``conflict_id`` "the same non-enumerating shape" as a denied one, and ``local_scope`` (below)
#: derives the scope FROM the authorized η, so this specific error is structurally unreachable
#: through this surface today — mapped anyway, not assumed away (the same discipline
#: ``memory_health.py``'s own ``_PIN_FAILURES`` table applies to it).
_CONFLICT_NOT_FOUND = "conflict_not_found"
#: ``ConflictResolutionService.resolve``/``reopen`` refuse an illegal FSM edge (already resolved,
#: already dismissed, still ``DETECTED`` with no human action pending) with this — a genuine
#: conflict about STATE, not about existence, so it is NOT folded into ``_CONFLICT_NOT_FOUND``.
_CONFLICT_ILLEGAL_TRANSITION = "conflict_illegal_transition"

#: Every failure ``resolve``/the inbox ``view`` can raise, mapped to a transport status + a stable
#: machine-readable name. The exception MESSAGE is never echoed (§5's content-free discipline):
#: ``ConflictUnresolvedError`` covers BOTH "no such conflict" (spec's explicit non-enumerating
#: case) AND "this decision names no winner" / "winner_id is not a member" / "merge needs a
#: merged_text_ref" (``ConflictResolutionService._validate_decision``) — mu-core deliberately uses
#: ONE exception type for all of them (``resolution.py``), so this surface cannot structurally
#: split "absent" from "malformed decision" without parsing the refused message, which would
#: violate the same non-enumerating discipline it exists to keep. Mapped uniformly to 404 +
#: ``conflict_not_found`` — the safe direction (a probe never learns more than "no"), flagged here
#: as a reported precision loss rather than silently assumed exact.
_CONFLICT_FAILURES: tuple[tuple[type[Exception], int, str], ...] = (
    (ConflictUnresolvedError, 404, _CONFLICT_NOT_FOUND),
    (IllegalConflictTransitionError, 409, _CONFLICT_ILLEGAL_TRANSITION),
    (NamespaceIsolationError, 404, _CONFLICT_NOT_FOUND),
)

#: The ``except`` clause for a conflicts call site. Anything OUTSIDE this tuple is a genuine bug
#: and propagates loud (DEV-STANDARDS rule 8 — no bare catch, no silent fallback).
CONFLICT_SURFACE_ERRORS: tuple[type[Exception], ...] = tuple(
    exc for exc, _, _ in _CONFLICT_FAILURES
)

#: mu-local's own session default (``mu_local.local_memory._DEFAULT_SESSION``), restated here for
#: the same reason ``memory_health.DEFAULT_SESSION`` is: a client surface that builds η itself
#: must land on the SAME partition every other local verb does.
DEFAULT_SESSION = "default"


def local_scope(ns: Namespace) -> ClientScope:
    """The acting identity for a LOCAL-plane conflicts call, derived FROM the authorized η.

    Identical in shape and rationale to ``mu_client.memory_health.local_scope`` — there is exactly
    ONE principal on a LOCAL device, and η's ``user`` slot is it. Restated rather than imported so
    this module has no import-time coupling to ``memory_health`` (the two are siblings modelling
    two different design-doc specs, and a future spec's client module should not have to import
    ``memory_health`` to get a scope). ⚠ Same warning as the original: never copy this onto the
    hosted plane, where the principal comes from a verified credential, not the requested η.
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
    """Build the PRIVATE η a ``user``/``session`` pair addresses. Identical mapping to
    ``memory_health.namespace_for`` — a conflicts list/resolve issued by the same caller as a
    ``recall``/``health`` must describe the SAME partition, or reversing the slot mapping would
    silently address an empty one."""
    return Namespace(
        org=settings.default_namespace,
        workspace=settings.default_workspace,
        user=user or settings.default_user,
        session=session or DEFAULT_SESSION,
        visibility=Visibility.PRIVATE,
    )


def namespace_on_the_wire(request: dict[str, Any]) -> Namespace:
    """Decode the 5-part η every namespaced route carries (``Namespace.parts()``, CANONICAL §7.3).
    Raises ``ValueError``/``KeyError``/``TypeError`` on anything malformed — the caller turns that
    into :func:`malformed_request_response`. Identical to ``memory_health.namespace_on_the_wire``,
    restated for the same no-cross-import reason as :func:`local_scope`."""
    return Namespace.from_parts(tuple(request["namespace"]))


def private_plane_refusal(ns: Namespace) -> dict[str, Any] | None:
    """``None`` when ``ns`` is PRIVATE; otherwise the refusal payload. mu-client is a
    PRIVATE-plane host (ADR-0003): there is no shared-room conflict inbox here to read or resolve
    — a SHARED η reaching this surface is refused before either engine service is ever called."""
    if ns.visibility is Visibility.PRIVATE:
        return None
    return {"status": 403, "error": SHARED_PLANE_REFUSED}


def malformed_request_response() -> dict[str, Any]:
    """400 + a stable name. Content-free by construction: echoes NO part of the request — not the
    offending field, not the value, not the conflict id."""
    return {"status": 400, "error": MALFORMED_REQUEST}


def conflict_failure_response(exc: Exception) -> dict[str, Any]:
    """Map one caught :data:`CONFLICT_SURFACE_ERRORS` member onto its transport payload.
    Content-free: the status and the stable name only — never ``str(exc)`` (see the module-level
    discussion of why ``ConflictUnresolvedError`` cannot be split further here)."""
    for exc_type, status, name in _CONFLICT_FAILURES:
        if isinstance(exc, exc_type):
            return {"status": status, "error": name}
    raise AssertionError(  # pragma: no cover — CONFLICT_SURFACE_ERRORS is derived from this table
        f"unmapped conflict failure {type(exc).__name__}"
    )


def manual_decision_of(request: dict[str, Any], *, ns: Namespace) -> ManualDecision:
    """Decode a ``conflicts/resolve`` body into mu-core's own validated ``ManualDecision``.

    ``resolved_by`` is deliberately NOT read from the wire: it is an AUDIT field (never an authz
    principal — ``ManualDecision``'s own docstring), and on THIS plane there is exactly one
    principal, so it is set to ``ns.user`` — the same ``(principal_id == η.user)`` shape
    :func:`local_scope` builds. A caller cannot forge a different human's name onto a decision
    made from their own device. Raises ``pydantic.ValidationError`` (a subclass the call site
    catches alongside ``KeyError``/``TypeError``) on an unknown ``kind`` or an over-long
    ``merged_text_ref`` — mu-core's own bound, never restated here.
    """
    # Imported lazily (not at module top) so this module's own import surface stays as light as
    # ``memory_health.py``'s — these two types are read here and nowhere else in mu-client.
    from mu_engine.services.conflict.resolution import ManualDecision, ManualDecisionKind

    return ManualDecision(
        kind=ManualDecisionKind(str(request["kind"])),
        winner_id=(lambda v: str(v) if v is not None else None)(request.get("winner_id")),
        merged_text_ref=(lambda v: str(v) if v is not None else None)(
            request.get("merged_text_ref")
        ),
        resolved_by=ns.user,
    )
