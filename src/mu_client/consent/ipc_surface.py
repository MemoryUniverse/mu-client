"""The IPC transport shape of the agent-share consent surface (D4 §4.2-D step 4).

Mirrors :mod:`mu_client.memory_health`'s role for the health/pin routes: route names, the closed
set of stable error names, request parsing and failure→payload mapping live HERE, so
:class:`~mu_client.daemon.ipc.IpcServer` stays a dispatcher and never grows a second opinion about
what a malformed consent request is.

**Why the daemon serves these at all, when the CLI can call the service directly.** D4 §4.2-D step 4
asks for *"a persistent 'your agent is shared here' affordance with one-tap revoke"* — persistent
meaning it outlives a command invocation. The daemon is the only resident process on this device, so
it is where a persistent affordance can live. The CLI reaches the same
:class:`~mu_client.consent.service.AgentShareConsentService` directly when no daemon is running,
because a consent screen an owner cannot open without first starting a daemon is not an affordance.

**Content-free (rule 3).** Every payload is ids, capability names, enum members, timestamps and
fixed English. Refusals echo NO part of the request.
"""

from __future__ import annotations

from typing import Any, Final

from mu_client.errors import (
    InvalidRevokeReasonError,
    SharedPlaneNotConfiguredError,
    SharedPlaneUnreachableError,
)

__all__ = [
    "AGENT_SHARE_REVOKE_ROUTE",
    "AGENT_SHARE_ROUTE",
    "CONSENT_INVALID_REASON",
    "CONSENT_MALFORMED_REQUEST",
    "CONSENT_SURFACE_ERRORS",
    "SHARED_PLANE_UNCONFIGURED",
    "SHARED_PLANE_UNREACHABLE",
    "consent_failure_response",
    "consent_malformed_response",
    "consent_request_of",
]

#: Flat names, matching the shipped ``health``/``pin``/``unpin`` convention (AD-22) rather than the
#: design set's dotted MCP names — an IPC route table is not the MCP tool namespace.
AGENT_SHARE_ROUTE: Final = "agent-share"
AGENT_SHARE_REVOKE_ROUTE: Final = "agent-share-revoke"

#: 503 — this device has no server configured, so there is no share to inspect. NOT 404: "there is
#: no share" and "this device cannot look" are different answers (see
#: :class:`~mu_client.errors.SharedPlaneNotConfiguredError`).
SHARED_PLANE_UNCONFIGURED: Final = "shared_plane_not_configured"

#: 502 — a server IS configured and did not answer. The owner must be able to tell this from the
#: line above, because only one of them means "retry later".
SHARED_PLANE_UNREACHABLE: Final = "shared_plane_unreachable"

CONSENT_MALFORMED_REQUEST: Final = "malformed_request"

#: 400 — the request was well-formed but ``reason`` was not a NAMED reason
#: (:data:`~mu_client.consent.wire.NAMED_REASON_PATTERN`). Distinct from
#: :data:`CONSENT_MALFORMED_REQUEST` so a caller can tell "you sent the wrong shape" from "that
#: value is not allowed on a content-free ledger row".
CONSENT_INVALID_REASON: Final = "invalid_revoke_reason"

#: The closed set ``IpcServer`` catches on this route pair. Anything outside it is a bug and
#: propagates (DEV-STANDARDS rule 8: never a silent wrong answer).
CONSENT_SURFACE_ERRORS: tuple[type[Exception], ...] = (
    InvalidRevokeReasonError,
    SharedPlaneNotConfiguredError,
    SharedPlaneUnreachableError,
)


def consent_request_of(request: dict[str, Any]) -> tuple[str, str, str | None]:
    """Extract ``(room_id, agent_principal_id, reason)``, raising ``KeyError``/``TypeError`` on a
    malformed body so the caller answers :func:`consent_malformed_response`.

    ``reason`` is passed through UNTOUCHED. It is refused — as a named
    :class:`~mu_client.errors.InvalidRevokeReasonError`, mapped to a 400 by
    :func:`consent_failure_response` — at step 0 of
    :meth:`~mu_client.consent.service.AgentShareConsentService.revoke`, before the server is read
    and before anything is cut. A duplicate check here was written first and then removed: it
    changed no observable behaviour (the route answers the same 400 either way), and a check that
    cannot be made to fail on its own is a call site that looks wired and does nothing.
    """
    room_id = request["room_id"]
    agent_principal_id = request["agent_principal_id"]
    reason = request.get("reason")
    if not isinstance(room_id, str) or not isinstance(agent_principal_id, str):
        raise TypeError("room_id and agent_principal_id must be strings")
    if reason is not None and not isinstance(reason, str):
        raise TypeError("reason must be a string when supplied")
    if not room_id or not agent_principal_id:
        raise ValueError("room_id and agent_principal_id must be non-empty")
    return room_id, agent_principal_id, reason


def consent_malformed_response() -> dict[str, Any]:
    """400 + a stable name, echoing NO part of the request."""
    return {"status": 400, "error": CONSENT_MALFORMED_REQUEST}


def consent_failure_response(exc: Exception) -> dict[str, Any]:
    """Map one caught :data:`CONSENT_SURFACE_ERRORS` member onto its transport payload.

    Content-free: the status, the stable name, and — for an unreachable server — the upstream HTTP
    status when there was one, because "the server said 403" and "the server did not answer" are
    the difference between a permissions problem and an outage.
    """
    if isinstance(exc, InvalidRevokeReasonError):
        # The RULE, never the rejected value — echoing it would put the very prose this refusal
        # exists to keep off a ledger row onto an IPC payload instead.
        return {"status": 400, "error": CONSENT_INVALID_REASON, "rule": exc.rule}
    if isinstance(exc, SharedPlaneNotConfiguredError):
        return {"status": 503, "error": SHARED_PLANE_UNCONFIGURED}
    unreachable = exc if isinstance(exc, SharedPlaneUnreachableError) else None
    payload: dict[str, Any] = {"status": 502, "error": SHARED_PLANE_UNREACHABLE}
    if unreachable is not None and unreachable.status_code is not None:
        payload["upstream_status"] = unreachable.status_code
    return payload
