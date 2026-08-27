"""``SharedPrivateGuard`` — the honest local/shared plane boundary at the MCP surface.

The local-plane MCP server is PRIVATE-plane ONLY (ADR-0003 "SDK-independent local/shared MCP
boundaries"; AGENT-INTEGRATION-AUDIT-AND-PLAN.md §4 Phase 1). Everything a caller adds/recalls
through it stays in the caller's own private partition; there is NO shared room, grant, or
``authorized_ids`` stamp anywhere on this path — that is the PARKED ``mu-server`` transfer plane.

This guard makes the boundary EXPLICIT and HONEST at the tool surface: any shared-plane argument
(``visibility``/``subject``/``predicate``/``object``) supplied to a local tool is REJECTED before
the request is routed to the engine — a fixed, safe error that neither silently drops the field nor
pretends a shared capability exists. This is the transport-level rejection ADR-0003 specifies
("reject ``private``/shared before it can be serialized or routed").

Defense-in-depth, NOT the only check: ``LocalMemory.add``/``recall`` also reject these fields via
``validate_plane_fields(..., shared_configured=False)`` -> ``PlaneFieldRejectedError``. This guard
is the earlier, MCP-layer rejection so the boundary is visible at the surface a stranger's agent
touches, not only deep inside the engine.
"""

from __future__ import annotations

__all__ = ["SHARED_PLANE_FIELDS", "SharedPlaneRejectedError", "SharedPrivateGuard"]

# The canonical shared-plane fields of the unified verb superset (design §2.5). On the local plane
# every one of these MUST be absent — supplying any is a boundary violation, not a valid request.
SHARED_PLANE_FIELDS: tuple[str, ...] = ("visibility", "subject", "predicate", "object")


class SharedPlaneRejectedError(ValueError):
    """A shared-plane argument reached a PRIVATE-plane local tool (ADR-0003 boundary violation)."""


class SharedPrivateGuard:
    """Rejects shared-plane arguments on the local (private) MCP plane.

    A local stub by design (Phase 1): there is no shared plane to route TO yet, so the only honest
    behaviour is to refuse any shared-plane argument outright. When the transfer plane is un-parked,
    this becomes the split point where shared verbs route to the shared registry instead.
    """

    def assert_private(self, **fields: object) -> None:
        """Raise :class:`SharedPlaneRejectedError` if ANY supplied field is non-``None``.

        Callers pass the shared-plane arguments by name (e.g.
        ``guard.assert_private(visibility=visibility, subject=subject, ...)``); a ``None`` value
        means "not supplied" and passes, matching the engine's own plane-gate semantics.
        """
        offending = sorted(name for name, value in fields.items() if value is not None)
        if offending:
            raise SharedPlaneRejectedError(
                "shared-plane arguments are not accepted on the local (private) MCP plane: "
                f"{', '.join(offending)}. The shared/transfer plane is not available here "
                "(it is the parked mu-server plane); this server serves your private memory only."
            )
