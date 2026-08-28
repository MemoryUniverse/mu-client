"""**The "exposes X / keeps Y private" contract, COMPUTED** — Decision D4 §4.2-A.

``SERVER-AND-COLLAB-DESIGN-REVIEW.md:120`` gives the client one sentence to render:

    *"This agent can read/write in this room. It cannot see your private memory. Your commands to
    it are visible to everyone in the room."*

The reason D4 chose option (b) — a first-class client consent object — over option (a) — treating
the server's ``bind``/``unbind`` as sufficient — is that an owner must be able to **see what sharing
their agent exposes before they consent**. A sentence that is always printed carries no information
about *this* grant. So this module does not print it: it derives it, from the grant's actual
capability set and this device's actual capability set, and it prints a DIFFERENT sentence when the
derivation says the first one would be false.

--------------------------------------------------------------------------------------------
The computation
--------------------------------------------------------------------------------------------
Let ``G`` be the grant's ``capabilities`` and ``K`` this device's known capability vocabulary
(:func:`mu_client.consent.capabilities.known_capabilities`, split by plane).

* ``exposed_shared``  = ``G ∩ K_shared`` — what the agent may do in the room. The "X".
* ``exposed_local``   = ``G ∩ K_local``  — what the agent may do to your PRIVATE MEMORY.
* ``withheld_local``  = ``K_local minus G``  — the "Y". Everything this device can do that
  this grant does **not** confer.
* ``unrecognised``    = ``G minus K``    — capabilities the grant names that this client cannot
  explain.

Two invariants are then genuinely decided, not asserted:

* :attr:`ExposureInvariant.NO_LOCAL_CAPABILITY_EXPOSED` holds iff ``exposed_local`` is empty. **This
  is the one that earns D4's privacy sentence.** When it is broken, :meth:`AgentExposureContract.
  render` states the exposure instead of denying it.
* :attr:`ExposureInvariant.EVERY_GRANTED_CAPABILITY_RECOGNISED` holds iff ``unrecognised`` is empty.
  A consent screen that quietly drops a permission it does not understand claims a narrower
  exposure than the grant actually confers, which is the same class of error as the first.

--------------------------------------------------------------------------------------------
Two corrections to that computation, both of which had shipped a FALSE privacy sentence
--------------------------------------------------------------------------------------------
1. **Plane is decided by NAMESPACE, never by the offered tool set.** ``K_local`` spans every
   REGISTERED local tool, and any ``memory.local.*`` name outside it is still classified LOCAL (as
   an :func:`~mu_client.consent.capabilities.unexplained_local_capability`). Deciding the plane from
   :func:`~mu_client.mcp.surface.offered_tool_names` meant that under the DEFAULT configuration —
   ``expose_automatic_tools`` / ``expose_health_tool`` / ``expose_pin_tools`` all ``False``, i.e. 7
   of 14 tools withdrawn — a grant naming ``memory.local.add`` fell out of the vocabulary, left
   ``exposed_local`` empty, and printed *"It CANNOT see your private memory"* over a grant that
   confers a write into it. A withdrawn tool is a reason a grant may be inert ON THIS DEVICE; it is
   never a reason to tell an owner the grant is not about their private memory.
2. **The privacy sentence needs BOTH invariants, not one.** ``NO_LOCAL_CAPABILITY_EXPOSED`` alone
   says *"nothing I recognise as local is granted"*. If a name is unrecognised, this client does not
   know what plane it acts on — it may be a private-memory permission in a namespace this build has
   never seen. Printing a denial next to an admission of incompleteness is the exact error
   ``EVERY_GRANTED_CAPABILITY_RECOGNISED`` exists to catch, so :meth:`AgentExposureContract.render`
   requires both to hold and otherwise says what it cannot say.

--------------------------------------------------------------------------------------------
Disclosures are NOT invariants, and the separation is deliberate
--------------------------------------------------------------------------------------------
D4's third clause — *"Your commands to it are visible to everyone in the room"* — is a PROTOCOL
FACT, not a property of this grant: a command to a shared agent is a ``RoomMessage`` addressed to
it, and every ``RoomMessage`` is SHARED and fanned to the whole room
(``SERVER-AND-COLLAB-DESIGN-REVIEW.md:130``). It is true of every grant that ever exists. Dressing a
constant as a computation would make the object look like it checked something it did not, so it
lives in :attr:`AgentExposureContract.disclosures` — things the owner must be told, stated as
fixed facts, kept apart from the two things this device actually decided.

**Content-free (rule 3).** Capability names, principal ids, room ids and fixed English. No memory
content of any kind can reach this object: it never touches a store.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, computed_field

from mu_client.config import McpSettings
from mu_client.consent.capabilities import (
    Capability,
    CapabilityPlane,
    is_local_capability_name,
    known_capabilities,
    local_capabilities,
    unexplained_local_capability,
)
from mu_client.consent.wire import AgentShareGrantView

__all__ = [
    "AgentExposureContract",
    "ExposureDisclosure",
    "ExposureInvariant",
    "compute_exposure",
]


class ExposureInvariant(StrEnum):
    """A property of THIS grant that this device decided by computation."""

    #: Held iff the grant confers no ``memory.local.*`` capability. This is what makes D4's *"It
    #: cannot see your private memory"* an earned statement rather than a slogan.
    NO_LOCAL_CAPABILITY_EXPOSED = "no_local_capability_exposed"

    #: Held iff every capability the grant names is one this client can explain. When broken, the
    #: rendered contract is incomplete BY THIS CLIENT'S OWN ADMISSION, and says so.
    EVERY_GRANTED_CAPABILITY_RECOGNISED = "every_granted_capability_recognised"


class ExposureDisclosure(StrEnum):
    """A protocol fact the owner must be told. True of every grant — see the module docstring for
    why these are not invariants."""

    #: ``SERVER-AND-COLLAB-DESIGN-REVIEW.md:130``: a command to the shared agent *is* a
    #: ``RoomMessage`` addressed to it, already fanned to everyone.
    COMMANDS_ARE_VISIBLE_TO_THE_ROOM = "commands_are_visible_to_the_room"

    #: D4 §4.2-A: already-posted ``AGENT_RESULT`` messages survive a revoke
    #: (invalidate-don't-delete).
    POSTED_RESULTS_SURVIVE_REVOKE = "posted_results_survive_revoke"

    #: CANONICAL-CONTRACTS.md:255 / AD-61: a revoke cuts the agent's permission and the admission
    #: of its next result; it does not stop a process running on its owner's own machine.
    REVOKE_DOES_NOT_STOP_A_RUNNING_AGENT = "revoke_does_not_stop_a_running_agent"


_DISCLOSURE_TEXT: dict[ExposureDisclosure, str] = {
    ExposureDisclosure.COMMANDS_ARE_VISIBLE_TO_THE_ROOM: (
        "Your commands to this agent are ordinary room messages: everyone in the room sees them."
    ),
    ExposureDisclosure.POSTED_RESULTS_SURVIVE_REVOKE: (
        "Anything this agent has already posted stays in the room after you revoke."
    ),
    ExposureDisclosure.REVOKE_DOES_NOT_STOP_A_RUNNING_AGENT: (
        "Revoking cuts this agent's permission and refuses its next result; it does not stop a "
        "run already in progress on its owner's own machine."
    ),
}

#: Every disclosure applies to every grant. Named as a tuple so the render order is fixed and a
#: reader can see there is no per-grant branch here.
ALL_DISCLOSURES: tuple[ExposureDisclosure, ...] = (
    ExposureDisclosure.COMMANDS_ARE_VISIBLE_TO_THE_ROOM,
    ExposureDisclosure.POSTED_RESULTS_SURVIVE_REVOKE,
    ExposureDisclosure.REVOKE_DOES_NOT_STOP_A_RUNNING_AGENT,
)


class AgentExposureContract(BaseModel):
    """**The inspectable answer to "what does this grant expose?"**

    Frozen. Every field is derived in :func:`compute_exposure`; nothing here is settable by a
    caller who wants a friendlier answer.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    # ---- identity (all content-free ids) ----------------------------------------------------
    grant_id: str = Field(min_length=1)
    room_id: str = Field(min_length=1)
    agent_principal_id: str = Field(min_length=1)
    granted_by: str = Field(min_length=1)

    # ---- liveness ---------------------------------------------------------------------------
    #: When the SERVER dates this consent act. Carried verbatim (its clock, not this one) because
    #: it is what a blanket cut has to be compared against — see :meth:`_survivor_lines`.
    issued_at: datetime
    #: The SERVER's answer (``routes/rooms.py:849``), carried verbatim.
    server_active: bool
    #: THIS DEVICE's answer. ``True`` when a durable local tombstone covers this grant. It is a
    #: SEPARATE field from ``server_active`` on purpose: when they disagree, the disagreement is
    #: the information, and :meth:`effectively_live` resolves it fail-closed.
    locally_revoked: bool
    expires_at: datetime | None = None
    revoked_at: datetime | None = None
    #: What this device's DURABLE record says about the server leg of the cut covering this grant.
    #: ``None`` when there is no local cut at all. ``False`` is the reportable state
    #: :class:`~mu_client.consent.tombstone.GrantTombstone`'s ``server_confirmed`` column exists
    #: for — read back here, on a LATER read, which is what makes that column's docstring true.
    local_cut_server_confirmed: bool | None = None
    #: When a BLANKET cut exists for this (room, agent) pair but does NOT cover this grant, the
    #: instant of that cut. Two very different things produce it and this device cannot tell them
    #: apart — see :meth:`render`.
    uncovering_blanket_cut_at: datetime | None = None

    # ---- the contract ------------------------------------------------------------------------
    exposed_shared: tuple[Capability, ...] = ()
    exposed_local: tuple[Capability, ...] = ()
    withheld_local: tuple[Capability, ...] = ()
    unrecognised: tuple[str, ...] = ()

    invariants_held: tuple[ExposureInvariant, ...] = ()
    invariants_broken: tuple[ExposureInvariant, ...] = ()
    disclosures: tuple[ExposureDisclosure, ...] = ALL_DISCLOSURES

    @computed_field  # type: ignore[prop-decorator]
    @property
    def effectively_live(self) -> bool:
        """Whether this device will treat the share as live. **Fail-closed by construction.**

        A local tombstone wins over a server that still says ACTIVE. That asymmetry is the point:
        the revoke path writes the tombstone BEFORE it calls the server, so a revoke whose server
        leg failed leaves this device reporting the share as withdrawn — never as live. The reverse
        (server revoked, device unaware) is already handled by ``server_active`` being ``False``.

        ⚠ It is a statement about what THIS DEVICE will display, **not** about what the agent can
        still do — the server is the authority on that, and :meth:`render` warns explicitly when
        the two disagree. A ``@computed_field`` rather than a bare ``@property`` so ``model_dump``
        carries the judgement to the daemon's IPC consumers rather than making each of them
        re-derive it (which is the drift this object exists to prevent).
        """
        return self.server_active and not self.locally_revoked

    def render(self) -> tuple[str, ...]:
        """The consent screen, as lines. **Every claim on it is derived, none is fixed.**

        Three branches are load-bearing and each is pinned by its own test:

        * the privacy sentence is printed only when BOTH invariants hold (module docstring,
          correction 2); when only the recognition invariant is broken the screen says it CANNOT
          say, rather than denying and admitting incompleteness in the same breath;
        * a local cut that the server has not confirmed, over a grant the server still reports
          ACTIVE, prints the warning that the agent **can still act** — the receipt-only version of
          that fact vanished when the process exited, which left the durable screen silent about
          the one survivor of a failed revoke;
        * a blanket cut that does not cover this grant is AMBIGUOUS (deliberate re-share vs. this
          device's clock disagreeing with the server's) and is reported as ambiguous.
        """
        lines: list[str] = []
        if self.locally_revoked:
            lines.append(
                "This device has WITHDRAWN this share. It will not be shown as live here again."
                if self.server_active
                else "This share is withdrawn."
            )
        elif not self.server_active:
            lines.append("This share is not live (it was revoked or has expired).")
        else:
            lines.append(f"Agent {self.agent_principal_id} is shared into room {self.room_id}.")

        lines.extend(self._survivor_lines())

        if self.exposed_shared:
            for capability in self.exposed_shared:
                lines.append(f"  EXPOSES (this room): {capability.name} — {capability.summary}")
        else:
            lines.append("  EXPOSES (this room): nothing this client recognises.")

        lines.extend(self._private_memory_lines())

        for capability in self.withheld_local:
            lines.append(f"  KEEPS PRIVATE: {capability.name} — {capability.summary}")

        if self.unrecognised:
            lines.append(
                "  ⚠ This share also grants permissions this client cannot explain, so the list "
                f"above is INCOMPLETE: {', '.join(self.unrecognised)}"
            )

        for disclosure in self.disclosures:
            lines.append(f"  NOTE: {_DISCLOSURE_TEXT[disclosure]}")
        return tuple(lines)

    def _survivor_lines(self) -> tuple[str, ...]:
        """What a local cut did NOT reach, on the PERSISTENT screen rather than a transient receipt.

        ``mu_client.consent.service``'s module docstring: *"a client revoke that silently succeeds
        when the server never heard it is the same failure wearing a green tick."* That was true of
        the receipt and false of this screen: an owner who revoked offline, closed the terminal and
        came back was told "WITHDRAWN" while the server still held the grant ACTIVE and the agent
        could still read and write in the room. The survivor of a failed revoke is the SERVER-SIDE
        GRANT, and it is named here.
        """
        lines: list[str] = []
        if self.locally_revoked and self.server_active:
            if self.local_cut_server_confirmed is False:
                lines.append(
                    "  ⚠ THE SERVER NEVER CONFIRMED YOUR REVOKE and still reports this share "
                    "ACTIVE — the agent can still act in this room. Revoke again."
                )
            else:
                lines.append(
                    "  ⚠ The server still reports this share ACTIVE — the agent can still act in "
                    "this room."
                )
        if self.uncovering_blanket_cut_at is not None:
            lines.append(
                "  ⚠ This device recorded a withdrawal for this agent at "
                f"{self.uncovering_blanket_cut_at.isoformat()}, but the server dates this grant "
                f"later ({self.issued_at.isoformat()}), so the withdrawal does not cover it. If "
                "you did NOT deliberately re-share this agent, this device's clock disagrees with "
                "the server's — revoke again."
            )
        return tuple(lines)

    def _private_memory_lines(self) -> tuple[str, ...]:
        """D4's *"It cannot see your private memory"*, or the reason it cannot be said."""
        if (
            ExposureInvariant.NO_LOCAL_CAPABILITY_EXPOSED in self.invariants_held
            and ExposureInvariant.EVERY_GRANTED_CAPABILITY_RECOGNISED in self.invariants_held
        ):
            return (
                "  It CANNOT see your private memory: this share grants none of the "
                f"{len(self.withheld_local)} things this device can do to it.",
            )
        lines: list[str] = []
        for capability in self.exposed_local:
            lines.append(
                f"  ⚠ EXPOSES YOUR PRIVATE MEMORY: {capability.name} — {capability.summary}"
            )
            if capability.offered_here is False:
                lines.append(
                    "      (this device does not offer that tool today, so the grant is inert "
                    "HERE — but it is conferred, and another host or a flag flip honours it)"
                )
        if not lines:
            # The recognition invariant is broken and nothing is classifiable as LOCAL. This client
            # does not know what plane the unrecognised names act on, so it says so instead of
            # denying an exposure it cannot rule out.
            lines.append(
                "  ⚠ This client CANNOT SAY whether this share reaches your private memory: it "
                "grants permissions it does not recognise (listed below)."
            )
        return tuple(lines)


def compute_exposure(
    grant: AgentShareGrantView,
    *,
    mcp: McpSettings,
    locally_revoked: bool,
    local_cut_server_confirmed: bool | None = None,
    uncovering_blanket_cut_at: datetime | None = None,
) -> AgentExposureContract:
    """Derive the contract for ``grant`` against THIS device's capability surface.

    ``mcp`` is the live :class:`~mu_client.config.McpSettings`, so the answer tracks the tools this
    host actually offers — turn ``MU_MCP__EXPOSE_PIN_TOOLS`` on and ``pin``/``unpin`` join the
    withheld set, because they became things this device can do.
    """
    known = known_capabilities(mcp)
    granted = frozenset(grant.capabilities)

    exposed_shared: list[Capability] = []
    exposed_local: list[Capability] = []
    for name in sorted(granted):
        capability = known.get(name)
        if capability is None:
            # Not explainable — but the NAMESPACE still decides the plane. A `memory.local.*` name
            # this build has never heard of is a private-memory permission whatever else it is.
            if is_local_capability_name(name):
                exposed_local.append(unexplained_local_capability(name))
            continue
        if capability.plane is CapabilityPlane.SHARED:
            exposed_shared.append(capability)
        else:
            exposed_local.append(capability)

    withheld_local = tuple(
        capability for capability in local_capabilities(mcp) if capability.name not in granted
    )
    unrecognised = tuple(sorted(name for name in granted if name not in known))

    held: list[ExposureInvariant] = []
    broken: list[ExposureInvariant] = []
    (held if not exposed_local else broken).append(ExposureInvariant.NO_LOCAL_CAPABILITY_EXPOSED)
    (held if not unrecognised else broken).append(
        ExposureInvariant.EVERY_GRANTED_CAPABILITY_RECOGNISED
    )

    return AgentExposureContract(
        grant_id=grant.grant_id,
        room_id=grant.room_id,
        agent_principal_id=grant.agent_principal_id,
        granted_by=grant.granted_by,
        issued_at=grant.issued_at,
        server_active=grant.active,
        locally_revoked=locally_revoked,
        expires_at=grant.expires_at,
        revoked_at=grant.revoked_at,
        local_cut_server_confirmed=local_cut_server_confirmed,
        uncovering_blanket_cut_at=uncovering_blanket_cut_at,
        exposed_shared=tuple(exposed_shared),
        exposed_local=tuple(exposed_local),
        withheld_local=withheld_local,
        unrecognised=unrecognised,
        invariants_held=tuple(held),
        invariants_broken=tuple(broken),
    )
