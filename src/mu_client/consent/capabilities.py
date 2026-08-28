"""**What this device can do**, as a closed, inspectable vocabulary — the raw material of Decision
D4's *"exposes X / keeps Y private"* contract.

``SERVER-AND-COLLAB-DESIGN-REVIEW.md:118-121`` (D4 §4.2-A) gives the grant a
``capabilities: frozenset[str]`` and calls it *"what the shared agent may do — a **subset** of the
agent's full local capability"*. The server owns the grant; **only this device knows the set that
subset is taken from**, because only this device knows which tools its own MCP host actually
offers. Without that set, "keeps Y private" is unanswerable and the contract collapses into the
docstring D4 exists to replace.

--------------------------------------------------------------------------------------------
The two planes, and why the distinction is the whole point
--------------------------------------------------------------------------------------------
A capability is a **tool name** (``mu-server/src/mu_server/consent/model.py:72``: *"A capability is
a TOOL NAME (``room.participate``)"*), and the names come from two different planes:

* **SHARED** — what a bound agent may do *in the room*. Exactly one such name exists in the entire
  system today: ``room.participate`` (``mu-server/src/mu_server/agents/bridge.py:140``, the default
  of ``BindAgentBody.allowed_tools`` at ``routes/rooms.py:322``).
* **LOCAL** — what an agent may do to the owner's *private memory on this laptop*: the MCP tools
  :mod:`mu_client.mcp.server` registers, under the ``memory.local.`` prefix.

D4's rendered sentence — *"It cannot see your private memory"* — is TRUE exactly when the grant's
capability set contains no LOCAL-plane name. That is a computation over these two vocabularies, and
:mod:`mu_client.consent.exposure` performs it rather than asserting it. A grant naming
``memory.local.recall`` would make the sentence a lie, and the point of computing it is that the
client then says something else.

--------------------------------------------------------------------------------------------
Naming: why ``memory.local.<tool>`` and not the bare tool name
--------------------------------------------------------------------------------------------
The design set's own MCP names are dotted — ``memory.local.health`` / ``memory.local.status``
(``memory-health-pinning-spec.md`` §7.1:332), ``memory.pin`` / ``memory.unpin`` (§7.2:340) — while
this host registers FLAT names (``health``, ``pin``) per AD-22, because an MCP host prefixes tools
with the server id itself. A *capability* is not a tool invocation, it is a name in a shared
namespace that a server-side grant can carry, so it takes the dotted design-set form. The mapping
is one prefix and is applied in one place (:func:`local_capability_name`); nothing here re-lists
the tools, so a tool added to the MCP surface appears here automatically.

**Content-free (rule 3).** Every value in this module is a tool name or a fixed English summary of
a verb. No namespace, no memory id, no memory text ever reaches a ``Capability``.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Final

from pydantic import BaseModel, ConfigDict, Field

from mu_client.config import McpSettings
from mu_client.mcp.surface import REGISTERED_TOOL_NAMES, offered_tool_names

__all__ = [
    "LOCAL_CAPABILITY_PREFIX",
    "SHARED_CAPABILITIES",
    "TOOL_SUMMARIES",
    "Capability",
    "CapabilityPlane",
    "all_local_capabilities",
    "is_local_capability_name",
    "known_capabilities",
    "local_capabilities",
    "local_capability_name",
    "unexplained_local_capability",
]

#: The dotted namespace every LOCAL-plane capability lives in. See the module docstring for why the
#: capability name is not the flat MCP tool name.
LOCAL_CAPABILITY_PREFIX: Final = "memory.local."


class CapabilityPlane(StrEnum):
    """Which plane a capability acts on — the axis D4's privacy sentence turns on."""

    #: The owner's private memory, on this device. CANONICAL §4.1: *"LOCAL (private, on the
    #: member's machine)"*. Nothing here ever leaves the laptop.
    LOCAL = "local"
    #: The room, on the team server. A bound agent acts *"under its own principal in the room's
    #: shared partition only"* (D4 §4.2-A).
    SHARED = "shared"


class Capability(BaseModel):
    """One named thing an agent could be permitted to do, and where it acts.

    Frozen: a capability record is a fact about this build, not a mutable policy object.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    #: The wire name a grant's ``capabilities`` set would carry.
    name: str = Field(min_length=1, max_length=128)
    plane: CapabilityPlane
    #: Where this device implements it — for a human reading the consent screen.
    surface: str = Field(min_length=1, max_length=64)
    #: One content-free sentence. Describes the VERB, never any data it would touch.
    summary: str = Field(min_length=1, max_length=200)
    #: Whether THIS device currently offers the tool behind a LOCAL-plane capability.
    #:
    #: ``False`` when the MCP tool is withdrawn by an ``MU_MCP__EXPOSE_*`` flag. **A withdrawn tool
    #: does not make the capability harmless:** the grant still CONFERS it, and another host, a
    #: later build, or the same laptop with the flag flipped will honour it. So a withdrawn tool
    #: still counts as LOCAL-plane exposure and still breaks
    #: :attr:`~mu_client.consent.exposure.ExposureInvariant.NO_LOCAL_CAPABILITY_EXPOSED`; the flag
    #: only changes the *note* the consent screen adds under it.
    #:
    #: ``None`` when the question does not apply: a SHARED-plane capability is implemented by the
    #: ROOM, not by this device, and a LOCAL-plane name this build has never heard of has no tool
    #: here to be offered or withdrawn.
    offered_here: bool | None = None


#: One summary per registered MCP tool. A tool with no row here has no capability, and therefore
#: could be silently omitted from the "keeps private" answer — so
#: ``tests/unit/test_consent_capabilities_unit.py`` asserts this covers ``REGISTERED_TOOL_NAMES``
#: exactly, and the suite goes RED when a tool is added without one.
TOOL_SUMMARIES: Final[dict[str, str]] = {
    "add": "write a new memory into your private store",
    "recall": "search your private memory and read the matching entries",
    "search": "search your private memory and read the matching entries",
    "get": "read one of your private memories by id",
    "build_context": "assemble a large context window out of your private memory",
    "ask": "get a synthesised answer written over your private memory",
    "consolidate": "run distillation over your private short-term memory",
    "promote": "move one of your memories up a tier",
    "demote": "move one of your memories down a tier",
    "update": "rewrite one of your private memories",
    "delete": "retract one of your private memories",
    "health": "read the health lens over your whole private memory",
    "pin": "pin one of your memories so the lifecycle never forgets it",
    "unpin": "remove a pin you set",
}

#: The SHARED-plane vocabulary this client recognises. Exactly one name exists in the system
#: (``mu-server/src/mu_server/agents/bridge.py:140``); listing a second one we had not verified
#: would make :attr:`~mu_client.consent.exposure.AgentExposureContract.unrecognised` under-report,
#: which is the failure mode this whole module is built to avoid.
SHARED_CAPABILITIES: Final[tuple[Capability, ...]] = (
    Capability(
        name="room.participate",
        plane=CapabilityPlane.SHARED,
        surface="room",
        summary="read and write messages in this room, under its own name",
    ),
)


def local_capability_name(tool_name: str) -> str:
    """The capability name for an MCP tool. One prefix, applied in one place."""
    return f"{LOCAL_CAPABILITY_PREFIX}{tool_name}"


def is_local_capability_name(name: str) -> bool:
    """Whether ``name`` is a LOCAL-plane capability **by NAMESPACE**.

    ⚠ **This is the classifier, and it is deliberately not "is it a tool I currently offer".**
    Classifying by the offered set was a real bug: with the default configuration seven of the
    fourteen registered tools are withdrawn, so a grant naming ``memory.local.add`` fell out of the
    known vocabulary entirely, ``exposed_local`` stayed empty, ``NO_LOCAL_CAPABILITY_EXPOSED``
    reported HELD, and the consent screen printed *"It CANNOT see your private memory"* for a grant
    that confers a write into it. The plane is a property of the NAME; whether this build happens to
    offer the tool today is a property of the build.
    """
    return name.startswith(LOCAL_CAPABILITY_PREFIX)


def unexplained_local_capability(name: str) -> Capability:
    """A ``memory.local.*`` name this build has no summary for — classified, but not explained.

    It is LOCAL-plane (the namespace says so) and it is ALSO
    :attr:`~mu_client.consent.exposure.AgentExposureContract.unrecognised` (this build cannot say
    what it does). Both facts are reported; neither is used to suppress the other.
    """
    return Capability(
        name=name,
        plane=CapabilityPlane.LOCAL,
        surface="unknown",
        summary="a permission over your private memory that this client cannot explain",
        offered_here=None,
    )


def all_local_capabilities(mcp: McpSettings) -> tuple[Capability, ...]:
    """Every LOCAL-plane capability this build has a NAME for — offered or withdrawn.

    This is the CLASSIFICATION vocabulary. It spans ``REGISTERED_TOOL_NAMES``, not
    ``offered_tool_names``, because a grant that names a withdrawn tool is still a grant over
    private memory (see :func:`is_local_capability_name`).
    """
    offered = offered_tool_names(mcp)
    return tuple(
        Capability(
            name=local_capability_name(tool_name),
            plane=CapabilityPlane.LOCAL,
            surface="mcp-tool",
            summary=TOOL_SUMMARIES[tool_name],
            offered_here=tool_name in offered,
        )
        for tool_name in sorted(REGISTERED_TOOL_NAMES)
    )


def local_capabilities(mcp: McpSettings) -> tuple[Capability, ...]:
    """This device's OFFERED LOCAL-plane capability set under ``mcp`` — the "Y" in "keeps Y
    private".

    Derived from :func:`mu_client.mcp.surface.offered_tool_names`, i.e. from the SAME policy
    ``build_server`` withdraws tools by. A tool the model cannot call is not a capability this
    device *has*, so withdrawing ``add`` genuinely shrinks the set an owner is told is withheld —
    which is the honest answer, not a conservative one.

    ⚠ This is the "keeps private" set ONLY. It is **not** the classifier: see
    :func:`all_local_capabilities` and :func:`is_local_capability_name` for why using it as one
    printed a privacy guarantee that was false.
    """
    return tuple(
        capability for capability in all_local_capabilities(mcp) if capability.offered_here
    )


def known_capabilities(mcp: McpSettings) -> dict[str, Capability]:
    """Every capability name this client can EXPLAIN, keyed by name.

    Spans EVERY registered local tool (offered or withdrawn) plus the shared vocabulary — because
    "can I explain this name?" and "do I offer this tool today?" are different questions, and
    answering the first with the second is what made the privacy sentence false.

    A grant naming something outside this mapping is not silently ignored: it lands in
    :attr:`~mu_client.consent.exposure.AgentExposureContract.unrecognised` and breaks the
    ``EVERY_GRANTED_CAPABILITY_IS_RECOGNISED`` invariant, because a consent screen that drops a
    permission it does not understand is worse than one that admits it does not understand it.
    """
    known = {capability.name: capability for capability in all_local_capabilities(mcp)}
    known.update({capability.name: capability for capability in SHARED_CAPABILITIES})
    return known
