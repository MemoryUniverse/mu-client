"""**What a revoke did not reach** — named on this side too, and translated for a human.

Two obligations meet in this module.

**Obligation 1 — render the server's receipt honestly.** ``mu-server``'s revoke route states it as
a client requirement in so many words (``routes/rooms.py:912-916``):

    *"``state`` is ``PARTIAL`` in this build by construction — there is no ``revoke_ack`` intake, so
    no confirmation can land. A client that renders 'revoked everywhere' from a ``PARTIAL`` receipt
    is misreporting it."*

The server hands over a closed vocabulary of NAMED residue
(``mu-server/src/mu_server/consent/cascade_names.py:46-135``) precisely so a client can say what was
missed without inventing prose. :func:`explain` is that translation, and it refuses to drop a name
it does not recognise: an unknown residue is still *something a revoke did not reach*, so it renders
as unrecognised rather than vanishing.

**Obligation 2 — name what THIS side could not reach.** D4 §4.2-A gives the daemon its own leg of
the cascade: consume ``revoke_signal``, stop feeding the local subprocess, purge warm-cache entries,
emit ``revoke_ack``. Almost none of that is reachable from this build, and the honest thing — the
thing the server lane did rather than claim a clean cut — is to enumerate it as a closed vocabulary.
:class:`ClientCascadeResidue` is that enumeration. Every member cites the fact that makes it true,
and each was verified in source rather than assumed.

**Content-free (rule 3).** Every string here is a fixed English sentence about a *mechanism*. No
memory content, no namespace, no room body, no id.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Final

from pydantic import BaseModel, ConfigDict, Field

from mu_client.consent.wire import ServerCascadeResidue

__all__ = [
    "ClientCascadeResidue",
    "ResidueExplanation",
    "explain",
    "explain_all",
]


class ClientCascadeResidue(StrEnum):
    """What a revoke initiated on THIS device did not reach. Each member cites its evidence."""

    #: The revoke's server leg did not complete — transport failure, timeout, or a non-2xx answer.
    #: The LOCAL cut still stands (:mod:`mu_client.consent.service` writes the tombstone BEFORE it
    #: calls the server, mirroring ``mu-server/src/mu_server/agents/bridge.py:519-526``'s
    #: consent-first ordering), but the server-side consent record may still be ACTIVE and the
    #: agent may still be able to act in the room. This is the residue that must never be silent.
    SERVER_REVOKE_NOT_CONFIRMED = "server_revoke_not_confirmed"

    #: CANONICAL-CONTRACTS.md:255 (§4.3-B5) makes ``HostBridgePort`` daemon-local, and D4's
    #: correction of 2026-08-27 (AD-61) states that what a revoke cancels is the *accounting* and
    #: the *admission*, never the ``claude -p`` / ``codex exec`` process. On THIS device the
    #: statement is stronger still: ``HostBridgePort`` has no declaration and no implementation in
    #: any repo, and mu-client drives no agent subprocess at all — it captures FROM ``claude`` /
    #: ``codex`` via hooks and never runs them. So there is nothing here for a revoke to stop, and
    #: saying so is not the same as saying the cut was clean.
    LOCAL_SUBPROCESS_NOT_DRIVEN_HERE = "local_subprocess_not_driven_here"

    #: CANONICAL-CONTRACTS.md:259 (§4.3-B7) requires the daemon to *"return ``revoke_ack`` via
    #: ``POST`` on the sync channel"*. This client emits none: there is no ``revoke_ack`` intake
    #: route on the server either (the server names the same gap as
    #: ``REVOKE_ACK_NOT_INTAKEN``), so the ack has nowhere to land. Reported from both ends rather
    #: than from neither.
    REVOKE_ACK_NOT_EMITTED = "revoke_ack_not_emitted"

    #: CANONICAL-CONTRACTS.md:244 puts ``revoke_signal`` on the ``personal:{principal_id}``
    #: Centrifugo channel, and §4.3-B7 makes the daemon its consumer. This daemon has no Centrifugo
    #: connection (zero hits for any Centrifugo client in ``src/``) and could not authorize one if
    #: it did: ``GET /v1/stream/token`` is unbuilt (AD-35). **The consequence an owner has to be
    #: told:** a revoke performed on ANOTHER device, or by another owner, is not heard here — this
    #: device's cut happens only when the revoke is initiated here.
    CONTROL_FRAME_NOT_CONSUMED = "control_frame_not_consumed"

    #: CANONICAL-CONTRACTS.md:676-678 (X10) makes the ``revoke_signal`` handler *"a cache PURGE of
    #: every entry whose source grant is named in the frame"*. This device's only warm cache is
    #: ``inject/recall_bridge.py``'s ``_ScopedRenderCache``, and it is keyed by SESSION, not by
    #: source grant — there is no grant-scoped purge primitive to call. In this build's favour and
    #: NOT as a security property: nothing in that cache derives from a shared grant, because no
    #: shared recall path exists on this device. That is build order, not containment.
    WARM_CACHE_NOT_GRANT_SCOPED = "warm_cache_not_grant_scoped"

    #: BY DESIGN, and named so it is never mistaken for a gap. D4 §4.2-A: the agent's already-posted
    #: ``AGENT_RESULT`` messages **stay** — *"invalidate-don't-delete; they were legitimately
    #: shared"*. The server names the identical fact as ``POSTED_AGENT_RESULTS_RETAINED``; it is
    #: repeated here because a client that revoked and showed nothing about retained output would
    #: leave an owner believing the agent's contributions were withdrawn from the room.
    POSTED_AGENT_RESULTS_RETAINED = "posted_agent_results_retained"


class ResidueExplanation(BaseModel):
    """One residue, in terms an owner can act on."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    #: The raw name off the wire (or a :class:`ClientCascadeResidue` value). Kept verbatim so an
    #: operator can grep the server's ledger for it.
    name: str = Field(min_length=1)
    #: ``True`` when the residue describes something the design INTENDS to survive a revoke, rather
    #: than something this build cannot reach. The distinction changes what an owner should do:
    #: nothing, versus understand a bounded exposure window.
    by_design: bool
    #: One content-free sentence about the mechanism.
    text: str = Field(min_length=1)
    #: ``False`` when this client has no explanation for the name — see :func:`explain`.
    recognised: bool = True


_SERVER_TEXT: Final[dict[str, tuple[bool, str]]] = {
    ServerCascadeResidue.IN_FLIGHT_RESULT_MID_APPEND: (
        False,
        "A result the agent was already writing when you revoked won the race and is now a "
        "permanent message in the room.",
    ),
    ServerCascadeResidue.CONTROL_FRAME_NOT_DURABLE: (
        False,
        "The revoke notification to the agent's own device is best-effort: a device that was "
        "offline when you revoked will never be told by that notification.",
    ),
    ServerCascadeResidue.CONTROL_FRAME_NOT_PUBLISHED: (
        False,
        "No push tier is configured on the server, so no revoke notification was sent to the "
        "agent's device at all.",
    ),
    ServerCascadeResidue.PUBLISHED_FRAMES_RETAINED_IN_BROKER: (
        False,
        "Notifications already sitting in the message broker stay there until they expire. They "
        "carry ids only — reading any message body still needs access the revoke has cut.",
    ),
    ServerCascadeResidue.AUTHORIZED_IDS_RESTAMP_UNBUILT: (
        False,
        "The revoke cut the agent's access and its ability to post, but did not re-stamp search "
        "indexes, so it cannot yet cut findability on this path.",
    ),
    ServerCascadeResidue.WARM_CACHE_PURGE_UNBUILT: (
        False,
        "The server's own warm cache is not purged on revoke; entries age out on their TTL "
        "instead.",
    ),
    ServerCascadeResidue.DAEMON_SUBPROCESS_NOT_CANCELLABLE: (
        False,
        "The agent's process on its owner's own machine is not stopped by this revoke — the "
        "server has no connection into a private device. What is cut is its permission to act "
        "and the admission of anything it sends next.",
    ),
    ServerCascadeResidue.REVOKE_ACK_NOT_INTAKEN: (
        False,
        "No device can confirm it acted on this revoke, because the server has no route to "
        "receive such a confirmation. That is why the receipt says PARTIAL and not SETTLED.",
    ),
    ServerCascadeResidue.POSTED_AGENT_RESULTS_RETAINED: (
        True,
        "Messages the agent already posted stay in the room. They were legitimately shared, and "
        "the room's history is never rewritten.",
    ),
}

_CLIENT_TEXT: Final[dict[str, tuple[bool, str]]] = {
    ClientCascadeResidue.SERVER_REVOKE_NOT_CONFIRMED: (
        False,
        "This device has withdrawn the share, but the server did not confirm it. The agent may "
        "still be able to act in the room. Retry the revoke when the server is reachable.",
    ),
    ClientCascadeResidue.LOCAL_SUBPROCESS_NOT_DRIVEN_HERE: (
        False,
        "This device does not run the shared agent's process, so there was no local process for "
        "the revoke to stop.",
    ),
    ClientCascadeResidue.REVOKE_ACK_NOT_EMITTED: (
        False,
        "This device sent no confirmation of the revoke, because the server has no route to "
        "receive one.",
    ),
    ClientCascadeResidue.CONTROL_FRAME_NOT_CONSUMED: (
        False,
        "This device does not listen for revokes made elsewhere. A revoke performed on another "
        "device or by another owner will not be reflected here.",
    ),
    ClientCascadeResidue.WARM_CACHE_NOT_GRANT_SCOPED: (
        False,
        "This device's warm cache cannot be purged per share, because its entries are not "
        "recorded against one. No entry in it comes from a shared room today.",
    ),
    ClientCascadeResidue.POSTED_AGENT_RESULTS_RETAINED: (
        True,
        "Messages the agent already posted stay in the room. They were legitimately shared, and "
        "the room's history is never rewritten.",
    ),
}


def explain(name: str) -> ResidueExplanation:
    """Translate one residue name. **Never returns ``None`` and never drops a name.**

    An unrecognised name means the server is newer than this client and named something this build
    has no sentence for. Reporting it as unrecognised is the only honest option: silently omitting
    it turns a partial revoke into a clean-looking one, which is the precise misreport
    ``routes/rooms.py:912-916`` forbids.
    """
    known = _CLIENT_TEXT.get(name) or _SERVER_TEXT.get(name)
    if known is None:
        return ResidueExplanation(
            name=name,
            by_design=False,
            recognised=False,
            text=(
                "This server reported something the revoke did not reach that this version of the "
                "client does not recognise. Treat it as NOT reached and update the client."
            ),
        )
    by_design, text = known
    return ResidueExplanation(name=name, by_design=by_design, text=text)


def explain_all(names: tuple[str, ...]) -> tuple[ResidueExplanation, ...]:
    """Translate a residue list, preserving order and duplicates-free-ness of the source."""
    seen: set[str] = set()
    out: list[ResidueExplanation] = []
    for name in names:
        if name in seen:
            continue
        seen.add(name)
        out.append(explain(name))
    return tuple(out)
