"""Decision D4's **client-side agent-sharing consent object**
(``SERVER-AND-COLLAB-DESIGN-REVIEW.md`` :95 and §4.2-A at :118-124).

D4 chose option (b) — a first-class client consent object — over option (a) — *"treat bind/unbind as
sufficient"* — for one reason: an owner must be able to **see what sharing their agent exposes
before they consent, and revoke it afterwards.** This package is that, and no more than that.

======================================  ======================================================
Module                                  What it owns
======================================  ======================================================
:mod:`~mu_client.consent.capabilities`  What this device can do — the set "keeps Y private" is
                                        taken from. Derived from the live MCP surface policy.
:mod:`~mu_client.consent.exposure`      The exposes-vs-keeps-private contract, **computed**, with
                                        the two invariants D4's privacy sentence depends on.
:mod:`~mu_client.consent.wire`          The four server shapes ``mu-core`` has no home for.
:mod:`~mu_client.consent.client`        The two D4 REST routes over httpx.
:mod:`~mu_client.consent.tombstone`     The durable local withdrawal a revoke actually reaches.
:mod:`~mu_client.consent.residue`       What a revoke did NOT reach, named and translated.
:mod:`~mu_client.consent.service`       The two verbs, and the consent-first ordering.
:mod:`~mu_client.consent.composition`   The one wiring point.
======================================  ======================================================

**What is NOT here, and why — read this before adding to it.** There is no room runtime: no
``RoomClientPort`` implementation, no Centrifugo listener, no ``revoke_signal`` handler, no
``BoundAgentSupervisor``, no ``HostBridgePort``. Each is unreachable rather than merely unbuilt —
``GET /v1/stream/token`` does not exist (AD-35), so a control-frame handler here would be a call
site that never fires. :class:`~mu_client.consent.residue.ClientCascadeResidue` names each absence
on every revoke receipt this device produces, which is the honest alternative to a stub.
"""

from __future__ import annotations

from mu_client.consent.capabilities import Capability, CapabilityPlane
from mu_client.consent.composition import open_consent_service
from mu_client.consent.exposure import (
    AgentExposureContract,
    ExposureDisclosure,
    ExposureInvariant,
    compute_exposure,
)
from mu_client.consent.residue import ClientCascadeResidue, ResidueExplanation
from mu_client.consent.service import (
    AgentShareConsentService,
    AgentShareStatus,
    ClientRevocationOutcome,
)
from mu_client.consent.wire import AgentShareGrantView, RevocationReceiptView

__all__ = [
    "AgentExposureContract",
    "AgentShareConsentService",
    "AgentShareGrantView",
    "AgentShareStatus",
    "Capability",
    "CapabilityPlane",
    "ClientCascadeResidue",
    "ClientRevocationOutcome",
    "ExposureDisclosure",
    "ExposureInvariant",
    "ResidueExplanation",
    "RevocationReceiptView",
    "compute_exposure",
    "open_consent_service",
]
