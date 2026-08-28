"""The ONE place an :class:`~mu_client.consent.service.AgentShareConsentService` is built.

DEV-STANDARDS rule 9 (dependency injection, composition roots): the CLI, the daemon's IPC front
door and the integration exercise all reach the consent surface through
:func:`open_consent_service`, so there is one place the httpx client, the tombstone store and the
settings are wired together — and one place their teardown is ordered.

The context manager shape is deliberate. The tombstone store holds a real sqlite connection and the
wire client holds a real connection pool; a consent verb that leaked either would leave a laptop
holding a file handle for a screen the owner closed.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator

import httpx

from mu_client.config import ClientSettings
from mu_client.consent.client import HttpAgentShareClient
from mu_client.consent.service import AgentShareConsentService
from mu_client.consent.tombstone import SqliteGrantTombstones
from mu_client.errors import SharedPlaneNotConfiguredError

__all__ = ["open_consent_service"]


@contextlib.asynccontextmanager
async def open_consent_service(
    settings: ClientSettings, *, http: httpx.AsyncClient | None = None
) -> AsyncIterator[AgentShareConsentService]:
    """Build the consent service over the real wire and the real local store.

    Raises :class:`~mu_client.errors.SharedPlaneNotConfiguredError` when no server is configured —
    loud and named, never an empty result (see :class:`~mu_client.config.ConsentSettings`).
    ``http`` lets an integration exercise bind a client pointed at a REAL running ``mu-server``.
    """
    if settings.consent.server_base_url is None:
        raise SharedPlaneNotConfiguredError
    tombstones = SqliteGrantTombstones(settings.consent.tombstone_db_path)
    await tombstones.open()
    wire = HttpAgentShareClient(settings.consent, http=http)
    try:
        yield AgentShareConsentService(wire=wire, tombstones=tombstones, settings=settings)
    finally:
        # Ordered teardown, network first: closing the sqlite handle while a request is still in
        # flight would leave the durable half of a revoke unwritable.
        await wire.aclose()
        await tombstones.aclose()
