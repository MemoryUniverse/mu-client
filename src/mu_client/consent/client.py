"""``HttpAgentShareClient`` — the REST leg of Decision D4's client half.

Exactly two routes, both of which ship on ``mu-server`` today:

* ``GET  /v1/rooms/{room_id}/agent-share/{agent_principal_id}`` (``routes/rooms.py:836``) — the
  persistent *"your agent is shared here"* affordance (D4 §4.2-D step 4).
* ``POST /v1/rooms/{room_id}/agent-share/revoke`` (``routes/rooms.py:888``) — the one-tap revoke.

--------------------------------------------------------------------------------------------
Why this file is httpx and not ``mu-sdk``
--------------------------------------------------------------------------------------------
``mu-client/CLAUDE.md`` says *"all server interaction is via ``mu-sdk`` over the wire contract"*,
and the root ``CLAUDE.md`` says mu-client *"depends on mu-core ONLY"*. Those two sentences are in
conflict as literally written, because ``mu-sdk`` is **not in mu-core** — it is a fourth sibling
repo at ``mu_project/mu-sdk-python``. Taking it would be taking a fourth repo, and it would buy
nothing here: ``mu_sdk.MemoryClient`` wraps ``/v1/memories/*`` and ``/v1/context/*`` and has
**zero** room, bind, consent or trust surface (grep for
``rooms|bind|agent-share|consent|grant`` over ``mu-sdk-python/src`` returns two unrelated hits).
So the SDK cannot speak these two routes at all.

An owner ruling is REPORTED as a delta. Until it lands, this client owns a ~90-line httpx wrapper,
which keeps ``lint-imports``' ``client-has-no-server`` contract trivially clean and mirrors the
SDK's own ``HttpxTransport`` shape rather than inventing a second idiom.

--------------------------------------------------------------------------------------------
What it refuses to do
--------------------------------------------------------------------------------------------
* **No retries.** A revoke is not idempotent-safe to blind-retry from here: the server answers 200
  with a receipt for a live grant and **204 for one that was already withdrawn**
  (``routes/rooms.py:895-905``), and those two answers mean different things to an owner. Retry is
  the caller's decision, made with the residue in hand.
* **No response bodies in logs or errors.** :class:`~mu_client.errors.SharedPlaneUnreachableError`
  carries an operation name and a status code. Nothing else crosses.
* **No token anywhere but the header.** It never reaches a log line, an error message, or a URL.
"""

from __future__ import annotations

from types import TracebackType
from typing import Any, Final, Protocol

import httpx
import structlog

from mu_client.config import ConsentSettings
from mu_client.consent.wire import (
    AgentShareGrantView,
    RevocationReceiptView,
    RevokeAgentShareBody,
)
from mu_client.errors import SharedPlaneNotConfiguredError, SharedPlaneUnreachableError

__all__ = ["AgentSharePort", "HttpAgentShareClient"]

_log = structlog.get_logger("mu.client.consent")

#: ``routes/rooms.py:895-905``: 200 carries a receipt, 204 means "nothing live to withdraw" and is
#: NOT an error — *"a revoke of an already-revoked grant is idempotent and must not be an error,
#: but it must also not hand back a receipt implying a second cascade ran."*
_NO_CONTENT: Final = 204


class AgentSharePort(Protocol):
    """The narrow view :class:`~mu_client.consent.service.AgentShareConsentService` binds.

    A Protocol so the service is testable against a recording double without a network, while every
    integration exercise binds :class:`HttpAgentShareClient` against a REAL running ``mu-server``
    (DEV-STANDARDS: mocks ONLY in pure unit tests).
    """

    async def get_grant(
        self, *, room_id: str, agent_principal_id: str
    ) -> AgentShareGrantView | None: ...

    async def revoke(
        self, *, room_id: str, agent_principal_id: str, reason: str | None
    ) -> RevocationReceiptView | None: ...


class HttpAgentShareClient:
    """httpx implementation of :class:`AgentSharePort`.

    Owns its ``AsyncClient`` unless one is injected (integration tests inject a client bound to a
    real running server; nothing injects a transport double).
    """

    def __init__(self, settings: ConsentSettings, *, http: httpx.AsyncClient | None = None) -> None:
        if settings.server_base_url is None:
            raise SharedPlaneNotConfiguredError
        self._settings = settings
        self._base_url = settings.server_base_url.rstrip("/")
        self._owned = http is None
        self._http = http or httpx.AsyncClient(timeout=settings.request_timeout_s)

    async def __aenter__(self) -> HttpAgentShareClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Close only what this object opened — an injected client belongs to its owner."""
        if self._owned:
            await self._http.aclose()

    def _headers(self) -> dict[str, str]:
        token = self._settings.api_token
        if token is None:
            return {}
        return {"Authorization": f"Bearer {token.get_secret_value()}"}

    async def _request(
        self, method: str, path: str, *, operation: str, **kw: Any
    ) -> httpx.Response:
        try:
            return await self._http.request(
                method,
                f"{self._base_url}{path}",
                headers=self._headers(),
                timeout=self._settings.request_timeout_s,
                **kw,
            )
        except httpx.HTTPError as exc:
            # Content-free: the exception TYPE and the operation name, never the URL (it carries
            # a room id and an agent principal id) and never the body.
            _log.info("consent.transport_failed", operation=operation, error=type(exc).__name__)
            raise SharedPlaneUnreachableError(operation=operation) from exc

    async def get_grant(
        self, *, room_id: str, agent_principal_id: str
    ) -> AgentShareGrantView | None:
        """Read the consent grant, or ``None`` when the server answers its non-enumerating 404.

        ``routes/rooms.py:854-856``: absent and not-yours are ONE answer, deliberately, *"because a
        room's agent roster is a fact only a member may probe."* This client preserves that
        collapse instead of guessing which of the two it was.
        """
        response = await self._request(
            "GET",
            f"/v1/rooms/{room_id}/agent-share/{agent_principal_id}",
            operation="agent-share status",
        )
        if response.status_code == httpx.codes.NOT_FOUND:
            return None
        if response.status_code != httpx.codes.OK:
            raise SharedPlaneUnreachableError(
                operation="agent-share status", status_code=response.status_code
            )
        return AgentShareGrantView.model_validate(response.json())

    async def revoke(
        self, *, room_id: str, agent_principal_id: str, reason: str | None
    ) -> RevocationReceiptView | None:
        """Withdraw consent server-side. ``None`` means 204 — there was nothing live to withdraw.

        The 204 is not silently upgraded into a synthetic receipt: a receipt is evidence of an
        event, and no event occurred (``routes/rooms.py:895-901``).
        """
        body = RevokeAgentShareBody(agent_principal_id=agent_principal_id, reason=reason)
        response = await self._request(
            "POST",
            f"/v1/rooms/{room_id}/agent-share/revoke",
            operation="agent-share revoke",
            json=body.model_dump(mode="json"),
        )
        if response.status_code == _NO_CONTENT:
            return None
        if response.status_code != httpx.codes.OK:
            raise SharedPlaneUnreachableError(
                operation="agent-share revoke", status_code=response.status_code
            )
        return RevocationReceiptView.of(response.json())
