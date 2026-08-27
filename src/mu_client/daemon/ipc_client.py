"""``IpcClient`` — the CLI's front door onto the resident daemon's unix socket.

``memory-health-pinning-spec.md`` §7.1/§7.2 put ``mu health`` / ``mu pin <id>`` / ``mu unpin <id>``
on **the daemon loopback IPC**, not on the daemonless one-shot path ``mu add|recall|search`` uses.
That is the right call for these three and not an arbitrary difference: the health lens and the pin
bound are per-PARTITION facts the resident daemon already holds warm, and a pin is a mutation whose
``PrivateDelta`` the daemon's sync client is the eventual owner of (§7.2) — spinning up a second,
short-lived engine to answer them would answer from a different process's view of the same stores.

**Why this is not ``capture.hook._rpc``.** That helper deliberately swallows every failure into
``{}``/``None``, because ``capture_once`` is holding a record it must not lose and its correct
response to any refusal is "spool it myself". These verbs hold nothing and have no second front
door, so the opposite contract is the honest one: a daemon that is not running is a loud, typed
:class:`~mu_client.errors.DaemonUnreachableError`, never a half-answer. Two call sites, two
genuinely different error contracts — recorded here rather than collapsed into one helper that
would have to be right for both.

Content-free: this module logs nothing at all, and the only thing it ever puts in an exception
message is the socket path.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from mu_client.config import DaemonIpcSettings
from mu_client.errors import DaemonUnreachableError

__all__ = ["IpcClient"]


class IpcClient:
    """One newline-delimited-JSON round trip per :meth:`request`, connection closed after."""

    def __init__(self, settings: DaemonIpcSettings) -> None:
        self._settings = settings

    async def request(self, route: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Send ``{"route": route, **payload}`` and return the parsed reply.

        ``timeout_s`` bounds the WHOLE exchange (write + drain + read), not just the read — a
        daemon that stopped reading mid-drain must not hang the CLI forever.
        """
        socket_path = self._settings.socket_path.expanduser()
        timeout_s = self._settings.request_io_timeout_s
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_unix_connection(
                    path=str(socket_path), limit=self._settings.max_request_bytes
                ),
                timeout=timeout_s,
            )
        except (OSError, TimeoutError) as exc:
            raise DaemonUnreachableError(
                f"could not connect to the daemon at {socket_path} ({type(exc).__name__}) — "
                "start it with 'mu daemon run'"
            ) from exc
        try:
            return await asyncio.wait_for(
                self._exchange(reader, writer, {"route": route, **payload}), timeout=timeout_s
            )
        except (OSError, TimeoutError, ValueError) as exc:
            # ValueError covers an unparseable reply and a reply past our own read limit; OSError a
            # daemon that hung up mid-exchange; TimeoutError one that never answered. All three are
            # "this verb did not happen", which is a refusal, not a result.
            raise DaemonUnreachableError(
                f"the daemon at {socket_path} did not answer ({type(exc).__name__})"
            ) from exc
        finally:
            writer.close()

    async def _exchange(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        writer.write((json.dumps(payload) + "\n").encode("utf-8"))
        await writer.drain()
        line = await reader.readline()
        if not line:
            raise ValueError("empty reply")
        parsed = json.loads(line)
        if not isinstance(parsed, dict):
            raise ValueError("reply was not a JSON object")
        return parsed
