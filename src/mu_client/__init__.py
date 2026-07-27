"""``mu-client`` — the on-device LOCAL client (daemon-app-skeleton-spec.md).

Public surface this stage (foundation + daemonless — the daemon is a later stage):

* :class:`~mu_client.config.ClientSettings` — the one client-side env boundary.
* :class:`~mu_client.host.LocalMemoryHost` — hosts mu-local's ``LocalMemory`` with a clean async
  lifecycle (``start``/``aclose``, cancellation-safe) + observability + error mapping.
* :func:`~mu_client.host.daemonless_host` — the one-shot context manager the CLI and any
  programmatic caller use: construct the host, do the op, tear down, NO daemon required.
* ``mu add|recall|search`` — the daemonless CLI (:mod:`mu_client.cli`).

Depends on ``mu-core`` ONLY (``mu_contracts`` + ``mu_engine`` + ``mu_local``) — NEVER ``mu_server``
(import-linter ``client-has-no-server`` + the grep backstop, see ``.importlinter`` /
``tests/unit/test_import_boundaries.py``).
"""

from __future__ import annotations

from mu_client.config import ClientSettings, ModelProfileSettings
from mu_client.errors import ClientError, ClientNotStartedError
from mu_client.host import LocalMemoryHost, daemonless_host

__all__ = [
    "ClientError",
    "ClientNotStartedError",
    "ClientSettings",
    "LocalMemoryHost",
    "ModelProfileSettings",
    "daemonless_host",
]
