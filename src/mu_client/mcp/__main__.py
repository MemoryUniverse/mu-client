"""``mu-mcp`` / ``python -m mu_client.mcp`` — run the local-plane MCP server over stdio.

The stdio transport is what a Claude Code / Codex / generic MCP client spawns and speaks JSON-RPC
to (see ``.mcp.json``). This entrypoint owns ONLY process wiring — the server, its tools, and the
engine lifecycle all live in :func:`mu_client.mcp.server.build_server`.

STDIO HYGIENE (gap C): a stdio MCP server's STDOUT is the JSON-RPC channel and MUST carry nothing
else. structlog's default logger factory writes to STDOUT, and stdlib ``logging`` defaults to a
STDERR handler only once ``basicConfig`` runs — either can corrupt the JSON-RPC stream. Before the
server (and its engine lifespan, which logs ``host.start.*``) runs, :func:`_configure_stdio_logging`
pins BOTH structlog and stdlib logging to STDERR, so STDOUT stays pure JSON-RPC.
"""

from __future__ import annotations

import logging
import sys

import structlog

from mu_client.mcp.server import build_server

__all__ = ["main"]


def _configure_stdio_logging() -> None:
    """Route ALL diagnostic output to STDERR so STDOUT stays pure JSON-RPC (gap C).

    * structlog — its default ``PrintLoggerFactory`` targets ``sys.stdout``; repoint it at
      ``sys.stderr`` (leaving structlog's other defaults untouched). Every ``structlog.get_logger``
      in the engine/host/daemon inherits this.
    * stdlib ``logging`` — install a single STDERR ``StreamHandler`` at the root via
      ``basicConfig(..., force=True)`` so any library logging (the ``mcp``/anyio stack's benign
      ``Internal Server Error`` init noise included) lands on STDERR, never STDOUT.
    """
    logging.basicConfig(stream=sys.stderr, level=logging.WARNING, force=True)
    structlog.configure(logger_factory=structlog.PrintLoggerFactory(file=sys.stderr))


def main() -> None:
    """Build the server and serve it over stdio (blocks until the client disconnects)."""
    _configure_stdio_logging()
    build_server().run(transport="stdio")


if __name__ == "__main__":
    main()
