"""``mu-mcp`` / ``python -m mu_client.mcp`` — run the local-plane MCP server over stdio.

The stdio transport is what a Claude Code / Codex / generic MCP client spawns and speaks JSON-RPC
to (see ``.mcp.json``). This entrypoint owns ONLY process wiring — the server, its tools, and the
engine lifecycle all live in :func:`mu_client.mcp.server.build_server`.
"""

from __future__ import annotations

from mu_client.mcp.server import build_server

__all__ = ["main"]


def main() -> None:
    """Build the server and serve it over stdio (blocks until the client disconnects)."""
    build_server().run(transport="stdio")


if __name__ == "__main__":
    main()
