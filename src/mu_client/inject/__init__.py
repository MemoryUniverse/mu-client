"""The recall/inject bridge (capture-spec.md §7.2) — recall from mu-local, render the
``additionalContext`` payload the host's hook returns for prompt injection."""

from __future__ import annotations

from mu_client.inject.recall_bridge import RecallInjectBridge, RenderedContext

__all__ = ["RecallInjectBridge", "RenderedContext"]
