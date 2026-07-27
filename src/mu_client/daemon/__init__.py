"""The resident ``mu-client`` daemon (daemon-app-skeleton-spec.md) — serves capture + recall over
a local unix socket and drains the SQLite-WAL outbox. Scope note: THIS package builds the subset
daemon-app-skeleton-spec.md's own scope table names ``[skeleton]`` restricted to what this repo's
build stage owns (capture + outbox + inject + IPC front door); device-sync (§8), cross-plane
listeners (Centrifugo control frames, §8/§9), and bound-agent supervision are SHARED-plane /
multi-device features out of THIS stage's scope — see ``app.py``'s module docstring."""

from __future__ import annotations
