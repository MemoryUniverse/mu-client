"""Capture cluster (capture-spec.md) — ``RawActivity``/parsers/hook entrypoint. ``[core-shaped,
client-scoped]``: this package builds the daemonless + daemon-shared capture types this repo owns
(mu-client has no mu-core capture package to extend onto; the shapes mirror capture-spec.md §4
verbatim so a future promotion into mu-core's ``mu_local`` is a straight lift)."""

from __future__ import annotations
