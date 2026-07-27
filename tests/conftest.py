"""Shared mu-client test fixtures. Host ports come from the central env boundary (``.env.test``
in ``mu-core``, symlinked/copied here as ``.env.test`` too — never a hardcoded literal)."""

from __future__ import annotations

import uuid

import pytest

from mu_client.config import ClientSettings


@pytest.fixture(scope="session")
def client_settings() -> ClientSettings:
    return ClientSettings()


@pytest.fixture
def uid() -> str:
    return uuid.uuid4().hex[:12]
