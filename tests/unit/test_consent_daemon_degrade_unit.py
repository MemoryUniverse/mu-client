"""**A shared-plane failure must never take the FULL-LOCAL daemon down.**

``mu-client/CLAUDE.md``: *"FULL-LOCAL must be a complete, good memory system with no server
required."* Capture, inject and the outbox are LOCAL-plane and depend on nothing here; the consent
surface is the only SHARED-plane thing the daemon owns. ``LocalDaemon.start()``'s comment asserted
that a failure to build it was CAUGHT — and there was no ``try`` in the block, so an unreadable
consent sqlite file aborted the entire daemon.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from structlog.testing import capture_logs

from mu_client.config import ClientSettings, ConsentSettings
from mu_client.consent.composition import open_consent_service
from mu_client.daemon.app import LocalDaemon
from mu_client.errors import ClientError, ConsentStoreCorruptionError

pytestmark = pytest.mark.unit


def _settings(tmp_path: Path, *, db_name: str) -> ClientSettings:
    return ClientSettings(
        consent=ConsentSettings(
            server_base_url="http://127.0.0.1:1",  # never dialled: nothing here makes a request
            tombstone_db_path=tmp_path / db_name,
        )
    )


async def test_an_unreadable_consent_store_degrades_instead_of_killing_the_daemon(
    tmp_path: Path,
) -> None:
    """The reachable error, through the REAL ``open_consent_service`` and the REAL sqlite open.

    **MUTATION:** delete the ``except ClientError`` arm in ``_open_consent_surface`` -> RED
    (``ConsentStoreCorruptionError`` propagates and ``start()`` would abort).
    """
    corrupt = tmp_path / "consent.sqlite"
    corrupt.write_bytes(b"this is not a sqlite database, it is 47 bytes of noise")

    # Precondition, proven rather than assumed: the composition root really does raise here.
    with pytest.raises(ConsentStoreCorruptionError) as raised:
        async with open_consent_service(_settings(tmp_path, db_name="consent.sqlite")):
            pass
    assert isinstance(raised.value, ClientError), "the guard's clause must cover this error"

    daemon = LocalDaemon(_settings(tmp_path, db_name="consent.sqlite"))
    await daemon._open_consent_surface()
    assert daemon._consent is None
    assert daemon._consent_exit is None, "a half-entered exit stack must not survive the failure"


async def test_a_healthy_store_still_produces_a_real_consent_surface(tmp_path: Path) -> None:
    """The degrade must not be a blanket swallow: a working configuration still gets the surface.

    **MUTATION:** make ``_open_consent_surface`` return early unconditionally -> RED.
    """
    daemon = LocalDaemon(_settings(tmp_path, db_name="fresh.sqlite"))
    try:
        await daemon._open_consent_surface()
        assert daemon._consent is not None
    finally:
        if daemon._consent_exit is not None:
            await daemon._consent_exit.aclose()


async def test_a_full_local_boot_is_silent_rather_than_reporting_a_failure() -> None:
    """ "No server configured" is the NORMAL configuration, not a degrade — and it must not read
    like one.

    Both the early return and the ``except ClientError`` arm end at the same state
    (``_consent is None``, ``_consent_exit is None``), because ``open_consent_service`` refuses
    before it opens anything. So the state assertions alone CANNOT distinguish them — measured: the
    mutation that deletes the early return left them green. The difference that is real is the log
    line: without the early return, every FULL-LOCAL boot warns
    ``daemon.consent_surface_unavailable`` and an operator reads a working laptop as a broken one.

    **MUTATION:** drop the ``server_base_url is None`` early return from
    ``_open_consent_surface`` -> RED. (This test was rewritten after its first version measured
    GREEN under exactly that mutation.)
    """
    daemon = LocalDaemon(ClientSettings())
    with capture_logs() as logs:
        await daemon._open_consent_surface()
    assert daemon._consent is None
    assert daemon._consent_exit is None
    assert [
        entry for entry in logs if entry.get("event") == "daemon.consent_surface_unavailable"
    ] == []


async def test_a_broken_shared_plane_does_say_so(tmp_path: Path) -> None:
    """The other half of the same distinction: a configured-but-unopenable surface is a real
    degrade and is reported, content-free (the exception TYPE, never the store path or server URL).

    **MUTATION:** delete the ``_log.warning`` call in ``_open_consent_surface`` -> RED.
    """
    (tmp_path / "consent.sqlite").write_bytes(b"not a sqlite database")
    daemon = LocalDaemon(_settings(tmp_path, db_name="consent.sqlite"))
    with capture_logs() as logs:
        await daemon._open_consent_surface()
    warned = [entry for entry in logs if entry.get("event") == "daemon.consent_surface_unavailable"]
    assert len(warned) == 1
    assert warned[0]["error"] == "ConsentStoreCorruptionError"
    assert str(tmp_path) not in str(warned[0]), "content-free: no store path on the log line"
