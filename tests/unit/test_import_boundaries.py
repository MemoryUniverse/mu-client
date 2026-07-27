"""The ``client-has-no-server`` gate's grep backstop (daemon-app-skeleton-spec.md §10;
PACKAGING-v2 §5.3): ``import-linter``'s forbidden-modules contract cannot fully resolve a module
that is never installed cross-repo (``mu_server`` lives in a sibling repo this venv never sees), so
CI (and this test) also runs the exact grep gate the spec names verbatim."""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
_FORBIDDEN = re.compile(r"^\s*(from|import)\s+mu_server\b")


def test_grep_backstop_finds_zero_mu_server_imports() -> None:
    """Mirrors the daemon-app-skeleton-spec.md §10 backstop literally: ``! grep -rEn
    '^\\s*(from|import)\\s+mu_server\\b' packages/mu-client/src`` — re-implemented as a Python walk
    (no dependency on a ``grep`` binary being present) so the assertion is portable + typed."""
    hits: list[str] = []
    for path in _SRC_ROOT.rglob("*.py"):
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if _FORBIDDEN.match(line):
                hits.append(f"{path}:{lineno}: {line.strip()}")
    assert not hits, "mu_client must never import mu_server; found:\n" + "\n".join(hits)


def test_real_grep_binary_backstop_agrees() -> None:
    """Belt-and-suspenders: the LITERAL command the spec names, run for real if ``grep`` is on
    PATH (skips silently if not — the pure-python test above is the portable source of truth)."""
    grep = shutil.which("grep")
    if grep is None:
        return
    result = subprocess.run(  # noqa: S603 — fully-resolved path, fixed args, no shell, test-only
        [grep, "-rEn", r"^\s*(from|import)\s+mu_server\b", str(_SRC_ROOT)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1, f"unexpected grep output:\n{result.stdout}"
