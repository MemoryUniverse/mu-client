"""The ``client-has-no-server`` gate's grep backstop (daemon-app-skeleton-spec.md §10;
PACKAGING-v2 §5.3): ``import-linter``'s forbidden-modules contract cannot fully resolve a module
that is never installed cross-repo (``mu_server`` lives in a sibling repo this venv never sees), so
CI (and this test) also runs the exact grep gate the spec names verbatim.

⚠ **Scope: the WHOLE REPO, not ``src/``.** Both gates were previously blind outside the package —
``.importlinter`` sets ``root_packages = mu_client``, and this file scanned ``src/`` only — so
``scripts/run_real_mu_server_for_consent_it.py`` could land five real ``mu_server`` imports inside
this Apache-2.0 repo and both gates still reported clean. A gate that cannot see a violation is not
evidence of its absence. The scan now covers every ``*.py`` in the repo, with a SINGLE argued
exemption listed in :data:`EXEMPT` — and the exemption itself is asserted to still exist and to
still import ``mu_server``, so deleting or rewriting that harness cannot leave a stale hole behind.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SRC_ROOT = _REPO_ROOT / "src"
_FORBIDDEN = re.compile(r"^\s*(from|import)\s+mu_server\b")

#: Repo-relative paths allowed to import ``mu_server``, each with the argument for it.
#:
#: ``scripts/run_real_mu_server_for_consent_it.py`` — a TEST HARNESS that is never imported by
#: ``mu_client``, lives outside ``src/``, ships in no wheel (``pyproject.toml``'s
#: ``[tool.hatch.build.targets.wheel] packages = ["src/mu_client"]``) and is executed by
#: ``mu-server``'s OWN virtualenv as a separate OS process. mu-client's integration test talks to it
#: over HTTP. No import crosses the boundary at runtime; only bytes on a socket do. The alternative
#: — a conformance server written by this lane — would prove only that the client agrees with
#: itself, which is worth nothing for the server judgements (receipt ``state``, the ``unreachable``
#: residue list, the 200-vs-204 split) the client half exists to render honestly.
EXEMPT: frozenset[str] = frozenset({"scripts/run_real_mu_server_for_consent_it.py"})

#: Directories with no first-party source in them.
_SKIP_DIRS = frozenset({".git", ".venv", "venv", "__pycache__", ".mypy_cache", ".ruff_cache"})


def _python_files() -> list[Path]:
    return [
        path
        for path in _REPO_ROOT.rglob("*.py")
        if not _SKIP_DIRS.intersection(path.relative_to(_REPO_ROOT).parts)
    ]


def _hits(paths: list[Path]) -> list[str]:
    hits: list[str] = []
    for path in paths:
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if _FORBIDDEN.match(line):
                hits.append(f"{path.relative_to(_REPO_ROOT)}:{lineno}: {line.strip()}")
    return hits


def test_grep_backstop_finds_zero_mu_server_imports() -> None:
    """Mirrors the daemon-app-skeleton-spec.md §10 backstop literally: ``! grep -rEn
    '^\\s*(from|import)\\s+mu_server\\b' packages/mu-client/src`` — re-implemented as a Python walk
    (no dependency on a ``grep`` binary being present) so the assertion is portable + typed."""
    hits = _hits(list(_SRC_ROOT.rglob("*.py")))
    assert not hits, "mu_client must never import mu_server; found:\n" + "\n".join(hits)


def test_no_file_outside_src_imports_mu_server_either_except_the_named_harness() -> None:
    """The hole the ``src/``-only scan left open.

    ``scripts/`` is in neither gate's scope, and five real ``mu_server`` imports shipped there with
    both reporting clean. Anything outside :data:`EXEMPT` is a violation."""
    offenders = sorted(
        {
            hit.split(":", 1)[0]
            for hit in _hits(_python_files())
            if hit.split(":", 1)[0] not in EXEMPT
        }
    )
    assert not offenders, (
        "only the argued harness in EXEMPT may import mu_server from this repo; found: "
        + ", ".join(offenders)
    )


def test_every_exemption_still_exists_and_still_needs_to_be_one() -> None:
    """An exemption nobody re-checks becomes a permanent, invisible hole.

    Each entry must still be a real file that really does import ``mu_server`` — so a harness that
    is deleted, moved, or rewritten to stop importing the server forces the exemption out of the
    list rather than leaving a pre-authorised gap behind it."""
    for relative in sorted(EXEMPT):
        path = _REPO_ROOT / relative
        assert path.is_file(), f"EXEMPT names {relative}, which does not exist — drop the entry"
        assert _hits([path]), (
            f"{relative} no longer imports mu_server — drop it from EXEMPT rather than leaving a "
            "pre-authorised hole in the boundary gate"
        )


def test_the_exempt_harness_is_not_shipped_in_the_wheel() -> None:
    """The exemption's argument depends on the harness never being distributed.

    ``[tool.hatch.build.targets.wheel] packages = ["src/mu_client"]`` is what makes that true; if
    the packaging config ever widened, an Apache-2.0 wheel would carry an import of the commercial
    repo."""
    packaging = (_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert 'packages = ["src/mu_client"]' in packaging
    for relative in sorted(EXEMPT):
        assert not relative.startswith(
            "src/"
        ), f"{relative} is inside the packaged tree — no exemption can apply there"


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
