"""Root conftest — carries the VM-only suite guard (root CLAUDE.md rule 13)."""

from __future__ import annotations


# --- BEGIN VM-ONLY TEST GUARD (CLAUDE.md rule 13) ---
def pytest_cmdline_main(config: object) -> int | None:
    """Refuse to run a SUITE on the developer laptop. See root CLAUDE.md rule 13.

    Suites run on `mu-dev-vm` via `infra/mu-vm/vm_test.sh <repo>`. This is enforced here rather
    than merely written down, because the written rule was ignored repeatedly and a full suite
    costs ~2.5 GB of pytest RSS on a 15 GB laptop that is also running the agents — twice it
    exhausted swap and killed the session mid-run.

    It is not only about footprint. A local run reads the developer's working tree, its caches and
    its ``sys.path``: mu-core's own CI line passes here with 1568 tests and, under
    ``python -m pytest``, collects ZERO with an error that reads like a healthy deselect count.
    A local green does not tell you what a clean checkout or a CI runner sees.

    ESCAPE HATCHES, in order of preference:
      * run it on the VM: ``infra/mu-vm/vm_test.sh <repo> [args]``  (sets MU_ON_VM=1 there)
      * one file runs on the VM too: ``vm_test.sh <repo> tests/x/test_y.py``
      * a deliberate local full run: ``MU_ALLOW_LOCAL_TESTS=1 pytest ...`` — say why in your report.
    """
    import os
    import sys

    if os.environ.get("MU_ON_VM") or os.environ.get("MU_ALLOW_LOCAL_TESTS"):
        return None
    if os.environ.get("CI"):
        return None

    sys.stderr.write(
        "\n"
        "REFUSED: suites run on the VM, not on this laptop (root CLAUDE.md rule 13).\n"
        "  ->  infra/mu-vm/vm_test.sh <repo> [pytest args]\n"
        "      (~15x faster; the stores are localhost there)\n"
        "  ->  ONE file on the VM too: vm_test.sh <repo> tests/x/test_y.py\n"
        "  ->  deliberate local full run: MU_ALLOW_LOCAL_TESTS=1 (and say why)\n"
        "\n"
    )
    return 4


# --- END VM-ONLY TEST GUARD ---
