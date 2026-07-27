"""``python -m mu_client`` — same entrypoint as the ``mu`` console script."""

from __future__ import annotations

from mu_client.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
