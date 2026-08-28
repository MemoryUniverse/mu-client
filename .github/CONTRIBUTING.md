# Contributing to mu-client

`mu-client` is the on-device half of Memory Universe: the daemonless `mu` CLI, the MCP server your
agent talks to, the capture hooks, the SQLite-WAL outbox, and the host around `mu-core`'s embedded
engine. It talks to the hosted plane only over versioned wire contracts, and it **never imports**
`mu-server` — that is enforced, not merely intended (see the gates below).

## Setup — you need two repositories

There is no way around this yet, so it is the first thing said rather than a footnote:
`mu-client` depends on `mu-core` by **filesystem path**. `pyproject.toml` points at
`../mu-core/packages/...` and `uv.lock` carries nine editable pins to the same place. A clone of
this repo on its own cannot resolve `mu-contracts` and never could.

```bash
git clone https://github.com/MemoryUniverse/mu-core.git
git clone https://github.com/MemoryUniverse/mu-client.git
cd mu-core && git checkout dev/mlm-build && cd ../mu-client   # see the note below
uv sync --locked --extra dev
```

**The `git checkout dev/mlm-build` is load-bearing.** `mu-core`'s GitHub *default* branch is `main`,
but its *integration trunk* — the branch this client is developed and tested against — is
`dev/mlm-build`. A plain `git clone` leaves you on `main`, which is behind — and measurably so:
against `mu-core` at `main`, this repo's own `uv sync --locked` fails outright with *"The lockfile
at `uv.lock` needs to be updated"*, so nothing runs at all. CI does the same checkout for the same
reason; it is one `env:` line at the top of [`ci.yml`](workflows/ci.yml) (`MU_CORE_REF`).

This coupling ends when `mu-core` is published to PyPI and these path dependencies become version
ranges. Until then it is real, and hiding it would only move the surprise to someone's first clone.

## The gates — run them before you push

Exactly what CI runs, in the same order:

```bash
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync lint-imports
uv run --no-sync mypy src
uv run --no-sync pytest -m "not integration"
```

`--no-sync` matters: without it `uv run` re-resolves the environment and can silently hand you a
different tool version than the lockfile pins.

Measured on a clean two-repo checkout, before this workflow was written:

| Gate | Result |
|---|---|
| `ruff check .` | `All checks passed!` |
| `ruff format --check .` | `105 files already formatted` |
| `lint-imports` | `Contracts: 1 kept, 0 broken.` (88 files, 415 dependencies) |
| `mypy src` | `Success: no issues found in 55 source files` (~4.5 min) |
| `pytest -m "not integration"` | `462 passed, 39 deselected` + one known flake, below |
| `uv build` | sdist + wheel |

## One known flaky test

`tests/unit/test_sqlite_wal.py::test_ac_1_1_two_real_os_processes_exactly_one_acquires` fails
intermittently — **2 failures in 6 consecutive runs** on a clean checkout:

```
AssertionError: expected exactly one ACQUIRED, got ['ACQUIRED', 'ACQUIRED']
```

It is a defect in the test, not in `SqliteWalLeaseAdapter`. The test starts two OS processes that
each acquire the lease and hold it for a fixed `1.0` seconds. Each process must first start a
Python interpreter and import `mu_client` (and through it `mu_contracts` / `mu_engine`), and that
start-up cost varies by more than a second between the two. When the second process is that late,
the first has already released the lease and the second acquires it entirely legitimately — two
`ACQUIRED`s, and a red test that proves nothing about mutual exclusion.

The fix is a rendezvous rather than a longer sleep: the second process must attempt its acquire
while the first is provably still holding. Until that lands, this test may turn CI red for reasons
that have nothing to do with your change. **Re-run it. Do not delete it, do not mark it skip, and
do not add automatic retries** — a rerun plugin would convert a defect we can see into one we
cannot.

## Integration tests

Marked `integration`, deselected in CI, and they mock nothing — real Valkey, Qdrant and FalkorDB
containers, real transports, real data. They run on the project's dev VM. If your change touches
the outbox, the daemon, the capture path or the engine host and you cannot run them, say so in the
PR and a maintainer will.

## Conventions

- **Commits** follow Conventional Commits: `fix(outbox): …`, `feat(mcp): …`, `!` for breaking.
- **`mu_client` must never import `mu_server`.** `import-linter` enforces it, and a grep test in
  `tests/unit/test_import_boundaries.py` backstops it because the two repos cannot resolve each
  other's modules. If you need something from the hosted plane, you need it over the wire.
- **No memory content in logs, traces, events, metrics or error messages.** This process runs on a
  user's own laptop, next to their editor; the promise that their remembered text stays there is
  the product. A log line that could carry it is a bug, and reviewers will block on it.
- **Async on every I/O path**, with timeouts and cancellation handled.
- **Tests must be able to fail.** Mutate the line your new test guards and watch it go red before
  you trust it. The flake above is exactly what a test that cannot fail *for the right reason*
  costs later.

## Licensing

By contributing you agree that your contributions are licensed under the
[Apache License 2.0](../LICENSE).
