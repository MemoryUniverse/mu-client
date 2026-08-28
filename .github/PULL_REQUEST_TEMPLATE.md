## What this changes

<!-- One paragraph. What is different after this PR that was not true before it? -->

## Why

<!-- The problem, not the patch. Fixes #123 -->

## How to see it fail without this change

<!-- Name the test, or paste the command and the output you saw before the fix. "Tests added" is
     not evidence; a test you watched go red is. -->

## Gates

Run locally, on this branch, with `mu-core` checked out as a sibling on `dev/mlm-build`:

- [ ] `uv run --no-sync ruff check .`
- [ ] `uv run --no-sync ruff format --check .`
- [ ] `uv run --no-sync lint-imports`
- [ ] `uv run --no-sync mypy src`
- [ ] `uv run --no-sync pytest -m "not integration"`
- [ ] Integration tests (real Valkey / Qdrant / FalkorDB) — ran / not applicable / could not run (say which)

> If CI failed only on `test_ac_1_1_two_real_os_processes_exactly_one_acquires`, that is the known
> flake documented in CONTRIBUTING.md. Re-run it. Do not silence it in this PR.

## Checks that are not automatable

- [ ] **No memory content, token or personal data in any log, trace, event, metric or error
      message** this PR adds or changes.
- [ ] No new import of `mu_server`, and no new assumption that the hosted plane exists.
- [ ] Anything written outside this repo's own data directory (agent config, hooks, user files) is
      backup-first and never clobbers what it did not write.
- [ ] New I/O is async, has a timeout, and handles cancellation.
- [ ] Any new test can actually fail — I mutated the line it guards and watched it go red.
- [ ] Nothing here weakens or disables a gate. If a gate had to change, that is this PR's headline.

## Anything a reviewer should push back on

<!-- Shortcuts, open questions, boundaries you had to bend. -->
