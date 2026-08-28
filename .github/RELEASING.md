# Releasing — proposed convention

**Status: a proposal, not yet in force.** `mu-client` has **no git tag** and is **not on PyPI**
(`pypi.org/pypi/mu-client` → `404`, verified 2026-08-28). Neither are the three `mu-core`
distributions it runs on. A maintainer should ratify or amend this before the first tag is cut;
do not cut one against a file that still says "proposal".

The convention below matches `mu-core`'s so the four public repositories read the same way.

## Versioning

SemVer, `v`-prefixed annotated tags: `v0.1.0`. Pre-1.0: a **minor** bump may break anything
including the on-disk outbox format and the MCP tool surface; a **patch** bump never changes a
format, a schema, or a wire contract. No compatibility promise before `v1.0.0`.

## The blocker that is specific to this repo

`mu-client` cannot be released before `mu-core` is, and not merely for taste: this repo's
`pyproject.toml` names `mu-contracts`, `mu-engine` and `mu-local` as **filesystem path
dependencies** (`../mu-core/packages/...`), and `uv.lock` carries nine editable pins to the same
paths. A wheel built from this tree declares dependencies that resolve on exactly one machine.

So the order is fixed:

1. `mu-core` publishes `mu-contracts`, `mu-engine`, `mu-local` and tags `v0.1.0`.
2. **Here**, replace the `[tool.uv.sources]` path entries with version ranges
   (`mu-contracts>=0.1,<0.2`, and the same for the other two), `uv lock`, confirm a clean clone of
   *this repo alone* now syncs, and update the README's Quickstart — which currently tells a reader
   to clone `mu-core` as a sibling, and is correct only for as long as step 2 has not happened.
3. Only then tag `v0.1.0` here.

Tagging before step 1 ships a package nobody can install.

## The procedure

1. Land everything on the trunk. CI green (including a *re-run* of the known flaky lease test —
   see CONTRIBUTING.md — never a suppressed one).
2. `chore(release): v0.1.0` — bump `version` in `pyproject.toml`, `uv lock`, one commit.
3. `git tag -a v0.1.0 -m "mu-client v0.1.0"` on that commit.
4. Push commit, then tag.
5. GitHub Release from the tag, notes grouped by Conventional-Commit type, breaking changes first,
   written for someone deciding whether to upgrade.
6. Attach the sdist and wheel the `build` job already produces.

Steps 3-6 stay manual until the first release has been done by hand once. A `release.yml` that has
never run is not automation, it is an untested script with a trigger.

## What a tag here does not claim

Not that the hosted plane is reachable, not that any migration has been applied, and not that the
agent integrations have been verified against the current release of Claude Code or Codex. Those
are facts about someone's machine on a particular day; they belong in the release notes as prose,
where they can be qualified.
