# Security policy

`mu-client` runs on a user's own machine, next to their editor, with access to their agent sessions
and their local memory stores. A vulnerability here is a vulnerability on someone's laptop. Please
report it privately.

## Reporting a vulnerability

**Do not open a public issue.** Use GitHub's private vulnerability reporting:
**[Security → Report a vulnerability](https://github.com/MemoryUniverse/mu-client/security/advisories/new)**.
Only you and the maintainers can see it.

If that form is unavailable to you, open a normal issue containing only *"I need a private channel
for a security report"* — **no details** — and a maintainer will open an advisory and invite you.

## What to include

- What an attacker can do, and what access they need first (local user? another process? a
  malicious MCP client? a hostile agent transcript?).
- The smallest reproduction you have.
- The commit you saw it on.

**Never include real memory content, credentials, tokens or personal data.** Redact, and say what
you redacted. Keeping remembered text on the machine it was remembered on is the entire point of
this component; a security report is not the place to make an exception.

## What to expect

| | Target |
|---|---|
| Acknowledgement | within 3 working days |
| First assessment | within 10 working days |
| Fix or a dated plan | agreed with you on the advisory |

Credit in the advisory unless you ask us not to.

## Supported versions

**None yet.** No git tag, no PyPI release. Fixes land on the trunk and nowhere else until the first
release — see [RELEASING.md](RELEASING.md).

## Scope

Especially in scope, because of where this process runs:

- Anything that writes memory content, a token or a namespace's data into a log, trace, event,
  metric, crash report or error message.
- Anything that lets a **hostile agent transcript** — text this client captures and stores by
  design — change what the client does: command execution, path traversal, config rewriting,
  injection into a prompt that then acts.
- The `mu install` / `mu uninstall` config-editing paths: a clobbered or backdoored hook block in a
  user's agent configuration.
- Credentials or tokens written to disk with the wrong permissions, or read from the wrong place.
- Any path where the local client sends more to the hosted plane than the user consented to share.
- A local privilege or namespace boundary crossed through the MCP server or the daemon IPC surface.

Out of scope: third-party dependency advisories with no exploitable path through this code (report
upstream, and tell us so we can pin), attacks that require an already-root local attacker, and the
hosted plane itself (`mu-server`), which is not in this repository.
