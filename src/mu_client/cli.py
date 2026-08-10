"""The mu-client CLI — ``mu add|recall|search`` (daemonless one-shot, daemon-app-skeleton-spec.md
§2/§6), ``mu capture-once`` (the hook entrypoint, capture-spec.md §2.2), ``mu flush`` (drain the
outbox + spool with no daemon required), and ``mu daemon run`` (the resident daemon).

``add``/``recall``/``search`` each construct the host, do the ONE op, tear down — NO daemon, NO
socket. This module owns argument parsing + stdout rendering ONLY; every actual op runs through a
programmatic entrypoint (:func:`mu_client.host.daemonless_host`, :func:`mu_client.capture.hook.
capture_once`, :class:`mu_client.daemon.app.LocalDaemon`) a non-CLI caller could use identically —
one behaviour, two front doors.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import signal
import sys
from collections.abc import Sequence
from pathlib import Path

from mu_engine.storage.domain.memory import MemoryTier
from mu_local.views import MemoryListView, MemoryWriteResult

from mu_client.capture.claude_tailer import backfill_thinking
from mu_client.capture.hook import capture_once, replay_spool
from mu_client.capture.model import HostKind
from mu_client.config import get_client_settings
from mu_client.daemon.app import LocalDaemon
from mu_client.errors import cli_error_boundary
from mu_client.host import daemonless_host
from mu_client.install import claude_code as install_claude_code
from mu_client.outbox.sqlite_outbox import SqliteOutbox
from mu_client.workers.ingest_client import InProcessLocalIngest
from mu_client.workers.pool import OutboxWorker

__all__ = ["main"]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mu", description="Memory Universe — daemonless local memory CLI."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    add_p = sub.add_parser("add", help="Ingest one memory (STM -> deterministic STM->MTM promote).")
    add_p.add_argument("content", help="The text to remember.")
    add_p.add_argument("--user", default=None, help="Overrides ClientSettings.default_user.")
    add_p.add_argument("--session", default=None, help="Session id (default: 'default').")

    for name, help_text in (
        ("recall", "Federated ranked recall (STM floor + MTM dense + LTM graph, fused)."),
        ("search", "Alias for 'recall' (mem0 muscle-memory verb)."),
    ):
        p = sub.add_parser(name, help=help_text)
        p.add_argument("query", help="The recall query text.")
        p.add_argument("--user", default=None, help="Overrides ClientSettings.default_user.")
        p.add_argument("--session", default=None, help="Session id (default: 'default').")
        p.add_argument(
            "--tier",
            choices=[t.value for t in MemoryTier],
            default=None,
            help="Narrow to one tier's channel; omit to fuse all three.",
        )
        p.add_argument("--limit", type=int, default=10, help="Max hits to return.")

    capture_p = sub.add_parser(
        "capture-once",
        help="Hook entrypoint: read one hook-event JSON on stdin, append to the durable outbox "
        "(daemon fast-path if reachable, else direct SQLite-WAL append), exit 0.",
    )
    capture_p.add_argument(
        "--host",
        choices=[h.value for h in HostKind],
        default=HostKind.CLAUDE_CODE.value,
        help="Which host emitted this hook event.",
    )

    backfill_p = sub.add_parser(
        "backfill-thinking",
        help="Phase 0B: tail a Claude Code session transcript JSONL for REASONING "
        "(thinking blocks + mid-turn intermediate messages), append the salient decisions/"
        "findings to the durable outbox (then 'mu flush' to drive them into the stores).",
    )
    backfill_p.add_argument(
        "--transcript",
        type=Path,
        required=True,
        help="Path to the Claude Code session transcript JSONL "
        "(~/.claude/projects/<slug>/<session>.jsonl, or a hook payload's transcript_path).",
    )
    backfill_p.add_argument(
        "--session",
        default=None,
        help="Override the η.session slot (default: the transcript's own sessionId / file stem).",
    )

    sub.add_parser(
        "flush",
        help="Drain the spool + SQLite-WAL outbox into mu-local (PENDING -> remember -> ACKED). "
        "No daemon required.",
    )

    daemon_p = sub.add_parser("daemon", help="The resident daemon (capture/recall over IPC).")
    daemon_p.add_argument("daemon_command", choices=["run"], help="Only 'run' this stage.")

    install_p = sub.add_parser(
        "install",
        help="Idempotently write a host's managed hook block (Phase 0 installer).",
    )
    install_sub = install_p.add_subparsers(dest="install_target", required=True)
    install_cc_p = install_sub.add_parser(
        "claude-code",
        help="Write the mu_capture_once.sh hook block into a Claude Code settings.json.",
    )
    install_cc_p.add_argument(
        "--settings-path",
        type=Path,
        default=Path("~/.claude/settings.json").expanduser(),
        help="Target settings.json (default: ~/.claude/settings.json; pass a test path to avoid "
        "touching a real one).",
    )
    install_cc_p.add_argument(
        "--hook-script",
        type=Path,
        default=install_claude_code.DEFAULT_HOOK_SCRIPT,
        help="Absolute path to mu_capture_once.sh (default: this checkout's scripts/hooks/).",
    )

    uninstall_p = sub.add_parser(
        "uninstall", help="Remove a host's managed hook block (Phase 0 installer)."
    )
    uninstall_sub = uninstall_p.add_subparsers(dest="install_target", required=True)
    uninstall_cc_p = uninstall_sub.add_parser(
        "claude-code", help="Remove ONLY the managed mu_capture_once.sh entries from settings.json."
    )
    uninstall_cc_p.add_argument(
        "--settings-path",
        type=Path,
        default=Path("~/.claude/settings.json").expanduser(),
        help="Target settings.json (default: ~/.claude/settings.json; pass a test path to avoid "
        "touching a real one).",
    )
    uninstall_cc_p.add_argument(
        "--hook-script",
        type=Path,
        default=install_claude_code.DEFAULT_HOOK_SCRIPT,
        help="Absolute path to mu_capture_once.sh (must match the one install used).",
    )

    return parser


def _render_write(result: MemoryWriteResult) -> None:
    print(
        f"memory_id={result.memory_id} content_hash={result.content_hash} "
        f"promoted={result.promoted} tiers_written={','.join(result.tiers_written)}"
    )


def _render_list(listing: MemoryListView) -> None:
    if listing.degraded is not None:
        print(f"[degraded: {listing.degraded}]", file=sys.stderr)
    if not listing.items:
        print("(no results)")
        return
    for item in listing.items:
        floor = " [floor]" if item.is_floor else ""
        print(
            f"{item.fused_score:.4f}  {item.tier}/{item.channel}{floor}  "
            f"{item.memory_id}  {item.content}"
        )


def _run_install(args: argparse.Namespace) -> int:
    if args.install_target != "claude-code":
        raise AssertionError(f"unreachable: unknown install target {args.install_target!r}")
    result = install_claude_code.install(args.settings_path, hook_script_path=args.hook_script)
    print(
        f"settings_path={result.settings_path} backup_path={result.backup_path} "
        f"events_added={list(result.events_added)} "
        f"events_already_present={list(result.events_already_present)} "
        f"unrelated_hooks_preserved={result.unrelated_hooks_preserved}"
    )
    return 0


def _run_uninstall(args: argparse.Namespace) -> int:
    if args.install_target != "claude-code":
        raise AssertionError(f"unreachable: unknown install target {args.install_target!r}")
    result = install_claude_code.uninstall(args.settings_path, hook_script_path=args.hook_script)
    print(
        f"settings_path={result.settings_path} backup_path={result.backup_path} "
        f"events_removed={list(result.events_removed)} "
        f"events_not_present={list(result.events_not_present)} "
        f"unrelated_hooks_preserved={result.unrelated_hooks_preserved}"
    )
    return 0


async def _run_capture_once(args: argparse.Namespace) -> int:
    settings = get_client_settings()
    raw = sys.stdin.buffer.read()
    response = await capture_once(settings, host=HostKind(args.host), raw=raw)
    print(json.dumps(response))
    return 0  # capture NEVER fails/blocks the host turn (capture-spec.md §3/§13)


async def _run_backfill_thinking(args: argparse.Namespace) -> int:
    settings = get_client_settings()
    result = await backfill_thinking(
        settings, transcript_path=args.transcript, session_id=args.session
    )
    # Content-free summary (counts only, never reasoning text) — the CLI's own stdout surface.
    print(
        f"appended={result.appended} records_scanned={result.records_scanned} "
        f"thinking_blocks_seen={result.thinking_blocks_seen} "
        f"thinking_blocks_plaintext={result.thinking_blocks_plaintext} "
        f"since_byte={result.since_byte} end_offset={result.end_offset} halted={result.halted}"
    )
    return 0  # backfill is best-effort enrichment — never a nonzero exit on empty/absent reasoning


async def _run_flush() -> int:
    settings = get_client_settings()
    outbox = SqliteOutbox(settings.outbox.outbox_path)
    await outbox.open()
    try:
        replayed = await replay_spool(settings, outbox)
        async with daemonless_host(settings) as host:
            ingest = InProcessLocalIngest(host, user=settings.default_user)
            worker = OutboxWorker(
                outbox,
                ingest,
                settings=settings.outbox,
                org=settings.default_namespace,
                workspace=settings.default_workspace,
                user=settings.default_user,
            )
            drained = acked = skipped = dead_lettered = 0
            while True:
                tick = await worker.run_once()
                if tick.drained == 0:
                    break
                drained += tick.drained
                acked += tick.acked
                skipped += tick.skipped
                dead_lettered += tick.dead_lettered
        print(
            f"spool_replayed={replayed} drained={drained} acked={acked} skipped={skipped} "
            f"dead_lettered={dead_lettered}"
        )
    finally:
        await outbox.aclose()
    return 0


async def _run_daemon(args: argparse.Namespace) -> int:
    del args  # only 'run' this stage; argparse already enforced the choice
    daemon = LocalDaemon(get_client_settings())
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):  # signal handlers are POSIX-only
            loop.add_signal_handler(sig, stop.set)
    async with daemon.lifespan():
        await stop.wait()
    return 0


@cli_error_boundary
async def _run(argv: Sequence[str]) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "capture-once":
        return await _run_capture_once(args)
    if args.command == "backfill-thinking":
        return await _run_backfill_thinking(args)
    if args.command == "flush":
        return await _run_flush()
    if args.command == "daemon":
        return await _run_daemon(args)
    if args.command == "install":
        return _run_install(args)
    if args.command == "uninstall":
        return _run_uninstall(args)
    async with daemonless_host() as host:
        if args.command == "add":
            _render_write(await host.add(args.content, user=args.user, session=args.session))
            return 0
        tier = MemoryTier(args.tier) if args.tier is not None else None
        verb = host.recall if args.command == "recall" else host.search
        listing = await verb(
            args.query, user=args.user, session=args.session, tier=tier, limit=args.limit
        )
        _render_list(listing)
        return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Console-script entrypoint (``pyproject.toml`` ``[project.scripts] mu``)."""
    return asyncio.run(_run(sys.argv[1:] if argv is None else argv))


if __name__ == "__main__":
    raise SystemExit(main())
