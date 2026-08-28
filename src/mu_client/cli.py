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
from typing import Any, TypeVar

from mu_contracts.contracts.recall import RecallResult
from mu_contracts.contracts.views import MemoryWriteResult
from mu_contracts.domain.model.health import MemoryHealthView
from mu_contracts.domain.model.pin import PinResult
from mu_engine.storage.domain.memory import MemoryTier
from pydantic import BaseModel, ValidationError

from mu_client.capture.claude_tailer import backfill_thinking
from mu_client.capture.codex import backfill_codex
from mu_client.capture.hook import capture_once, replay_spool
from mu_client.capture.model import HostKind
from mu_client.config import get_client_settings, render_endpoint_env
from mu_client.consent.composition import open_consent_service
from mu_client.consent.wire import NAMED_REASON_RULE
from mu_client.daemon.app import LocalDaemon
from mu_client.daemon.ipc_client import IpcClient
from mu_client.errors import DaemonReplyInvalidError, cli_error_boundary
from mu_client.host import daemonless_host
from mu_client.install import claude_code as install_claude_code
from mu_client.install import codex as install_codex
from mu_client.memory_health import (
    HEALTH_ROUTE,
    PIN_ROUTE,
    UNPIN_ROUTE,
    namespace_for,
)
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
    add_p.add_argument(
        "--importance",
        type=float,
        default=None,
        help="Importance in [0,1] (canonical AddRequest.importance_score). Drives the ONE "
        "STM->MTM promotion gate (DeterministicPromoteStage: importance >= importance_promote, "
        "default 0.6). Omit to leave the engine's own default (0.5, STM-only).",
    )

    # ---- memory-health + pinning (memory-health-pinning-spec.md §7.1/§7.2) -------------------
    # These three speak to the RESIDENT DAEMON over its unix socket (the surface the spec names
    # first), not to the daemonless one-shot host `add`/`recall`/`search` use — see
    # `daemon/ipc_client.py` for why. Still "one behaviour, two front doors": the daemon's route
    # handlers are the programmatic entrypoint, and this module only parses and renders.
    health_p = sub.add_parser(
        "health",
        help="Show the health of your memory: stale / low-confidence / conflicting / decaying "
        "items, plus pinned and archived markers. Read-only — changes nothing.",
    )
    health_p.add_argument("--user", default=None, help="Overrides ClientSettings.default_user.")
    health_p.add_argument("--session", default=None, help="Session id (default: 'default').")
    health_p.add_argument(
        "--flag",
        action="append",
        default=None,
        dest="flags",
        help="Show only entries carrying this flag (repeatable): stale, low_confidence, "
        "conflicting, decaying, pinned, archived. Omit for the service's own default.",
    )
    health_p.add_argument(
        "--cursor", default=None, help="Continue the previous page (from 'next_cursor')."
    )

    pin_p = sub.add_parser(
        "pin",
        help="Pin one memory: never demoted, garbage-collected or auto-superseded. Changes "
        "RETENTION only — not what is recalled, and not who can read it.",
    )
    pin_p.add_argument("memory_id", help="The id of the memory to pin.")
    pin_p.add_argument(
        "--reason",
        default=None,
        help="Short NAMED classification for your own pin ('policy', 'decision'), max 200 chars. "
        "Not a note field — it is persisted on the item and never carried on the event bus.",
    )
    pin_p.add_argument("--user", default=None, help="Overrides ClientSettings.default_user.")
    pin_p.add_argument("--session", default=None, help="Session id (default: 'default').")

    unpin_p = sub.add_parser(
        "unpin", help="Unpin one memory, letting the normal lifecycle resume for it."
    )
    unpin_p.add_argument("memory_id", help="The id of the memory to unpin.")
    unpin_p.add_argument("--user", default=None, help="Overrides ClientSettings.default_user.")
    unpin_p.add_argument("--session", default=None, help="Session id (default: 'default').")

    # ---- Decision D4: the agent-share consent surface -----------------------------------------
    # D4 §4.2-D step 4 asks for an explicit opt-in flow that "shows the exposes-vs-private
    # contract" plus "a persistent 'your agent is shared here' affordance with one-tap revoke".
    # There is deliberately NO `share` verb here: on mu-server a grant is minted by
    # `POST /v1/rooms/{id}/bind` and there is no issue endpoint, because "sharing an agent IS the
    # consent act" (mu-server/src/mu_server/routes/rooms.py:829-834). Offering `mu agent-share
    # grant` would be a CLI verb with no route behind it.
    share_p = sub.add_parser(
        "agent-share",
        help="What sharing an agent into a room exposes, and one-tap revoke (Decision D4).",
    )
    share_sub = share_p.add_subparsers(dest="agent_share_action", required=True)

    share_status_p = share_sub.add_parser(
        "status",
        help="Show the exposes-vs-keeps-private contract for a shared agent, computed against "
        "what THIS device can actually do.",
    )
    share_status_p.add_argument("--room", required=True, help="Room (session) id.")
    share_status_p.add_argument("--agent", required=True, help="The shared agent's principal id.")

    share_revoke_p = share_sub.add_parser(
        "revoke",
        help="Withdraw the share. Cuts on THIS device first (durably), then asks the server, then "
        "reports everything the revoke could not reach.",
    )
    share_revoke_p.add_argument("--room", required=True, help="Room (session) id.")
    share_revoke_p.add_argument("--agent", required=True, help="The shared agent's principal id.")
    share_revoke_p.add_argument(
        "--reason",
        default=None,
        help="A NAMED reason for an operator ('user_revoked', 'policy_change'). It lands on a "
        "content-free trust-ledger row, so it is REFUSED unless it is a name: "
        f"{NAMED_REASON_RULE}. Prose about the conversation is not a name.",
    )

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

    backfill_codex_p = sub.add_parser(
        "backfill-codex",
        help="Phase 4: tail a codex rollout JSONL (~/.codex/sessions/.../rollout-*.jsonl) for the "
        "session's user prompts, assistant turns and tool calls, append them to the durable outbox "
        "(then 'mu flush' to drive them into the stores).",
    )
    backfill_codex_p.add_argument(
        "--rollout",
        type=Path,
        required=True,
        help="Path to a codex rollout JSONL "
        "(~/.codex/sessions/YYYY/MM/DD/rollout-<ts>-<uuid>.jsonl).",
    )
    backfill_codex_p.add_argument(
        "--session",
        default=None,
        help="Override the η.session slot (default: the rollout's session_meta / filename UUID).",
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
    install_cc_p.add_argument(
        "--mcp-json-path",
        type=Path,
        default=Path(".mcp.json").resolve(),
        help="Target .mcp.json to register the memory-universe MCP server into, with the resolved "
        "store endpoints baked into its env block (default: ./.mcp.json in the cwd).",
    )
    install_cc_p.add_argument(
        "--no-mcp",
        action="store_true",
        help="Only write the capture hooks; skip registering the MCP server in .mcp.json.",
    )

    install_codex_p = install_sub.add_parser(
        "codex",
        help="Write the codex notify hook (+ MU MCP server) into a codex config.toml (Phase 4).",
    )
    install_codex_p.add_argument(
        "--config-path",
        type=Path,
        default=Path("~/.codex/config.toml").expanduser(),
        help="Target codex config.toml (default: ~/.codex/config.toml; pass a TEST path or set "
        "CODEX_HOME to avoid touching a real one).",
    )
    install_codex_p.add_argument(
        "--notify-script",
        type=Path,
        default=install_codex.DEFAULT_NOTIFY_SCRIPT,
        help="Absolute path to mu_codex_notify.sh (default: this checkout's scripts/hooks/).",
    )
    install_codex_p.add_argument(
        "--no-mcp",
        action="store_true",
        help="Only write the notify hook; skip registering the memory-universe MCP server.",
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

    uninstall_codex_p = uninstall_sub.add_parser(
        "codex", help="Remove ONLY the managed notify hook + memory-universe MCP server from codex."
    )
    uninstall_codex_p.add_argument(
        "--config-path",
        type=Path,
        default=Path("~/.codex/config.toml").expanduser(),
        help="Target codex config.toml (default: ~/.codex/config.toml; pass a TEST path).",
    )
    uninstall_codex_p.add_argument(
        "--notify-script",
        type=Path,
        default=install_codex.DEFAULT_NOTIFY_SCRIPT,
        help="Absolute path to mu_codex_notify.sh (must match the one install used).",
    )

    return parser


def _render_write(result: MemoryWriteResult) -> None:
    print(
        f"memory_id={result.memory_id} content_hash={result.content_hash} "
        f"promoted={result.promoted} tiers_written={','.join(result.tiers_written)}"
    )


def _render_list(listing: RecallResult) -> None:
    if listing.degraded is not None:
        print(f"[degraded: {listing.degraded.value}]", file=sys.stderr)
    if not listing.items:
        print("(no results)")
        return
    for item in listing.items:
        floor = " [floor]" if item.is_floor else ""
        print(
            f"{item.fused_score:.4f}  {item.tier}/{item.channel}{floor}  "
            f"{item.memory_id}  {item.content}"
        )


def _render_ipc_failure(payload: dict[str, object]) -> int:
    """A non-200 reply from the daemon. Prints the daemon's own STABLE error NAME and status —
    never a message body, so nothing the daemon knows about a memory can reach stdout/stderr
    here. Exit code 1, matching :func:`~mu_client.errors.cli_error_boundary`."""
    print(
        f"mu: {payload.get('error', 'daemon_error')} (status={payload.get('status')})",
        file=sys.stderr,
    )
    return 1


_ReplyModel = TypeVar("_ReplyModel", bound=BaseModel)


def _reply_body(model: type[_ReplyModel], payload: dict[str, Any]) -> _ReplyModel:
    """Validate a 200 IPC reply against the mu-core contract it claims to be.

    The renderers below took ``payload['memory_id']`` / ``entry['tier']`` directly, which meant any
    reply of an unexpected shape became a raw ``KeyError`` traceback: ``cli_error_boundary``
    re-raises everything outside the ``MemoryUniverseError`` hierarchy, so the CLI's promise of a
    single content-free refusal line quietly did not hold on that path. Parsing through the
    contract closes the class rather than one instance of it — and it is the same discipline the
    IPC side now applies inbound (``memory_health.pin_request_of``): let mu-core's own frozen,
    ``extra="forbid"`` model be the judge of the shape, and never re-state its rules here.

    ``status`` is the IPC ENVELOPE, not part of either contract, so it is dropped before
    validation (both models are ``extra="forbid"``).
    """
    try:
        return model.model_validate({k: v for k, v in payload.items() if k != "status"})
    except ValidationError as exc:
        raise DaemonReplyInvalidError(model.__name__) from exc


def _render_health(view: MemoryHealthView) -> None:
    """Content-free by construction: ``MemoryHealthView`` has no content field to print (mu-core
    did not build the spec's ``preview``), so every value below is an id, an enum, a number or a
    timestamp. ``retention_unknown`` is printed rather than hidden — a lens that could not compute
    decay for part of its page must SAY so."""
    summary = view.summary
    partial = " [partial: a tier was unreachable]" if view.partial else ""
    print(
        f"total={summary.total} pinned={summary.pinned_count} "
        f"retention_unknown={summary.retention_unknown}{partial}"
    )
    if summary.by_flag:
        print(
            "  "
            + "  ".join(
                f"{flag.value}={count}"
                for flag, count in sorted(summary.by_flag.items(), key=lambda kv: kv[0].value)
            )
        )
    if not view.entries:
        print("(nothing at risk)")
    for entry in view.entries:
        flags = ",".join(sorted(flag.value for flag in entry.flags)) or "-"
        pinned = " [pinned]" if entry.pinned else ""
        print(
            f"{entry.memory_id}  {entry.tier.value}/{entry.state.value}  "
            f"retention={entry.retention:.4f}  {flags}{pinned}"
        )
    if view.next_cursor:
        print(f"next_cursor={view.next_cursor}")


def _render_pin(result: PinResult) -> None:
    print(
        f"memory_id={result.memory_id} pinned={result.pinned} "
        f"pinned_at={result.pinned_at} version={result.version}"
    )


async def _run_health(args: argparse.Namespace) -> int:
    settings = get_client_settings()
    ns = namespace_for(settings, user=args.user, session=args.session)
    reply = await IpcClient(settings.ipc).request(
        HEALTH_ROUTE,
        {"namespace": list(ns.parts()), "flags": args.flags, "cursor": args.cursor},
    )
    if reply.get("status") != 200:
        return _render_ipc_failure(reply)
    _render_health(_reply_body(MemoryHealthView, reply))
    return 0


async def _run_pin(args: argparse.Namespace, *, route: str) -> int:
    settings = get_client_settings()
    ns = namespace_for(settings, user=args.user, session=args.session)
    payload: dict[str, Any] = {"namespace": list(ns.parts()), "memory_id": args.memory_id}
    if route == PIN_ROUTE:
        payload["reason"] = args.reason
    reply = await IpcClient(settings.ipc).request(route, payload)
    if reply.get("status") != 200:
        return _render_ipc_failure(reply)
    _render_pin(_reply_body(PinResult, reply))
    return 0


async def _run_agent_share(args: argparse.Namespace) -> int:
    """``mu agent-share status|revoke`` — the owner's own consent surface.

    Goes DIRECT to :func:`~mu_client.consent.composition.open_consent_service` rather than through
    the daemon's IPC routes, on purpose: a consent screen an owner cannot open without first
    starting a daemon is not an affordance. The daemon serves the same service on the same two
    verbs for the resident case (:mod:`mu_client.consent.ipc_surface`); both call one service, so
    there is no second opinion about what a grant exposes.
    """
    settings = get_client_settings()
    async with open_consent_service(settings) as consent:
        if args.agent_share_action == "revoke":
            outcome = await consent.revoke(
                room_id=args.room, agent_principal_id=args.agent, reason=args.reason
            )
            for line in outcome.render():
                print(line)
            # EXIT CODE IS NOT 0 WHEN THE SERVER DID NOT CONFIRM. A script that revokes in a loop
            # must be able to tell "withdrawn everywhere it could be" from "withdrawn here only,
            # the agent may still be acting in the room" WITHOUT parsing prose.
            return 0 if outcome.server_confirmed else 2
        status = await consent.describe(room_id=args.room, agent_principal_id=args.agent)
        for line in status.render():
            print(line)
        return 0


def _run_install(args: argparse.Namespace) -> int:
    if args.install_target == "codex":
        return _run_install_codex(args)
    if args.install_target != "claude-code":
        raise AssertionError(f"unreachable: unknown install target {args.install_target!r}")
    result = install_claude_code.install(args.settings_path, hook_script_path=args.hook_script)
    print(
        f"settings_path={result.settings_path} backup_path={result.backup_path} "
        f"events_added={list(result.events_added)} "
        f"events_already_present={list(result.events_already_present)} "
        f"unrelated_hooks_preserved={result.unrelated_hooks_preserved}"
    )
    if not args.no_mcp:
        # Bake the RESOLVED store endpoints (CWD-independent — resolve_env_files) into the
        # registered MCP server's env block, so a `mu-mcp` Claude Code spawns from any project dir
        # reaches the real stores (gap A part 2).
        endpoint_env = render_endpoint_env(get_client_settings())
        mcp_result = install_claude_code.register_mcp_server(args.mcp_json_path, env=endpoint_env)
        print(
            f"mcp_json_path={mcp_result.mcp_json_path} backup_path={mcp_result.backup_path} "
            f"server_name={mcp_result.server_name} "
            f"endpoint_vars_written={mcp_result.endpoint_vars_written}"
        )
    # Gap B: the one-time-trust / headless-testing guidance (interactive-vs-headless artifact).
    print(
        install_claude_code.post_install_guidance(
            settings_path=args.settings_path, mcp_json_path=args.mcp_json_path
        )
    )
    return 0


def _run_install_codex(args: argparse.Namespace) -> int:
    with_mcp = not args.no_mcp
    mcp_env = render_endpoint_env(get_client_settings()) if with_mcp else None
    result = install_codex.install(
        args.config_path,
        notify_script_path=args.notify_script,
        with_mcp=with_mcp,
        mcp_env=mcp_env,
    )
    print(
        f"config_path={result.config_path} backup_path={result.backup_path} "
        f"notify_written={result.notify_written} "
        f"notify_already_present={result.notify_already_present} "
        f"notify_conflict={result.notify_conflict} "
        f"mcp_server_registered={result.mcp_server_registered} "
        f"endpoint_vars_written={result.endpoint_vars_written} "
        f"mcp_servers_preserved={result.mcp_servers_preserved}"
    )
    print(install_codex.post_install_guidance(config_path=args.config_path, with_mcp=with_mcp))
    return 0


def _run_uninstall(args: argparse.Namespace) -> int:
    if args.install_target == "codex":
        result_codex = install_codex.uninstall(
            args.config_path, notify_script_path=args.notify_script
        )
        print(
            f"config_path={result_codex.config_path} backup_path={result_codex.backup_path} "
            f"notify_removed={result_codex.notify_removed} "
            f"notify_foreign_left={result_codex.notify_foreign_left} "
            f"mcp_server_removed={result_codex.mcp_server_removed} "
            f"mcp_servers_preserved={result_codex.mcp_servers_preserved}"
        )
        return 0
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


async def _run_backfill_codex(args: argparse.Namespace) -> int:
    settings = get_client_settings()
    result = await backfill_codex(settings, rollout_path=args.rollout, session_id=args.session)
    print(
        f"appended={result.appended} records_scanned={result.records_scanned} "
        f"session_id={result.session_id} since_byte={result.since_byte} "
        f"end_offset={result.end_offset} halted={result.halted}"
    )
    return 0  # a rollout with no capturable turns is a valid no-op, never a nonzero exit


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
    if args.command == "backfill-codex":
        return await _run_backfill_codex(args)
    if args.command == "flush":
        return await _run_flush()
    if args.command == "daemon":
        return await _run_daemon(args)
    if args.command == "install":
        return _run_install(args)
    if args.command == "uninstall":
        return _run_uninstall(args)
    if args.command == "health":
        return await _run_health(args)
    if args.command in (PIN_ROUTE, UNPIN_ROUTE):
        return await _run_pin(args, route=args.command)
    if args.command == "agent-share":
        return await _run_agent_share(args)
    async with daemonless_host() as host:
        if args.command == "add":
            _render_write(
                await host.add(
                    args.content,
                    user=args.user,
                    session=args.session,
                    importance_score=args.importance,
                )
            )
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
