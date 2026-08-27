"""Deterministic distillation of recall hits into USEFUL injected context (validation gap D).

The recall/inject path (``RecallInjectBridge.render`` and the MCP silent resource) used to render
the session's STM recall verbatim — one ``- {content}`` line per hit. Because session-scoped recall
leans on an STM recency FLOOR (``is_floor`` items, query-insensitive), that verbatim dump carries
the raw machine noise a coding host generates every turn: ``Write: {…}`` file writes, ``Bash: …``
command output, ``Read: …`` file reads. Those are tool-capture lines (``capture/parsers.py::
_map_event`` stores a ``PostToolUse`` as ``"<ToolName>: <outcome>"``), NOT human-meaningful facts or
decisions — injecting them wastes the host's context budget and buries the real signal.

This module renders DISTILLED context instead, with three deterministic, fast (hot-path) passes and
NO new memory logic — it only re-shapes the hits recall already returned:

1. **Filter tool-noise** — drop ``<ToolName>: …`` capture/output lines (:func:`is_tool_noise`),
   keeping human-meaningful turns (user prompts, assistant answers, distilled LTM facts have no
   tool-name prefix).
2. **Dedupe by content** — one entry per distinct fact (whitespace/case-normalised), so a fact
   repeated across STM/MTM/LTM channels is injected once.
3. **Prefer promoted/salient** — the query-insensitive STM recency-FLOOR dump (``is_floor``) sinks
   below the genuinely-ranked hits; Python's STABLE sort preserves recall's own fused-score order
   WITHIN each group, so the relevance contract recall computed is respected, never overridden.

Determinism + speed matter because :func:`distill_items` runs on the inject hot path (every
``GET /recall/{session}`` and every silent-resource read) — it is pure, allocation-light, and does
no I/O.
"""

from __future__ import annotations

from collections.abc import Sequence

from mu_contracts.contracts.recall import RecallItemView

__all__ = ["NOISE_TOOL_NAMES", "distill_items", "is_tool_noise"]

# Claude Code / Codex tool names whose ``PostToolUse`` captures are stored as ``"<ToolName>: …"``
# (``capture/parsers.py::_map_event`` — ``text = f"{tool_name}: {_truncate(outcome)}"``). These are
# machine tool-capture / command-output lines, not human facts or decisions, so they are dropped
# from injected context. A ``mcp__<server>__<tool>`` capture (an MCP tool call) is likewise noise
# and matched by prefix below. Names NOT in this set (a user prompt, an assistant answer, a
# ``Note:``/``Decision:`` line) are KEPT — allow-by-default, deny only the known tool surface.
NOISE_TOOL_NAMES: frozenset[str] = frozenset(
    {
        "Bash",
        "BashOutput",
        "Read",
        "Write",
        "Edit",
        "MultiEdit",
        "NotebookEdit",
        "NotebookRead",
        "Glob",
        "Grep",
        "LS",
        "TodoWrite",
        "WebFetch",
        "WebSearch",
        "Task",
        "KillBash",
        "KillShell",
        "SlashCommand",
        "ExitPlanMode",
    }
)


def is_tool_noise(content: str) -> bool:
    """True when ``content`` is a tool-capture / tool-output line to DROP from injected context.

    Detects the ``"<ToolName>: <outcome>"`` shape the capture parser stores for a ``PostToolUse``
    event: the leading token before the FIRST colon is compared against :data:`NOISE_TOOL_NAMES`
    (and the ``mcp__`` MCP-tool prefix). Content with no colon, or whose leading token is not a
    known tool name (a plain fact, a ``Note:``/``Decision:`` line, a ``12:30`` timestamp, an
    ``http://`` URL), is NOT noise — the filter denies only the known machine tool surface, so a
    human-meaningful line is never silently dropped."""
    head = content.lstrip()
    name, sep, _ = head.partition(":")
    if not sep:
        return False
    name = name.strip()
    return name in NOISE_TOOL_NAMES or name.startswith("mcp__")


def _normalise(content: str) -> str:
    """Whitespace-collapsed, case-folded key for content-dedup — so ``"Ada  lives in Paris"`` and
    ``"ada lives in paris"`` collapse to one injected entry."""
    return " ".join(content.split()).casefold()


def distill_items(items: Sequence[RecallItemView]) -> list[RecallItemView]:
    """Return the distilled, injection-ready subset of ``items`` (order = injection order).

    Filters tool-noise, dedupes by normalised content, and sinks the query-insensitive STM
    recency-floor dump below the genuinely-ranked hits — all deterministic, allocation-light, and
    I/O-free (hot path). Recall's own fused-score ordering is preserved within the ranked and the
    floor groups (STABLE sort on ``is_floor``), so this re-shapes the injected view WITHOUT
    overriding the relevance recall computed."""
    ordered = sorted(items, key=lambda item: item.is_floor)
    kept: list[RecallItemView] = []
    seen: set[str] = set()
    for item in ordered:
        if is_tool_noise(item.content):
            continue
        key = _normalise(item.content)
        if not key or key in seen:
            continue
        seen.add(key)
        kept.append(item)
    return kept
