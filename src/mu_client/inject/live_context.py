"""The **context-aware assembler**: ``LiveSessionContext`` -> the lean ``RenderedContext.body``
(`live-session-context-design.md` §4/§5/§6, ratified as CANONICAL-CONTRACTS.md §7.22).

**Where this sits, and the plane ruling it rests on.** The DTO SHAPES are plane-agnostic and live
in mu-core (:mod:`mu_contracts.contracts.live_context`). The *state* is plane-hosted, and for the
FULL-LOCAL / HYBRID-primary row that plane is the **daemon** — `live-session-context-design.md:144`
("**daemon** ``WarmRecallCacheService`` (mu-client, Daemon-process scope)"), seconded by
`recall-service-design.md §2.2`'s two-warm-caches table, and already the shipped reality since
S3-02 (``recall_bridge.py:1-5``). This module is therefore the daemon-side assembler, extending the
ONE owner rather than standing up a second service (CANONICAL §7.22: "ONE owner =
``WarmRecallCacheService``").

**Only the unambiguous half is built here.** `rooms-sessions-subscriptions-spec.md:41`'s Package
cell reads ``mu-core(client)/mu-server`` against §3's ``mu-client``, and §3's own room row hands the
room instance to a ``RoomInjectionCoordinator`` that contradicts §3's opening "ONE owner" sentence
and CANONICAL §7.22. Both of those bear ONLY on the room / shared-fan-out row. That row is **not
built** — see :class:`SharedZone` handling below: the shared zone is modelled, guarded and rendered
(so a private fact reaching it is unrepresentable *before* anyone builds the fan-out), but nothing
here composes or fans out a room's shared zone. Reported, not picked.

**The four load-bearing mechanisms, in the order :func:`assemble` applies them:**

1. **§5.1 prompt-aware SELECTION** (`:181-182`) — recall already RANKED against the prompt; the
   assembler additionally uses the prompt as a relevance gate for which slabs earn a slot, so a
   slab the current prompt does not touch is *deprioritized before budget trimming*. Selection, not
   just ranking.
2. **§5.3 dedup vs already-in-context** (`:187-192`) — "the core lean mechanism". Skip any
   candidate whose ``content_hash`` is already in ``injected_digest`` (the host has it from an
   earlier inject), and render a fact that collides with a recency-floor slab ONCE, in
   ``<recent>``. Without this the object is a cache, not a lean-delta assembler.
3. **§5.2 per-section sub-budgets** (`:184-185`) — a pooled ceiling lets a long persona starve
   facts and a recall flood evict the floor. Trim lowest-salience from the MIDDLE of
   ``<recalled_memory>`` first; never the top fact, never an ``is_floor`` slab.
4. **§6 pointers in the state, bodies by id** (`:214-216`) — ``text=None`` + ``artifact_ref`` is a
   stub-plus-id; the body is hydrated by id at render time under budget, and a hydration the budget
   forbids emits a **NAMED marker**, never a silent drop (CANONICAL-CONTRACTS.md:640, gap **G5** —
   note the brief's "§7.10-G6" is off by one: G6 is reference-aware artifact retention).

**Trimming SPILLS; it does not discard.** Whenever selection/budget/hydration left anything out,
the assembler reports ``full_body`` alongside ``body`` and the bridge routes the difference through
the SAME F4 spill-to-file path an over-budget body already took (``recall_bridge._budget``), so
"named degrade, never a silent truncate" holds for the new trimming exactly as it did for the old
truncation. This is why per-section budgeting did not silently retire the F4 spill contract.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import datetime
from typing import Protocol

from mu_contracts.contracts.live_context import (
    SECTION_ORDER,
    ContextSlab,
    LiveSessionContext,
    PrivateSlice,
    Section,
    content_hash_of,
)
from mu_contracts.contracts.recall import RecallItemView
from mu_contracts.domain.model.memory import Namespace, Visibility
from pydantic import BaseModel, ConfigDict, Field, model_validator

__all__ = [
    "ArtifactHydratorPort",
    "AssembledContext",
    "DegradeSink",
    "LiveContextSettings",
    "SectionBudgets",
    "assemble",
    "prompt_tokens",
    "slab_from_recall_item",
    "update_recalled",
]

#: Word-ish tokens for the §5.1 relevance gate. Deterministic and allocation-light: this runs on
#: the inject hot path, the same constraint ``distill.py``'s module docstring states.
_TOKEN_RE = re.compile(r"[a-z0-9]+")

#: §5.1 must not let a stopword manufacture overlap between a prompt and every slab in the store —
#: that would make the gate a no-op while looking like it works. Length alone is not enough (a
#: 3-char cut keeps "the"/"and"/"for" while dropping the meaningful "api"/"sql"/"ttl"), so both a
#: length floor and a small explicit closed-class list are applied.
_MIN_TOKEN_CHARS = 3
_STOPWORDS: frozenset[str] = frozenset(
    {
        "the",
        "and",
        "for",
        "are",
        "was",
        "were",
        "you",
        "your",
        "our",
        "its",
        "with",
        "that",
        "this",
        "from",
        "into",
        "what",
        "when",
        "where",
        "which",
        "who",
        "how",
        "why",
        "does",
        "did",
        "has",
        "have",
        "had",
        "can",
        "will",
        "would",
        "should",
        "about",
        "there",
        "then",
        "than",
        "but",
        "not",
        "any",
        "all",
        "out",
        "get",
        "got",
        "let",
        "use",
        "using",
        "just",
    }
)

#: The named-degrade sink :func:`assemble` calls on every §6 non-hydration — ``(mode, detail)``.
#: Threaded in rather than imported so the ONE content-free ``log_degraded`` seam in the bridge
#: stays the only place this module's degrades reach a log (CLAUDE.md rule 3).
type DegradeSink = Callable[[str, str], None]

_MEMORY_CONTEXT_OPEN = "<memory_context>"
_MEMORY_CONTEXT_CLOSE = "</memory_context>"

#: The NAMED marker a persona brief cut to its §5.2 sub-budget leaves behind. Without it the brief
#: ends mid-sentence with nothing saying so, and the remainder — recorded as delivered under the
#: old full-brief hash — was never sent on any turn.
_PERSONA_TRUNCATED_MARKER = "- [persona brief truncated — inject budget]"


class SectionBudgets(BaseModel):
    """§5.2's per-section split (`live-session-context-design.md:185`), verbatim as the doc's
    stated default: "persona <=15%, session_state ~15%, recalled_memory ~40%, recent floor ~25%,
    references ~5%" — explicitly marked *ablate (G-CE2)*, hence a config surface, not constants.

    ``reasoning`` is not in the doc's split (``<reasoning>`` is opt-in, `:172`); it draws from the
    same pool and is given the remainder so the fractions sum to 1 and no section is funded by
    accident."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    persona: float = Field(default=0.15, gt=0, le=1)
    session_state: float = Field(default=0.15, gt=0, le=1)
    recalled_memory: float = Field(default=0.40, gt=0, le=1)
    recent: float = Field(default=0.25, gt=0, le=1)
    references: float = Field(default=0.04, gt=0, le=1)
    reasoning: float = Field(default=0.01, gt=0, le=1)

    @model_validator(mode="after")
    def _sums_to_one(self) -> SectionBudgets:
        total = sum(self.fractions.values())
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"section budget fractions must sum to 1.0, got {total}")
        return self

    @property
    def fractions(self) -> dict[Section, float]:
        return {
            Section.PERSONA: self.persona,
            Section.SESSION_STATE: self.session_state,
            Section.RECALLED_MEMORY: self.recalled_memory,
            Section.RECENT: self.recent,
            Section.REFERENCES: self.references,
            Section.REASONING: self.reasoning,
        }

    def allocate(self, total_chars: int) -> dict[Section, int]:
        """Chars per section. Floor-rounded: the sum is <= ``total_chars``, never over — the
        opposite rounding would make the sub-budgets a way to EXCEED the pooled ceiling."""
        return {s: int(total_chars * f) for s, f in self.fractions.items()}


class LiveContextSettings(BaseModel):
    """The assembler's knobs.

    **Delta, reported not hidden:** DEV-STANDARDS rule 3 ("no hardcoding — everything flows from
    the central config") wants these on ``InjectSettings`` so they reach ``MU_INJECT__*``. They are
    folded in there (``mu_client/config.py``) as ``InjectSettings.live_context``; this model is the
    shape, declared here rather than in ``config.py`` because ``config.py`` may not import from
    ``inject/`` (``inject/`` already imports ``config``) and a settings model belongs beside the
    code whose contract it is."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    budgets: SectionBudgets = Field(default_factory=SectionBudgets)

    #: G-LSC2 (`live-session-context-design.md:261`) flags this bound as OPEN — "the digest grows
    #: with every distinct injected fact; a multi-day session could accumulate thousands of
    #: hashes. Needs a bound (LRU by injected-at...)". The spec supplies no number, so this is a
    #: STATED default, chosen the same way ``warm_cache_max_entries`` was (``config.py:317-328``):
    #: 512 hashes ~= 32 KB of hex, and comfortably exceeds the number of distinct facts a real
    #: host window can hold at ``body_budget_chars=10_000`` per turn, so the bound bites only on a
    #: genuinely long-running session — which is exactly when G-LSC2 says it must.
    injected_digest_max: int = Field(default=512, ge=1)

    #: Below this, a ``<references>`` sub-budget cannot hold a meaningful hydrated body, so §6
    #: emits the named non-hydration marker instead of spending a store read to discover it.
    reference_min_chars: int = Field(default=64, ge=1)

    #: §5.1's gate is a DEPRIORITIZATION, never a drop (`:182` "deprioritized before budget
    #: trimming"). When the prompt matches nothing at all — a query-less re-warm, an opaque
    #: prompt — the gate must not silently empty the block, so it degrades to ranking order.
    prompt_gate_enabled: bool = True


class ArtifactHydratorPort(Protocol):
    """§6's by-id body read (`:216` "``ContextRepository.get(artifact_ref)`` on the owning plane
    ... daemon ``content_root`` on LOCAL").

    Structurally identical to ``mu_engine.storage.ports.ContextRepository.get_blob``
    (``mu-core/packages/mu-engine/src/mu_engine/storage/ports.py:327``), declared here as a narrow
    Protocol so this module depends on the one method it uses rather than on the engine's storage
    layer. **Nothing wires a real hydrator today**: neither ``LocalMemoryHost`` nor ``LocalMemory``
    exposes the container's ``ContextRepository`` (verified: no accessor in
    ``mu-core/packages/mu-local/src/mu_local/local_memory.py``), and ``daemon/app.py`` is outside
    this lane's file ownership. Absent a hydrator every pointer slab renders the NAMED marker —
    which is the correct, non-silent behavior, and is what the tests pin."""

    async def get_blob(self, ns: Namespace, artifact_id: str) -> bytes | None: ...


class AssembledContext(BaseModel):
    """One assembly pass's result.

    ``body`` is what the host sees on an ACCUMULATING channel — the §5.3 lean delta.
    ``snapshot_body`` is the same block assembled WITHOUT §5.3 arm 1, i.e. everything this state
    currently says, and it is what a REPLACING channel must read.

    **Why both exist, and why conflating them is a product break rather than a style choice.**
    §5.3's digest models what the host's own context window already holds, which is only true of a
    surface that ACCUMULATES — the Claude Code hook's ``additionalContext``, appended to a
    transcript. ``/ready-context``, ``MemoryLifecycleManager.ready_context`` and the MCP
    ``silent-context`` resource REPLACE their attached content on every read: handing them a delta
    makes the second read empty and the third emptier, and an empty ``ready_context`` with
    ``wired=True`` affirmatively claims the warm cache has nothing to say. Both bodies are
    assembled from ONE pass over the candidates (one hydration, memoized), so this costs string
    work, not store reads.

    ``full_body`` is what the block would have been with nothing trimmed or skipped for budget —
    equal to ``body`` when nothing was left out, and the F4 spill payload when something was.
    ``emitted_hashes`` is what §5.3 unions back into ``injected_digest``: exactly the hashes of the
    lines present in ``body`` after the pooled ceiling ran, never a hash of something the ceiling
    cut — that union is the bridge's call and not this function's."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    body: str
    snapshot_body: str
    full_body: str
    emitted_hashes: tuple[str, ...]
    #: Slabs that existed as candidates but were skipped by §5.3 dedup. Distinguishes "the host
    #: already has everything" (a lean no-op, `no_delta`) from "there was nothing to say" (`cold`)
    #: — two states that render identically as an empty body and must not be conflated.
    deduped_count: int = 0
    #: True when selection/budget/hydration left something out => the F4 spill path applies.
    trimmed: bool = False


def prompt_tokens(prompt: str | None) -> frozenset[str]:
    """The §5.1 relevance gate's token set. Short tokens are dropped (see ``_MIN_TOKEN_CHARS``)."""
    if not prompt:
        return frozenset()
    return frozenset(
        t
        for t in _TOKEN_RE.findall(prompt.casefold())
        if len(t) >= _MIN_TOKEN_CHARS and t not in _STOPWORDS
    )


def slab_from_recall_item(item: RecallItemView, *, visibility: Visibility) -> ContextSlab:
    """One ranked hit -> one :class:`ContextSlab`, section assigned by what the hit IS.

    * ``artifact_ref`` set  -> :attr:`Section.REFERENCES` as a **pointer slab** (``text=None``):
      §6 says a ``kind=reference`` body is never pre-inlined into the state, it is hydrated by id
      at render time under budget. The surface DTO already carries ``artifact_ref``
      (``mu_contracts/contracts/recall.py:97``) and the inject path has never read it until now.
    * ``is_floor``         -> :attr:`Section.RECENT`, the verbatim recency floor (recall §1.3).
    * otherwise            -> :attr:`Section.RECALLED_MEMORY`, the answer-bearing band.

    ``content_hash`` prefers an engine-supplied hash if the item ever carries one and falls back to
    the render-side key — see :func:`~mu_contracts.contracts.live_context.content_hash_of` for why
    the canonical engine key is not reachable at this boundary today, and why the render-side key
    is in fact the right question for §5.3.
    """
    engine_hash = getattr(item, "content_hash", None)
    if item.artifact_ref is not None:
        section, text = Section.REFERENCES, None
    elif item.is_floor:
        section, text = Section.RECENT, item.content
    else:
        section, text = Section.RECALLED_MEMORY, item.content
    return ContextSlab(
        slab_id=item.memory_id,
        content_hash=engine_hash or content_hash_of(item.content),
        text=text,
        artifact_ref=item.artifact_ref,
        section=section,
        visibility=visibility,
        salience=item.rerank_score if item.rerank_score is not None else item.fused_score,
        is_floor=item.is_floor,
        tier=item.tier.value,
        provenance_ids=(item.memory_id,),
    )


def update_recalled(
    state: LiveSessionContext,
    *,
    principal_id: str,
    slabs: Sequence[ContextSlab],
    prompt_hash: str | None,
    now: datetime,
) -> LiveSessionContext:
    """§4 `:155`: **"Recall UPDATES the live context, it does not replace it."**

    Writes the ``recalled`` / ``recency_floor`` / reference slots of ONE principal's slice and
    **preserves** ``persona_brief``, ``reasoning_register``, ``tool_state`` and ``injected_digest``
    — the four slots recall knows nothing about, and whose survival across a recall is the whole
    difference between a structured state and a from-scratch recompute. Recall is one writer among
    several (the MLM sweep is another, §4 `:157`), not the owner of the object.

    Reference pointer slabs ride in ``recalled``: they are recall hits like any other, and §6
    hydrates them at RENDER time, not here.
    """
    slice_ = state.slice_for(principal_id)
    floor = tuple(s for s in slabs if s.is_floor)
    ranked = tuple(s for s in slabs if not s.is_floor)
    updated = slice_.model_copy(update={"recalled": ranked, "recency_floor": floor})
    return state.with_slice(principal_id, updated, now=now).model_copy(
        update={"last_prompt_hash": prompt_hash}
    )


def _relevance(slab: ContextSlab, tokens: frozenset[str]) -> int:
    """§5.1: how many prompt tokens this slab's text touches. A pointer slab has no text to match
    on, so it scores on its section's own merit and is ordered by salience alone — never gated to
    zero for lacking a body it is DEFINED as not having (§6)."""
    if slab.is_pointer or not tokens:
        return 0
    return len(tokens.intersection(_TOKEN_RE.findall((slab.text or "").casefold())))


def _select(
    slabs: Iterable[ContextSlab], *, tokens: frozenset[str], enabled: bool
) -> list[ContextSlab]:
    """§5.1 prompt-aware selection, as a stable DEPRIORITIZATION (`:182`).

    Slabs the prompt touches sort first, by (overlap desc, salience desc); slabs it does not touch
    keep their salience order behind them. Nothing is dropped here — dropping happens in §5.2 under
    budget, which is the order the spec states ("deprioritized *before* budget trimming"). The
    gate is skipped entirely when the prompt matches nothing, so a query-less re-warm degrades to
    pure ranking rather than to an arbitrary reshuffle."""
    ranked = list(slabs)
    scored = [(_relevance(s, tokens), s) for s in ranked]
    if not enabled or not any(score for score, _ in scored):
        return sorted(ranked, key=lambda s: -s.salience)
    return [s for _, s in sorted(scored, key=lambda pair: (-pair[0], -pair[1].salience))]


class _Line(BaseModel):
    """One rendered line, with the two facts the ceiling pass and §5.3 need about it.

    **``content_hash`` is the line's identity in ``injected_digest``, and a line that carries one
    is a line the host actually received.** The hash is attached HERE, at the moment the text is
    rendered, and read back off the SURVIVING lines after the pooled ceiling has run — never
    collected up-front from the candidate slabs. That ordering is the whole fix for the class of
    defect this file keeps attracting: a hash recorded for text that never reached the host makes
    that fact un-injectable for the rest of the session — present in the tiers, absent from every
    block, and named in no log.

    ``protected`` marks the lines §5.2 forbids trimming: the top fact of ``<recalled_memory>``
    and every ``is_floor`` slab. The ceiling drops every unprotected line in the block before it
    touches one of them."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    text: str
    content_hash: str | None = None  # None => a MARKER line: never enters the digest
    protected: bool = False


#: The order the pooled ceiling sacrifices sections in — lowest value first, mirroring §5.2's own
#: priorities read backwards: bulky hydrated references (F4) go before opt-in reasoning, before the
#: mid-band of recalled facts, before session state, before the persona lens, and the verbatim
#: recency floor goes LAST because §5.2 `:185` makes it the hard minimum.
_CEILING_DROP_ORDER: tuple[Section, ...] = (
    Section.REFERENCES,
    Section.REASONING,
    Section.RECALLED_MEMORY,
    Section.SESSION_STATE,
    Section.PERSONA,
    Section.RECENT,
)


def _omission_marker(count: int) -> _Line:
    """The NAMED marker a ceiling drop leaves behind. Content-free (a count, no text), and
    ``content_hash=None`` so it can never be mistaken for delivered content."""
    return _Line(text=f"- [{count} item(s) omitted — inject budget]")


def _fit(
    slabs: Sequence[ContextSlab], *, budget: int, protect_floor: bool
) -> tuple[list[_Line], bool]:
    """§5.2's trimming, for one section. Returns ``(rendered lines, trimmed)``.

    **Each emitted line carries its own hash.** §5.3 unions the emitted hashes into
    ``injected_digest``, and trimming skips from the MIDDLE — so "the first N slabs" is not "the
    slabs that were emitted". Inferring it marks trimmed slabs as already-injected, and a fact that
    never reached the host is then suppressed for the rest of the session: present in the tiers,
    absent from every block, and invisible in every log. The lean mechanism silently becoming a
    lossy one is the worst failure available to this file, so identity travels WITH the line.

    **"Trim lowest-salience from the middle band of ``<recalled_memory>`` first; never the top
    fact, never an ``is_floor=True`` slab"** (`:185`). Realized exactly:

    * the FIRST slab in the (already selected) order is emitted unconditionally — that is "the top
      fact", and a budget that cannot hold it is a misconfiguration, not a reason to emit nothing;
    * an ``is_floor`` slab is emitted unconditionally when ``protect_floor`` — the recency floor is
      a hard minimum that OVERDRAWS its section rather than being trimmed (recall §1.3/§2.3), which
      is what "never trimmed" has to mean if it means anything;
    * everything else is admitted in order until the section budget is spent, so what falls out is
      the lowest-priority middle band.

    **The overdraw is a section-level licence, not a licence on the whole block.** Both exemptions
    above let a section exceed its own sub-budget; neither may let the assembled block exceed the
    inject ceiling, which is a property of the HOST's window and not of this allocation. That
    second bound is :func:`_enforce_ceiling`'s job, and the two together are how §5.2's "never
    trimmed" and "per-section ceiling" stop contradicting each other.
    """
    lines: list[_Line] = []
    used = 0
    trimmed = False
    for index, slab in enumerate(slabs):
        text = f"- {slab.text}"
        must_emit = index == 0 or (protect_floor and slab.is_floor)
        if not must_emit and used + len(text) > budget:
            trimmed = True
            continue
        lines.append(_Line(text=text, content_hash=slab.content_hash, protected=must_emit))
        used += len(text)
    return lines, trimmed


def _dedup(slabs: Sequence[ContextSlab], *, seen: set[str]) -> tuple[list[ContextSlab], int]:
    """§5.3, both arms, in one pass over ``seen``.

    ``seen`` starts as ``injected_digest`` (arm 1 — "already in the host's context window from an
    earlier inject", `:189`) and is grown with the recency-floor hashes BEFORE the recalled band is
    filtered (arm 2 — a recalled fact colliding with a floor slab is "rendered once (in
    ``<recent>``, verbatim) and dropped from ``<recalled_memory>``", `:190`). Ordering the sections
    floor-first is what makes arm 2 fall out of arm 1's machinery instead of needing its own.

    The SNAPSHOT pass (:class:`AssembledContext`) runs the same function with an EMPTY ``seen``:
    arm 2 still applies (a block must not say the same thing twice), arm 1 does not (a replacing
    channel holds nothing from last turn)."""
    kept: list[ContextSlab] = []
    skipped = 0
    for slab in slabs:
        if slab.content_hash in seen:
            skipped += 1
            continue
        seen.add(slab.content_hash)
        kept.append(slab)
    return kept, skipped


async def _hydrate(
    slabs: Sequence[ContextSlab],
    *,
    ns: Namespace,
    budget: int,
    settings: LiveContextSettings,
    hydrator: ArtifactHydratorPort | None,
    degrade: DegradeSink,
    memo: dict[str, bytes | None],
) -> tuple[list[_Line], bool]:
    """§6 — bodies by id, at render time, under budget, with a NAMED marker on every path that
    does not produce a body (CANONICAL-CONTRACTS.md:640 G5; `recall-service-design.md §2.1`'s
    ``[reference {ref} not expanded — inject budget]`` inline marker **plus** the
    ``DegradedModeEntered(component="inject", mode="artifact_hydration_skipped",
    reason=ARTIFACT_HYDRATION_BUDGET)`` event).

    The marker is emitted for all three non-hydration paths and they are distinguished by ``mode``,
    never collapsed: budget-forbidden, hydrator-not-wired, artifact-absent. **A silent drop is
    indistinguishable from "there was nothing to hydrate"** — which is precisely why the inline
    marker (visible to the model) and the event (visible to the operator) are both required, not
    either-or.

    **Only the BUDGET paths set ``trimmed``.** "Not wired" and "absent" produce no body on any
    budget, so there is nothing for the F4 spill to spill and nothing a bigger ceiling would
    recover — reporting them as a trim made the daemon write a spill file, containing real memory
    content, on the ordinary path of every render that carried one pointer slab (no hydrator is
    wired today, so that is EVERY such render), and told the operator "over budget" about a body at
    1.6% of budget. A named absence is not a trim.

    ``memo`` caches the store read by ``artifact_ref`` so the delta pass and the snapshot pass cost
    one read between them, not two.

    ``ARTIFACT_HYDRATION_BUDGET`` is reused rather than extended per §9 item 7 ("No new
    ``DegradeReason``"), and CANONICAL-CONTRACTS.md:142 already reads it as covering "reference
    body not expanded (inject/answer budget) **or artifact absent**".
    """
    lines: list[_Line] = []
    used = 0
    trimmed = False
    for slab in slabs:
        ref = slab.artifact_ref
        if slab.text is not None:  # already-hydrated reference; no store read needed
            text = f"- {slab.text}"
            if used + len(text) <= budget or not lines:
                lines.append(_Line(text=text, content_hash=slab.content_hash))
                used += len(text)
            else:
                trimmed = True
            continue
        remaining = budget - used
        if hydrator is None:
            lines.append(_Line(text=_marker(ref, "not expanded — no artifact store wired")))
            degrade("artifact_hydration_unavailable", f"slab={slab.slab_id}")
            continue
        if remaining < settings.reference_min_chars:
            lines.append(_Line(text=_marker(ref, "not expanded — inject budget")))
            degrade("artifact_hydration_skipped", f"remaining={remaining}")
            trimmed = True
            continue
        key = ref or ""
        if key not in memo:
            memo[key] = await hydrator.get_blob(ns, key)
        blob = memo[key]
        if blob is None:
            lines.append(_Line(text=_marker(ref, "not expanded — artifact absent")))
            degrade("artifact_hydration_unavailable", f"slab={slab.slab_id}")
            continue
        body_text = blob.decode("utf-8", errors="replace")
        body_line = f"- {body_text}"
        if len(body_line) > remaining:
            # Deliberately NOT a truncated body: half an artifact presented as the artifact is a
            # silent lie, worse than the honest marker (§6 / G5).
            lines.append(_Line(text=_marker(ref, "not expanded — inject budget")))
            degrade("artifact_hydration_skipped", f"needed={len(body_line)} have={remaining}")
            trimmed = True
            continue
        # The hash is the HYDRATED body's, not the pointer slab's: what the host received is this
        # text, and §5.3's question is only ever "does the host already have THIS text".
        lines.append(_Line(text=body_line, content_hash=content_hash_of(body_text)))
        used += len(body_line)
    return lines, trimmed


def _marker(artifact_ref: str | None, why: str) -> str:
    """The inline NAMED non-hydration marker (`recall-service-design.md §2.1`, CANONICAL G5)."""
    return f"- [reference {artifact_ref} {why}]"


def _render(sections: Mapping[Section, Sequence[str]]) -> str:
    """§4's FORMAT invariant (`:161-173`), and nothing else.

    Fixed section sequence from :data:`SECTION_ORDER`; an empty section is OMITTED (absence is the
    house rule — an empty ``<persona></persona>`` spends tokens to say nothing); **no timestamp or
    volatile id anywhere, least of all at the top** — a volatile field there churns the etag every
    turn and destroys the KV-cache reuse §5.4's whole etag gate exists to buy (F6). Render-time
    metadata rides ``RenderedContext.computed_at``, out of band (`:173`)."""
    parts: list[str] = []
    for section in SECTION_ORDER:
        lines = sections.get(section) or ()
        if not lines:
            continue
        parts.append(f"<{section.value}>")
        parts.extend(lines)
        parts.append(f"</{section.value}>")
    if not parts:
        return ""
    return "\n".join((_MEMORY_CONTEXT_OPEN, *parts, _MEMORY_CONTEXT_CLOSE))


def _render_lines(sections: Mapping[Section, Sequence[_Line]]) -> str:
    return _render({section: [line.text for line in lines] for section, lines in sections.items()})


def _drop_one(kept: dict[Section, list[_Line]], dropped: dict[Section, int]) -> bool:
    """Sacrifice exactly one line, lowest-priority first. Returns False when nothing is left.

    Two passes: every UNPROTECTED line in :data:`_CEILING_DROP_ORDER` goes before any protected one
    is touched, and within a section the LAST line goes first — the lowest-salience tail of
    ``<recalled_memory>`` and the oldest entry of ``<recent>``."""
    for protected_too in (False, True):
        for section in _CEILING_DROP_ORDER:
            lines = kept.get(section)
            if not lines:
                continue
            for index in range(len(lines) - 1, -1, -1):
                if lines[index].protected and not protected_too:
                    continue
                lines.pop(index)
                dropped[section] = dropped.get(section, 0) + 1
                return True
    return False


def _with_markers(
    kept: Mapping[Section, list[_Line]], dropped: Mapping[Section, int]
) -> dict[Section, list[_Line]]:
    out: dict[Section, list[_Line]] = {}
    for section in SECTION_ORDER:
        lines = list(kept.get(section) or ())
        count = dropped.get(section, 0)
        if count:
            lines.append(_omission_marker(count))
        if lines:
            out[section] = lines
    return out


def _enforce_ceiling(
    sections: Mapping[Section, list[_Line]], *, budget: int, degrade: DegradeSink
) -> tuple[dict[Section, list[_Line]], bool]:
    """The POOLED ceiling §5.2's sub-budgets do not by themselves impose — and the reason nothing
    downstream has to truncate a rendered block ever again.

    §5.2 allocates per section so "a long persona cannot starve facts and a recall flood cannot
    evict the floor", and §5.2 also says the floor is never trimmed. Both exemptions in
    :func:`_fit` therefore let a section OVERDRAW its allocation, which means the sub-budgets alone
    bound nothing: 20 protected floor slabs against a 400-char ceiling rendered 4363 chars and
    reported ``trimmed=False``. Downstream, that was cut to length by a byte slice — which cut
    mid-line, dropped the closing ``</recent>`` and ``</memory_context>`` tags (breaking §4's
    ordered-XML FORMAT invariant, CANONICAL §7.22), and left the caller unioning the hashes of
    facts the host never received.

    So the block is bounded HERE, where identity is still attached to text: whole lines are
    dropped, never bytes; the drop is NAMED with a per-section count marker; and the hashes the
    caller unions are read off the lines that survive. §5.2's "never trimmed" is honored as a
    PRIORITY — every unprotected line in the block goes before one floor line does — rather than as
    an absolute that a fixed-size host window cannot actually grant.

    Returns ``(sections, dropped_anything)``.
    """
    kept = {section: list(lines) for section, lines in sections.items()}
    dropped: dict[Section, int] = {}
    while True:
        view = _with_markers(kept, dropped)
        if len(_render_lines(view)) <= budget:
            return view, bool(dropped)
        if not _drop_one(kept, dropped):
            # Not reachable from any sane config: it means the ceiling cannot hold even the
            # omission markers. Named, and empty — an empty block is honest; an over-budget one
            # would be handed to the host and cut by someone else.
            degrade("inject_ceiling_exhausted", f"budget={budget}")
            return {}, True


async def _build_sections(
    state: LiveSessionContext,
    slice_: PrivateSlice,
    *,
    seen: set[str],
    tokens: frozenset[str],
    budgets: Mapping[Section, int],
    settings: LiveContextSettings,
    hydrator: ArtifactHydratorPort | None,
    degrade: DegradeSink,
    memo: dict[str, bytes | None],
) -> tuple[dict[Section, list[_Line]], bool, int]:
    """One pass: ``SharedZone ⊕ private[principal]`` -> per-section lines, under §5.1/§5.2/§5.3/§6.

    Called TWICE by :func:`assemble` — once with ``seen`` seeded from ``injected_digest`` (the
    delta) and once with an empty ``seen`` (the snapshot). The ``memo`` makes the second pass free
    of store I/O. Returns ``(sections, trimmed, deduped_count)``.
    """
    trimmed = False
    deduped = 0
    lines: dict[Section, list[_Line]] = {}

    # --- <persona> (private). A brief is one blob, not slabs; deduped by its own hash so an
    # unchanged persona is not re-shouted every turn (§5.3 arm 1 applies to it like anything else).
    if slice_.persona_brief:
        budget = budgets[Section.PERSONA]
        brief = slice_.persona_brief
        over = len(brief) > budget
        if over:
            brief = brief[:budget]
        # The hash is of the text that is ACTUALLY RENDERED, never of the full brief. Hashing the
        # full brief and emitting a prefix recorded the whole persona as delivered: next turn the
        # <persona> section deduped away entirely and the remainder was never sent on any turn.
        digest = content_hash_of(brief)
        if digest in seen:
            deduped += 1
        else:
            seen.add(digest)
            persona_lines = [_Line(text=brief, content_hash=digest)]
            if over:
                trimmed = True
                degrade(
                    "persona_brief_truncated",
                    f"chars={len(slice_.persona_brief)} budget={budget}",
                )
                persona_lines.append(_Line(text=_PERSONA_TRUNCATED_MARKER))
            lines[Section.PERSONA] = persona_lines

    # --- <session_state> (SHARED running summary + open threads, ⊕ this principal's tool state).
    # §4 `:164`. The shared half is read straight off the guarded zone: every slab in it is
    # SHARED by construction (SharedZone's validator), so no filtering is needed or attempted here
    # — a filter would imply the zone might hold a private slab, and it may not.
    #
    # These lines carry hashes and go through ``seen`` like every other line. Exempting them made
    # the running summary — the largest stable block in the state after the persona brief — be
    # re-shouted verbatim on every single turn, which is the exact cost §5.3 exists to remove.
    state_lines: list[tuple[str, str]] = []
    if state.shared.running_summary:
        state_lines.append((f"- {state.shared.running_summary}", state.shared.running_summary))
    state_lines.extend((f"- {thread}", thread) for thread in state.shared.open_threads)
    state_lines.extend(
        (
            f"- {tool.kind} {tool.label}: {tool.status}"
            + (f" (result {tool.result_ref})" if tool.result_ref else ""),
            f"{tool.correlation_id}:{tool.kind}:{tool.label}:{tool.status}:{tool.result_ref}",
        )
        for tool in slice_.tool_state
    )
    if state_lines:
        budget = budgets[Section.SESSION_STATE]
        kept: list[_Line] = []
        used = 0
        for text, hashed in state_lines:
            digest = content_hash_of(hashed)
            if digest in seen:
                deduped += 1
                continue
            if kept and used + len(text) > budget:
                trimmed = True
                continue
            seen.add(digest)
            kept.append(_Line(text=text, content_hash=digest))
            used += len(text)
        if kept:
            lines[Section.SESSION_STATE] = kept

    # --- <recent> BEFORE <recalled_memory>: §5.3 arm 2 needs the floor hashes in ``seen`` before
    # the recalled band is filtered, so a fact the host just said is rendered ONCE, verbatim, here.
    floor = tuple(state.shared.recent_shared) + tuple(slice_.recency_floor)
    floor_kept, floor_deduped = _dedup(floor, seen=seen)
    deduped += floor_deduped
    if floor_kept:
        recent_lines, recent_trimmed = _fit(
            floor_kept, budget=budgets[Section.RECENT], protect_floor=True
        )
        trimmed = trimmed or recent_trimmed
        if recent_lines:
            lines[Section.RECENT] = recent_lines

    # --- <recalled_memory>: §5.1 selection, then §5.3 arm 1+2, then §5.2 trimming.
    recalled = [s for s in slice_.recalled if s.section is Section.RECALLED_MEMORY]
    selected = _select(recalled, tokens=tokens, enabled=settings.prompt_gate_enabled)
    kept_recalled, recalled_deduped = _dedup(selected, seen=seen)
    deduped += recalled_deduped
    if kept_recalled:
        recall_lines, recall_trimmed = _fit(
            kept_recalled, budget=budgets[Section.RECALLED_MEMORY], protect_floor=False
        )
        trimmed = trimmed or recall_trimmed
        if recall_lines:
            lines[Section.RECALLED_MEMORY] = recall_lines

    # --- <references>: §6 pointers hydrated by id, named marker on every non-hydration.
    refs = [s for s in slice_.recalled if s.section is Section.REFERENCES]
    ref_kept, ref_deduped = _dedup(refs, seen=seen)
    deduped += ref_deduped
    if ref_kept:
        ref_lines, ref_trimmed = await _hydrate(
            ref_kept,
            ns=state.namespace,
            budget=budgets[Section.REFERENCES],
            settings=settings,
            hydrator=hydrator,
            degrade=degrade,
            memo=memo,
        )
        trimmed = trimmed or ref_trimmed
        if ref_lines:
            lines[Section.REFERENCES] = ref_lines

    # --- <reasoning>: opt-in, bottom-adjacent (§7). Distilled DECISION lines only — raw CoT is
    # never re-injected (`:226`).
    reasoning, reasoning_deduped = _dedup(slice_.reasoning_register, seen=seen)
    deduped += reasoning_deduped
    if reasoning:
        reason_lines, reason_trimmed = _fit(
            reasoning, budget=budgets[Section.REASONING], protect_floor=False
        )
        trimmed = trimmed or reason_trimmed
        if reason_lines:
            lines[Section.REASONING] = reason_lines

    return lines, trimmed, deduped


def _emitted(sections: Mapping[Section, Sequence[_Line]]) -> tuple[str, ...]:
    """§5.3's union set, read off the SURVIVING lines in FORMAT order.

    Every hash here belongs to a line that is in the body the caller is about to hand the host.
    Marker lines carry no hash, so a ``[reference … not expanded]`` or ``[N item(s) omitted]``
    never claims delivery of the thing it is reporting the absence of."""
    seen: set[str] = set()
    out: list[str] = []
    for section in SECTION_ORDER:
        for line in sections.get(section) or ():
            if line.content_hash is None or line.content_hash in seen:
                continue
            seen.add(line.content_hash)
            out.append(line.content_hash)
    return tuple(out)


async def assemble(
    state: LiveSessionContext,
    *,
    principal_id: str,
    prompt: str | None,
    budget_chars: int,
    settings: LiveContextSettings,
    hydrator: ArtifactHydratorPort | None = None,
    degrade: DegradeSink | None = None,
) -> AssembledContext:
    """``LiveSessionContext`` -> the lean block, applying §5.1 -> §5.3 -> §5.2 -> §6 -> §4.

    **The one privacy rule this function exists to hold.** It renders ``SharedZone ⊕
    private[principal_id]`` and reads **exactly one** private slice — via
    :meth:`LiveSessionContext.slice_for`, never by iterating ``state.private``. §1 `:107`: "each
    participant's host receives ``SharedZone ⊕ private[self]``, never another member's slice."
    Iterating the dict here is the single edit that would turn this into a cross-principal
    disclosure, so there is no loop over ``private`` in this module at all.

    **Two bodies, one pass over the stores.** ``body`` is §5.3's lean delta, for the accumulating
    host channel; ``snapshot_body`` is the same state with arm 1 switched off, for the replacing
    pull channels (see :class:`AssembledContext`). The second pass re-runs pure string work and
    reuses the first pass's hydration through ``memo``.

    **``len(body) <= budget_chars`` is a guarantee, not an aspiration.** The pooled ceiling runs
    inside this function (:func:`_enforce_ceiling`) precisely so no caller has to cut a rendered
    block afterwards — a byte-slice downstream breaks §4's XML and silently un-injects whatever it
    cut."""
    slice_: PrivateSlice = state.slice_for(principal_id)
    tokens = prompt_tokens(prompt)
    budgets = settings.budgets.allocate(budget_chars)
    memo: dict[str, bytes | None] = {}

    def _degrade(mode: str, detail: str) -> None:
        if degrade is not None:
            degrade(mode, detail)

    def _quiet(mode: str, detail: str) -> None:
        """The snapshot pass reports nothing: it renders the same candidates the delta pass just
        reported on, and emitting each degrade twice would double-count every named absence in the
        operator's view of one render."""

    delta_sections, delta_trimmed, deduped = await _build_sections(
        state,
        slice_,
        seen=set(slice_.digest_set),
        tokens=tokens,
        budgets=budgets,
        settings=settings,
        hydrator=hydrator,
        degrade=_degrade,
        memo=memo,
    )
    delta_sections, delta_cut = _enforce_ceiling(
        delta_sections, budget=budget_chars, degrade=_degrade
    )
    body = _render_lines(delta_sections)
    trimmed = delta_trimmed or delta_cut

    snapshot_sections, _, _ = await _build_sections(
        state,
        slice_,
        seen=set(),
        tokens=tokens,
        budgets=budgets,
        settings=settings,
        hydrator=hydrator,
        degrade=_quiet,
        memo=memo,
    )
    snapshot_sections, _ = _enforce_ceiling(snapshot_sections, budget=budget_chars, degrade=_quiet)
    snapshot_body = _render_lines(snapshot_sections)

    return AssembledContext(
        body=body,
        snapshot_body=snapshot_body,
        full_body=_full_render(state, slice_) if trimmed else body,
        emitted_hashes=_emitted(delta_sections),
        deduped_count=deduped,
        trimmed=trimmed,
    )


def _full_render(state: LiveSessionContext, slice_: PrivateSlice) -> str:
    """The untrimmed block — the F4 spill payload (module docstring). Same FORMAT, same one-slice
    privacy rule, no budget and no dedup: this is what the host WOULD have seen, written to the
    spill file so "never a silent truncate" survives the move from truncation to trimming.
    Pointer slabs render as their marker: the spill file must not become a second, unbudgeted
    hydration path."""
    lines: dict[Section, list[str]] = {}
    if slice_.persona_brief:
        lines[Section.PERSONA] = [slice_.persona_brief]
    state_lines: list[str] = []
    if state.shared.running_summary:
        state_lines.append(f"- {state.shared.running_summary}")
    state_lines.extend(f"- {t}" for t in state.shared.open_threads)
    if state_lines:
        lines[Section.SESSION_STATE] = state_lines
    floor = tuple(state.shared.recent_shared) + tuple(slice_.recency_floor)
    if floor:
        lines[Section.RECENT] = [f"- {s.text}" for s in floor]
    recalled = [s for s in slice_.recalled if s.section is Section.RECALLED_MEMORY]
    if recalled:
        lines[Section.RECALLED_MEMORY] = [f"- {s.text}" for s in recalled]
    refs = [s for s in slice_.recalled if s.section is Section.REFERENCES]
    if refs:
        lines[Section.REFERENCES] = [
            f"- {s.text}" if s.text is not None else _marker(s.artifact_ref, "not expanded")
            for s in refs
        ]
    if slice_.reasoning_register:
        lines[Section.REASONING] = [f"- {s.text}" for s in slice_.reasoning_register]
    return _render(lines)
