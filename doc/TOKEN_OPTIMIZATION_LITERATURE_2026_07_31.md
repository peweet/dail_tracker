# Token-optimization literature review — full working notes (2026-07-31)

Rescued from the review session's scratchpad (session `0aa403c7`, 2026-07-31) so the
full per-paper idea inventory outlives the temp directory. The **durable summary +
adopted/rejected/open verdicts** live in memory:
`reference_token_optimization_literature_2026_07_31` — read that first; this file is
the supporting detail (per-paper numbers and the E/T/S/B/C/K/H/P idea codes it cites).
Full reader-agent transcripts remain only inside the session jsonl
(`~/.claude/projects/c--Users-pglyn-PycharmProjects-dail-extractor/0aa403c7-*.jsonl`).

All claims [Reported — reader agents, full-text fetch] unless marked otherwise.

---


<!-- ══════════ source: scratchpad/ideas_inventory.md ══════════ -->

# Ideas inventory — per-paper (compiled as readers return, 2026-07-31)

All entries [Reported — reader agent, full-HTML fetch] unless noted. Full reports in tasks/*.output.

## Reader E — determinism + position (arXiv:2605.28840, 2506.09501, 2502.17204, 2406.15981) ✔

### Behavioral reproducibility (2605.28840, Yagubyan)
Numbers: 1.6–3.3 distinct tool-sequences per 10 identical runs at deployed temp; tool-seq similarity 0.87 vs argument consistency 0.69; 60% of divergence in steps 1–2; N=10 → split-half reliability r=0.66 (paper recommends N≥30); high trajectory-consistency predicts 90.2% vs 61.2% correctness.
- E1: n=1 A/B benches can't distinguish config from noise → ≥5 runs/arm, decide only if delta > within-arm spread.
- E2: diff TOOL-CALL SEQUENCES, not transcripts/token counts (text <5% exact-match even on identical behavior).
- E3: cheap probe = compare only the FIRST TWO tool calls across 3–5 reruns (carries 60% of the signal).
- E4: high run-to-run spread ⇒ rewrite the task brief first (ambiguity d=0.74 beats model choice, n.s.).
- E5: no-ground-truth benches: use trajectory agreement as the score.

### Numerical nondeterminism (2506.09501, Yuan et al.)
Numbers: temp-0 BF16 accuracy std 9.15pp; output-LENGTH std 9,189 tok; >90% of examples diverge under BF16 vs 2.2% FP32. All mitigations serving-side.
- E6: temp-0 buys nothing for an API user; only lever = repetition + error bars.
- E7: converges with E1: ≥5 runs/arm, record spread in cost_of_change.jsonl ledger.
- E8: token/cost metrics are the WORST-case n=1 metric (length variance is the largest measured quantity) — the C1–C7 single-run measurements are Indicative-grade.

### Order Matters (2502.17204, ACL-F 2025) — ID confirmed
Numbers: hard-to-easy constraint order 53.3%→61.0% (~7pp, LLaMA3-8B); ~25% improvement multi-round; per-constraint accuracy ≈ attention received; content-constraints REVERSE (81→75 hard-first).
- E9: hardest/highest-consequence rules FIRST in CLAUDE.md (never-sum, never-Read-data, provenance) — currently third section.
- E10: multi-turn amplifies order effects (~25% vs 5–7pp) — Claude Code sessions are long multi-turn, so payoff is larger.
- E11: violated rule ⇒ MOVE it earlier, don't add prose (prose dilutes attention).

### Serial Position Effects (2406.15981, ACL-F 2025) — ID confirmed
Numbers: primacy in 73/104 instances, strengthens with length; "attend to middle" prompts ≈ zero effect; enumerate-then-decide partially mitigates (sometimes backwards); architecture dominates prompt (ARI 0.18–0.39 vs ~0).
- E12: first position strong, last second-best, MIDDLE is where rules die → safety rules mid-file are in the weakest slot.
- E13: never add "remember the rules above" lines — physically move the rule.
- E14: checklist/table format (enumerate-then-route) helps mid-file survival — our routing table is the right shape.
Caveats: 2023–24-era models; CLAUDE.md mapping is Extracted (reader's inference), direction-only for Claude.

### Cross-paper anchors
- A/B trustworthiness: 5 runs/arm pragmatic floor (both mechanisms agree); prefer structural metrics.
- Rule placement: first > last > middle; enumerate-then-act checklists partially rescue the middle.

## Reader D — prompt-side levers (2412.18547 TALE, 2407.16833 Self-Route) ✔

### TALE (2412.18547v5)
Numbers: budget line = "Let's think step by step and use less than [N] tokens:"; avg 461→149 output tok (−67%) at <3pp accuracy; GSM8K accuracy ROSE 81.35→84.46%; elasticity U-shape: budget 10 → 157 actual tok vs budget 50 → 86 (too-tight budgets BACKFIRE); estimator lands in ideal range ~61% of cases; optimal budget is task-complexity-dependent (≈20 tok trivial → ≈260 college math).
- T1: NUMERIC token ceiling in subagent prompt templates ("report in under 400 tokens"), not "be concise".
- T2: parent sets budget PER BRIEF (already knows task shape — estimator call unnecessary here).
- T3: elasticity floor — if an agent blows the budget, RAISE it into range; punitive tiny ceilings backfire (caution for flood_warn-style thresholds).
- T4: direct-answer arm empirically backs the R0 register rule (lookup questions: reasoning buys nothing).
- T5: offline binary-search a recurring brief's budget once, hard-code in template (cost_of_change bench could do this).
Not applicable: TALE-PT (fine-tuning), online estimator round-trip.

### Self-Route (2407.16833, DeepMind)
Numbers: LC beats RAG avg 49.7 vs 37.3 (Gemini-1.5-Pro); 63% of queries → identical answers both paths; self-declared "unanswerable" routing recovers ~99% LC quality at 38–61% cost; k-ablation: k=50 RAG costs 95% of LC while still losing; RAG failure taxonomy: multi-hop / broad-summary / unparseable query / implicit whole-doc.
- S1: codify the ESCALATION CLAUSE in the routing reflex: "if the small tool can't answer, SAY SO and Read the span — don't loop more tool calls" (self-declared insufficiency is the router).
- S2: add routing-table row: whole-module/whole-corpus comprehension questions → Read or explore directly, NOT iterated small tools (the 4 failure shapes).
- S3: escalate after 2–3 failed scoped searches — iterated retrieval passes the cost of just reading the file fast.
- S4: don't tool-fragment SMALL files — Read beats snippets when it fits; MCP path earns its keep on parquet/1,500-line files.
- S5: optional: tag escalations (MCP→Read) in the ledger; recurring escalations per tool = that tool needs a better return shape.
Not applicable: dense retrievers/chunking/k-tuning (we're BM25+structured), literal cost %s.

## Reader B — context management (2606.10209, 2602.04284, 2604.01664) ✔

### Less Context, Better Agents (2606.10209) — MOST TRANSFERABLE of the trio (training-free)
Numbers: full history 71.0% task completion / 1.48M tok → prune-to-last-5 tool pairs 79.0% / 535K (−63.9%) → prune+summary-of-evicted 91.6% / 553K (summary costs +3.4% tok, +12.6pp). Stale-state reference = 47% of full-context failures (acting on superseded snapshots). Input tokens = 99.75–99.87% of total spend. Plateau at N=5 window / W=3 summary. Sonnet 4.5 cross-check: 88.0→94.5%.
- B1: retention actively HURTS accuracy, not just cost — evidence for routing dumps through subagents (eviction-by-never-admitting).
- B2: progress-ledger convention: long tasks keep a running action log (done/remaining/errors) in a scratch file so compaction//clear can't destroy task awareness; subagent reports = current state + action-log bullets (their App. F format).
- B3: stale-state rule: after any mutating op, RE-QUERY current state; never reason from an earlier dump. Candidate CLAUDE.md line + MCP design principle (cheap current-state snapshot tools).
- B4: working set ≈ 2 operation cycles — sizing default for tool page sizes / brief scope.
- B5: attack INPUT tokens (re-feed), not output — token_ledger should measure re-feed.
Loop-control (unreachable): Algorithm 1 transcript eviction itself.

### Agent-Omit (2602.04284) — RL-trained; reachable residue = measurement
Numbers: trajectory anatomy = observations 52.2% / thoughts 45.1% / actions 2.7% of tokens; contribution decays with turn; omitting initial/final thoughts detrimental, turns 3–10 safe. NO training-free variant evaluated — prompt-level borrowing weakly evidenced.
- B6: token-anatomy diagnostic: split ledger spend into tool-output vs thought vs action, by turn position → tells you whether next ratchet targets MCP verbosity or reasoning length.
- B7: "keep the log of what was done, drop what was seen" — actions are 2.7%, observations 52.2%: tools write big results to scratch file, return digest + path.
- B8 (weak): plan-first/verify-last brief structure rather than "think less" mid-sweep (style-directive class — the weak class per Guardrails-Beat-Guidance).

### ContextBudget (2604.01664) — RL-trained; prompt-only arm scored 0.031 vs 0.127 trained = WARNING LABEL
Numbers: managed 8k budget beat 235B/128k model (0.147 vs 0.136 BrowseComp-Plus); adaptive folding (−42% compression calls at slack, +109% under load); budget-as-prompt WITHOUT training ≈ useless.
- B9: budget visibility helps mainly where a DETERMINISTIC hook acts on it (block/paginate), not model self-management — supports flood_warn/64KB-guard design, argues against adding advisory "context remaining" prose.
- B10: deferred loading = size-before-ingest: peek endpoints, mandatory LIMIT, tools returning total_size + first page (json_peek already this shape).
- B11: liftable line for any summarize/compact step: "Preserve user requirements and errors" (never fold those).
Cross-paper synthesis: observations dominate cost; stale observations cause errors; training-free remedy = recency + progress summary; RL papers' reachable residue = measurement + deterministic plumbing.

## Reader C — caching + skills (2601.06007, 2603.29919) ✔

### Don't Break the Cache (2601.06007, Lumer et al.)
Numbers: caching cuts cost 41–80%, TTFT +13–31%; Claude Sonnet 4.5: 77.8–78.5% cost cut, TTFT gains under EVERY strategy (20.9–22.9%, only model with no regression case); Anthropic pricing 1× input / 0.1× cache-read / 1.25× cache-write; savings scale with prefix size (88% at 50k); cache breaks: timestamps/session IDs in prompt, ANY tool-roster change, history rewrites (compaction = cache event).
- C1: audit hook output (SessionStart/UserPromptSubmit) + CLAUDE.md for volatile text (timestamps, counters, git status) — dynamic values last, stable within session.
- C2: freeze tool roster mid-session; /mcp restart mid-task = full prefix rewrite. Deferred-tools design = already the recommended pattern.
- C3: REPRICE the program: prefix bytes are 0.1× after turn one — trimming per-turn READS dominates trimming CLAUDE.md ~10× for dollars; CLAUDE.md trim is for attention, not spend. Refines cost-of-change doctrine.
- C4: /clear-between-tasks is also cache-optimal; avoid mid-task compaction near the end.
- C5: cost_of_change bench should SPLIT billed input by class (input/cache_creation/cache_read from usage fields) — "flat billed input" conflates three prices. EXPLAINS the C2 bench signature (read −62%, cost −20%, billed flat) [Extracted — reader's inference over paper pricing + our bench].
- C6: on Anthropic don't build cache-steering machinery (no regression case).
Not applicable: cache_control placement/TTL (Claude Code internal), UUID technique.

### SkillReducer (2603.29919, Gao et al.)
Numbers: 55,315-skill corpus: only 38.5% of body content is actionable (background 40.7%, examples 12.9%); descriptions −48%, bodies −39%, quality IMPROVED 0.722→0.742 (p=0.002); less-is-more grows with length (+11.8pp official skills, 95.8% compression on >10K-tok skills); 10.7% of skills never trigger at all; classify-before-compress worth 6.8pp over blind shortening.
- K1: rewrite skill descriptions to three-signal form (capability + trigger condition + unique identifiers, 20–40 tok); trigger-phrase enumerations = dead weight — our `impeccable`/`dataviz` descriptions are the named anti-pattern.
- K2: taxonomy pass per SKILL.md: core rules inline; background/templates → reference files; examples → one per concept.
- K3: reference files get a "when:" clause + 3–5 keywords; <30-tok references fold inline.
- K4: sort skills by token count, cut the biggest hardest (retention 0.942 even at >80% compression).
- K5: obsolescence audit: run each skill's eval WITHOUT the skill; no delta ⇒ retire (10.7% never trigger; no-skill scores 0.684 vs 0.722).
- K6: gates: (i) every operational concept survives somewhere; (ii) ~5 tasks score no worse; failures promote item back to core UNCOMPRESSED.
- K7: numeric floors: description ≤40 tok = can't route; reference <30 tok = inline; one example per concept.
Note: paper says NOTHING about CLAUDE.md routing tables (absence confirmed).
Cross-paper: prefix = cheap in dollars, expensive in attention → trim for quality (SkillReducer), keep byte-stable (cache paper), expect billed-input flat throughout.

## Reader A — harness engineering (2602.14690, 2603.09619) ✔ (paper 2 via PDF+PyMuPDF — no HTML exists)

### Harness Engineering study (2602.14690, 2,853 repos, Feb 2026 snapshot)
Numbers: context files dominate (CLAUDE.md in 45.9% of repos); advanced mechanisms rare (<20% except Cursor rules); 85.5% of skills bundle NO resources ("structured text not executable"); ZERO repos use subagent persistent memory; skills norm <500 lines (95.2% comply); "harness engineering in open source today is mostly context engineering"; Lulla et al. 2026: AGENTS.md presence → lower runtime+tokens at same completion.
- H1: executable skills = unexploited frontier — skills whose body says "run these steps" should ship the steps as scripts/ (only 5.8% do).
- H2: subagent persistent memory: supported, universally unused (0/131) — a recurring explore agent could keep its own findings file. (Counter-consideration: duplicate memory stores diverge; our auto-memory already injects into subagents.)
- H3: mechanisms compose (hooks↔settings V=0.36 etc.) — leverage is INTEGRATION between mechanisms, not adding more. This harness is already in the rarest tier; its own before/after measurements answer the field's open question locally.
- H4: reference-pointer convention (one-line gloss + pointer, never restate) is the empirically dominant layering style; layered context files risk conflicting instructions.
- H5: skills line-count lint (<500) matches community norm.
- H6: presence ≠ use — instrument whether config actually fires (we do: adoption ledger).
- H7: AGENTS.md portability hedge — optional insurance only for a committed Claude Code user.

### Context Engineering pyramid (2603.09619, conceptual, 25pp PDF)
Five criteria: relevance / sufficiency / isolation / economy / provenance ("context = the agent's OS"). Rot taxonomy: poisoning / distraction / confusion / clash (39% quality drop from early incomplete attempts left in context). Author concedes: only ECONOMY is cleanly measurable; no good conflict-prioritization mechanism exists anywhere.
- P1: five-criteria audit checklist per context source (rules file, tool output, memory, hook injection).
- P2: verdict-not-raw-hits inter-agent contract: conclusion + confidence + minimal citations, never dumps (their MAS leaked false-positive NER hits until filtered).
- P3: contract-first: no subagent spawn without a defined acceptance check; decompose until checkable (= our builder-brief convention, now with a named source).
- P4: authority gradient: subagents must report BRIEF INSUFFICIENCY as a first-class outcome ("brief did not specify X; I did not guess").
- P5: anti-reward-hacking isolation: keep answer keys/ratchet baselines out of the working agent's visibility; separate checker.
- P6: cache-stability discipline: batch CLAUDE.md/rules edits; volatile data never in always-loaded files (converges with C1).
- P7: rot taxonomy as post-mortem vocabulary for retro memories; /clear = anti-clash control.
- P8: memory poisoning can't be surgically removed → gate writes, periodically audit "what do you believe about this project" vs repo state.
- P9: four memory types priced differently → deliberate storage routing (conventions→always-loaded, decisions→memory files, schemas→MCP, workflows→skills) — we already do exactly this; name it.
- P10: PRIORITY ORDERING in rules: state explicitly what beats what (correctness/provenance > token economy > speed) — a rules list without ranking leaves conflicts to the model's proxy.
- P11: declare precedence BETWEEN layers (which file wins on collision).
- P12: hard meters only on what's measurable (cost/tokens); relevance/sufficiency = judgment passes, don't fake metrics.
- P13: delegation = authority + output contract, not file-tree slices.
Not applicable: enterprise governance, hyperscaler infra, A2A/crypto protocols, training-side.

## ALL READERS COMPLETE — gap review next

## Blog sweep ideas: see anthropic_blog_findings.md (B1–B14)


<!-- ══════════ source: scratchpad/anthropic_blog_findings.md ══════════ -->

# Anthropic blog sweep — candidate-relevant extract (agent, 2026-07-31, all [full-text])

8 posts read in full. Below: only items that are CANDIDATE ideas for this harness (not already scorecard-confirmed) + the numbers table. Full report in tasks/a05e4550801f952e8.output.

## Candidate ideas from the posts
1. **Stop-hook escalation ladder** (best-practices): check-in-prompt → /goal condition re-checked per turn → Stop hook (overridden after 8 consecutive blocks) → verification subagent. We use Stop hooks; /goal and verification-subagent rungs unused.
2. **Compaction steering in CLAUDE.md** (best-practices): "When compacting, always preserve the full list of modified files and any test commands" — we have no compaction instructions anywhere.
3. **/btw side questions** never enter history — adoption tip, zero config.
4. **Interview → SPEC.md → fresh session** for larger features; "/clear after 2 failed corrections on same issue".
5. **response_format enum (concise/detailed) on MCP tools** (writing-tools: Slack example 206→72 tok); resolve UUIDs to 0-indexed IDs; truncation messages that steer ("make small targeted searches").
6. **Agent-optimizes-tools loop** (writing-tools): concatenate eval transcripts → have Claude rewrite tool descriptions; −40% task completion time from one description rewrite; held-out test set against overfitting.
7. **Tool consolidation test** (context-eng + writing-tools): "if a human can't say which tool applies, the agent can't either"; more tools ≠ better.
8. **Feature-list JSON + init.sh + progress file** for long-running/multi-session work (long-running-agents post): JSON over Markdown for state files ("model less likely to inappropriately change JSON"); one feature per session; session-start protocol (read git log + progress before working).
9. **Skill splitting rule** (skills post): when SKILL.md unwieldy, split into files; mutually-exclusive contexts in separate paths; `disable-model-invocation: true` for side-effecting skills; watch name+description specifically.
10. **Emphasis tuning** (best-practices): IMPORTANT/YOU MUST raises adherence; debug signal "rule ignored ⇒ file too long, rule got lost".
11. **CLI > MCP for external services** (best-practices): "CLI tools are the most context-efficient way to interact with external services".
12. **LLM-judge eval recipe** (multi-agent post): single call, rubric, 0-1 score+pass/fail, ~20 queries to start; end-state evaluation for state-mutating agents.
13. **Delegation brief spec** (multi-agent post): objective / output format / tool guidance / task boundaries — matches our builder-brief convention; add "effort scaling: 3-10 calls simple, >10 subagents complex".
14. **Compaction levers** (context-eng): tool-result clearing is the "safest lightest touch" form; "maximize recall then improve precision".

## Numbers table (provenance = the posts themselves)
- Multi-agent beats single Opus 4 by 90.2% on research eval; token use explains 80% of variance; agents 4×, multi-agent 15× chat tokens.
- Tool-description rewrite −40% task time. Concise vs detailed tool response: 72 vs 206 tok.
- Memory+context-editing +39%, editing alone +29%, −84% tokens over 100 turns.
- Claude Code default tool-response cap 25,000 tok. Stop hook auto-overridden after 8 consecutive blocks.
- CLAUDE.md pruning test = "would removing this cause mistakes?" (matches our claude_md_prune_bench).

## Follow-up reads (found, not fetched)
- anthropic.com/research/claude-code-expertise (~400k-session usage study)
- anthropic.com/news/enabling-claude-code-to-work-more-autonomously
- 2026 Agentic Coding Trends Report PDF (resources.anthropic.com)

## Academic citations found in posts
Only in context-engineering post: Chroma context-rot, arXiv:1706.03762, arXiv:2306.15595 (position interpolation), working-memory (SAGE). Others cite docs/benchmarks (BrowseComp), no new papers for the corpus.


<!-- ══════════ source: scratchpad/corpus_candidates.md ══════════ -->

# Corpus scan candidates (snowball agent, 2026-07-31)

Verification key: V = arXiv page fetched+resolves · S = search-returned link, unfetched · R = ID from a seed's reference list, unconfirmed · U = training knowledge, unchecked.

## Selected for full read (5 reader agents dispatched)
1. 2602.14690 Harness Engineering for Agentic AI Coding Tools (2026) V — reader A
2. 2603.09619 Context Engineering: 5 context-quality criteria (2026) S — reader A
3. 2606.10209 Less Context, Better Agents (2026) V — reader B
4. 2602.04284 Agent-Omit: adaptive context omission (2026) V — reader B
5. 2604.01664 ContextBudget (2026) V — reader B
6. 2601.06007 Don't Break the Cache (2026) V — reader C
7. 2603.29919 SkillReducer (2026) V — reader C
8. 2412.18547 TALE token-budget-aware reasoning (2024/ACL-F 2025) S — reader D
9. 2407.16833 Self-Route: RAG vs long-context (2024) V — reader D
10. 2605.28840 How Consistent Are LLM Agents? (2026) S — reader E
11. 2506.09501 Numerical nondeterminism (2025) S — reader E
    + reader E verifies-by-title: Order Matters (2502.17204? R) and Serial Position Effects (2406.15981? R)

## Not selected (with reason)
- 2605.09104 Token Economics dual-view survey — vocabulary, not techniques
- 2606.17016 TokenPilot / 2606.01065 Leyline (ID ambiguous: also seen as 2605.05696) / 2606.30005 VISTA — serving-side or loop-control, unreachable from config
- 2603.09023 Demand Paging — design pattern already implemented as MEMORY hot/cold
- 2310.08560 MemGPT, 2504.19413 Mem0, 2502.12110 A-MEM — memory architecture already settled (file-based, scorecard row); A-MEM's supersede-detection noted as a future idea
- 2502.04362 DIM-Bench — evidence base for lean rules, no new mechanism
- 2507.21504 eval survey, 2507.02076 test-time-compute survey — index papers
- Training-side compression family (TokenSkip, C3oT, CoD fine-tune, BudgetThinker) — we don't fine-tune
- Near-misses: IntentKV 2606.09916, Consistency as Testable Property 2605.10516, FollowBench 2310.20410, EvolveR (no ID)

## Snowball caveats (from the agent)
- Context-engineering survey (2507.13334) HTML gives citation numbers not IDs; exhaustive bibliography needs the PDF.
- All 26xx IDs are post-cutoff, found via search; S-row one-liners are search snippets → [Reported] until fetched.
- Guardrails-Beat-Guidance refs snowball into agent-scaffold territory, thin for token economics; 2602.14690 is the better bridge.
