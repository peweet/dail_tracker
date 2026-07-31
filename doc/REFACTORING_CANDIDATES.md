# Refactoring Candidates — Token-Economics Pass

Status: **PLAN — nothing here is implemented.** Companion to
`doc/REFACTORING_TOKEN_ECONOMICS.md` (the patterns P1–P5 referenced below).
Drafted 2026-07-30 from a churn × size scan and three structural surveys.

Evidence bands: line counts and change counts are [Verified — `git ls-files | wc -l` and
`git log --name-only` this session]. Internal-structure and duplication claims are
[Reported — explore-agent surveys this session, spot-citations included but not
independently re-read]. Re-verify a cited line range before editing near it.

## Selection method

Per pattern P2, the saving is banked on every future change that touches a file, so
candidates are ranked by **changes since 2026-01-01 × file size**, filtered to files with a
preservable interface (P4). The repo's SECTION MAP headers and MCP `outline` reduce
navigation cost but, per the article's result, do not substitute for the split: the measured
token cliff only arrived when the largest file physically shrank.

| Rank | File | Lines | Changes | Interface to preserve |
|---|---|---|---|---|
| C1 | `utility/shared_css.py` | 6,572 | 96 | one symbol: `inject_css()` |
| C2 | `utility/pages_code/procurement.py` | 4,665 | 65 | one symbol: `procurement_page()` |
| C3 | `utility/pages_code/member_overview.py` | 2,498 | 56 | one symbol: `member_overview_page()` |
| C4 | `utility/ui/components.py` | 2,159 | 44 | ~50 symbols across 8 importing pages |
| C5 | `mcp_server/server.py` | 2,744 | 30 | 70 MCP tool names + stdio entry point |
| C6 | `test/sql_views/test_sql_views.py` | 3,579 | 29 | pytest discovery + `-m sql` marker + skip semantics |
| C7 | `extractors/procurement_public_body_extract.py` | 2,948 | 42 | `pipeline.py` chain `"public_body_payments"` + CLI `--list/--merge` |

Not candidates: the siting lane is already factored (largest file `engine.py` at 1,860
lines); `iris/iris_oifigiuil_etl_polars.py` has 16 functions with little repetition —
low dedup yield for its churn.

## Phase 0 — measurement first (pattern P1) — **BUILT AND BASELINED 2026-07-30**

- `tools/evals/cost_of_change_bench.py`: runs a frozen representative-change prompt (one per
  candidate, in `tools/evals/bench_prompts/`) in a fresh claude-agent-sdk session against a
  throwaway git worktree of HEAD, records SDK-billed usage plus the article-style
  files-read self-report (tokens ≈ chars ÷ 4), appends to `logs/cost_of_change.jsonl`, and
  discards the worktree. The agent runs with `setting_sources=[]` (no CLAUDE.md, hooks, or
  MCP) and no shell — pure code-orientation cost. Note: `allowed_tools` does NOT restrict
  under `bypassPermissions`; `disallowed_tools` is what blocks Bash (pilot finding).
- The largest-file ratchet is live in `tools/check_conventions.py` (`LARGEST_FILE_CAPS`).
  `mcp_server/server.py`'s cap was set at 2,843, not the survey's 2,744 — the file grew 99
  lines from a parallel session *during baselining*, which is the churn argument in
  miniature.

### Baselines (model claude-sonnet-5, one run each; C1–C6 at 2efa152, C7 at 432383f)

All figures [Verified — `logs/cost_of_change.jsonl` rows of 2026-07-30, `harness`-tagged].
"Input" = SDK-billed input tokens including per-turn cache re-reads — the cumulative context
the agent processed; "self-report est." = the agent's own files-read chars ÷ 4, the
article's metric (what it read of the codebase, once).

| Cand. | Change | Input | Output | Self-report est. | Turns | Wall | Cost |
|---|---|---|---|---|---|---|---|
| C1 | add CSS family | 836,499 | 9,741 | 6,625 | 19 | 132 s | $0.76 |
| C2 | add drill-down profile | 1,243,897 | 8,786 | 25,925 | 21 | 131 s | $0.82 |
| C3 | add profile section | 617,330 | 6,113 | 7,550 | 16 | 118 s | $0.45 |
| C4 | add badge component | 553,793 | 6,281 | 2,250 | 16 | 106 s | $0.45 |
| C5 | add MCP tool | 523,751 | 4,233 | 15,309 | 10 | 65 s | $0.48 |
| C6 | add view test | 421,684 | 3,736 | 18,007 | 9 | 71 s | $0.45 |
| C7 | add publisher reader | 1,421,454 | 17,346 | 24,125 | 21 | 207 s | $1.13 |

### C2 after-measurement (2026-07-31, HEAD 27f4607 — split executed)

Same frozen prompt, same model, one run each side [Verified — before/after rows in
`logs/cost_of_change.jsonl`]:

| Metric | Before (flat 4,665-line file) | After (12-module package) | Change |
|---|---|---|---|
| Self-reported code read (tokens est.) | 25,925 | 9,925 | **−62%** |
| Cost | $0.82 | $0.66 | −20% |
| Wall | 131 s | 115 s | −12% |
| Billed input (incl. per-turn cache re-reads) | 1,243,897 | 1,200,180 | −3.5% |
| Output | 8,786 | 6,838 | (noise) |
| Turns | 21 | 28 | +33% |
| Largest file in the layer | 4,665 | 737 | −84% |

**What this refines in the article's claim:** on the article's own metric — characters of
code the agent actually read — the split reproduces the effect (−62%; the after-run agent
read `profiles.py`, `page.py` and a 100-line slice of `_shared.py` instead of swallowing
the monolith — visible in the row's `reads` list). But **billed** input tokens barely moved,
because a multi-turn harness re-sends the whole context every turn: billed input ≈ resident
context × turns, and the turn count *rose* 21→28 (more, smaller files → more Read/Grep
round-trips). The economics still land — −20% dollars (cache reads are cheap per token) and
−12% wall — but the honest statement is: **splitting cuts what the agent reads and what the
change costs; it does not shrink per-turn context re-send, and it can add turns.** Caveat:
one run per side; turn count is noisy. The remaining candidates should confirm or kill the
pattern before it hardens into doctrine.

Reading of the baselines (inference from the table above, decomposed):
- Output is 1–2% of input everywhere — the article's read-dominance reproduces here.
- Billed input tracks **turn count** more than file size (each turn re-sends the grown
  context), and turn count tracks orientation difficulty: the two multi-site changes
  (C2 page+data-access, C7 config+reader+wiring) cost 1.2–1.4M input over 21 turns.
- A re-run after each candidate's refactor uses the same prompt (`prompt_sha256` is in the
  row); comparisons are valid only within candidate + prompt + model.

## Candidates

### C1 — `utility/shared_css.py` → `utility/shared_css/` package
The whole file is one 6.2 kLoC `<style>` string injected once by `inject_css()`; 19
per-domain selector families (`.pr-*`, `.mo-*`, `.lob-*`, …) sit interleaved, and the file
header itself warns the cascade is order-dependent — equal specificity, last rule wins.
[Reported — survey citing shared_css.py:9-13]

- **Dedup first (P3):** fold the ~420 repeated spacing declarations and ~199 border-radius
  rules toward the existing `:root` design tokens where they duplicate token values.
- **Split second:** per-family fragment modules concatenated by `inject_css()` from an
  **explicit ordered list**, preserving today's byte order exactly (split only at contiguous
  boundaries in pass one). Regrouping interleaved families into clean per-domain files is a
  **separate later pass** gated on a cascade-collision check plus visual review, because it
  reorders rules.
- **Hazards:** cascade order (above); three pages import `inject_css` directly; some pages
  (corporate, judiciary) carry their own inline CSS that overlaps this file's role.
- **Why it's rank 1:** top of both the size and churn lists; styling changes are frequent
  agent tasks and currently orient against a 6.5k-line file.

### C2 — `utility/pages_code/procurement.py` → package with per-domain render modules
Single `procurement_page()` entry (imported once in `app.py`) dispatches an 11-branch
query-param tree to 42 `_render_*` functions plus ~50 `_href`/`_pill`/format helpers.
[Reported — survey citing procurement.py:4444-4499, 271-378]

- **Dedup first:** the fetch → `if not res.ok: empty_state(); return` guard (~30 sites) to
  one helper; the 13 `_*_href` builders to one parameterised builder; the 8 `_*_pill`
  helpers toward `ui/components` (member_overview repeats the same idioms — cross-page win).
- **Split second:** `pages_code/procurement/` with the router in one module and one module
  per drill-down domain (suppliers, authorities, CPV, councils, TED, payment tiers), shared
  helpers in one, `__init__` re-exporting `procurement_page` so `app.py` is untouched.
- **Hazards:** low — no module-level Streamlit calls; `st.query_params`/session-state reads
  stay inside the router. Firewall unaffected: pages keep zero business logic.
- **Recommended first executable candidate** — cleanest interface, seams already visible.

### C3 — `utility/pages_code/member_overview.py` → package with per-section modules
The split is prefigured: nine `_section_*` handlers behind a section router, and two
sections (payments, interests) already live as imported panels. Same package treatment as
C2; keep the `_STAGE_KEY` session-state handling entirely inside the router module.
[Reported — survey citing member_overview.py:2273-2434, 156-164]

### C4 — `utility/ui/components.py` → package with re-export manifest
Seven observed groupings (text utilities; `dt_page`/lifecycle; layout; financial/narrative;
member-card library; pagination; support/footer). Split into `ui/components/` modules with
`__init__.py` re-exporting all ~50 symbols so the 8 importing pages change nothing.
**Hazard:** `dt_page` wraps every page — it goes wherever its imports stay acyclic
(`components` already imports from `ui.format`). [Reported — survey]

### C5 — `mcp_server/server.py` → per-register tool modules
70 `@mcp.tool` handlers repeating a try/except-`SourceUnavailable`-return-error-dict wrapper
(~14 explicit sites) and ~53 `_cur()` calls. [Reported — survey citing server.py:323-327]

- **Dedup first:** one error-wrapping decorator; a documented `_cur()` contract.
- **Split second:** `mcp_server/tools/{parliamentary,influence,legislation,procurement,
  siting,infra}.py`, each exposing `register(mcp, cur)`; `server.py` becomes the FastMCP
  instance, connection lifecycle, and registration manifest.
- **Hazards (repo-specific, all binding):** `_CONN`/`_CONN_LOCK` global with FastMCP
  threadpool; view registration order with `swallow_errors=True` (mis-registration fails
  silently); `resource_policy.capped_connect` is mandatory — never bare `duckdb.connect`;
  the server must never self-exit. Benchmark note: adding MCP tools is a recurring session
  task, so the read-path saving here lands directly on future harness work.

### C6 — `test/sql_views/test_sql_views.py` → parametrised + per-family files
118 flat `test_*` functions repeat one six-line skeleton (skip-if-missing → connect → load
SQL → materialise view → assert columns → assert rows). This is the article's own end-state
irony: after everything else shrinks, the biggest file left is the test library.
[Reported — survey citing test_sql_views.py:325-331]

- **Dedup first:** a `(sql_file, view_name, required_columns)` table driving one
  `@pytest.mark.parametrize` test collapses the skeleton cases; tests with bespoke
  assertions stay as explicit functions.
- **Split second:** per-view-family files under `test/sql_views/` with shared `_con`/
  `_load`/`_result` helpers in a `conftest.py`.
- **Hazards:** CI-vs-local fixture path switching must survive; parametrisation renames
  test IDs (anything keyed to old names — `--testmon` DBs, CI quarantine lists — resets);
  skip-not-fail semantics for missing fixtures must be preserved.

### C7 — `extractors/procurement_public_body_extract.py` → readers/harvest/classify modules
16 `read_*` publisher parsers, a per-tier harvest loop, and one dense `classify_and_flag`.
Already on `FetchReport` + `save_parquet` but not `extract_runner`/`coverage_io` — so this
candidate **merges into the existing convention ratchet** rather than being a separate
effort. [Reported — survey citing lines 1029-1175, 2614-2686]
- **Hazards:** module-level `REPORT`/`LAST_ERR` mutated across phases; merge-mode
  idempotency by publisher_id; the privacy quarantine is one-way; `save_parquet` row-floor
  guard stays. `services/runtime_env.py` must remain the first import at the entry point.

## Sequencing and what gets harder

Phase 0, then C2 → C1 (contiguous split only) → C4 → C5 → C3 → C6 → C7, re-running the
benchmark after each candidate completes (not after each internal step — learning #3 says
mid-sequence numbers mislead). Each candidate is independently shippable and reversible
(two-way door: re-exports mean callers never change; reverting is a file merge).

Costs named plainly: package splits add import indirection and one more hop for a human
reader who knew the old file; `git blame` fragments across the moves (use `--follow`);
open PRs touching these files at split time will conflict — land splits between feature
branches. The C1 regrouping pass and C6 parametrisation are the two steps that can change
behaviour (cascade order; test IDs) — both are flagged above and deferred within their
candidates. Missing information that would change the plan: real per-candidate baselines
from Phase 0 — if C2's baseline shows agents already orient cheaply via the SECTION MAP,
the expected cliff shrinks and C5/C6 (denser boilerplate) move up.
