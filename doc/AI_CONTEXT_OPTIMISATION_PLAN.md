---
tier: SPEC
status: LIVE
domain: infra
updated: 2026-07-14
supersedes: []
read_when: cutting AI/agent token spend, or adding docs/manifests/MCP tools
key: SPEC|LIVE|infra
---

# AI context & token optimisation — audit + plan

**Date:** 2026-07-14 · **Status:** TIER A + TIER B (retrieval layer) SHIPPED 2026-07-16/17; config (A0/A1) still owner-gated.

## What shipped
- **Section maps** on all 12 files >1,500 lines, generator `tools/section_map.py` (`--check` CI gate). Finding one thing across the whales: **~394k → ~23k tokens (95% cut)**.
- **`utility/pages_code/MANIFEST.md`** (sizes + domain-triple + CSS split-brain) and **`utility/README.MD`** rewritten from a stray ChatGPT prompt into a real map.
- **18 dead doc references** repointed (archived → `doc/archive/`, missing → supersedor).
- **Retrieval layer (Tier B keystone):** `tools/build_fact_cards.py` → `data/_meta/fact_cards.json` (199 facts, live footer read ~5s incl. the 200MB whales, `--check` gate, wired into `pipeline.py` after freshness) + curated `data/_meta/fact_grain.csv` (the never-sum rule as DATA) + MCP tools **`describe_dataset`** and **`list_datasets`**. Tests: `test/tools/test_fact_cards.py` (8).
- **Two data bugs found and fixed en route:** 8 councils orphaned by long publisher names (`_canon_la_publisher_names` in the consolidator — Dublin City's 40,431 rows rejoined, 22→30 councils join AFS; collision guard + `test_payments_la_canon_names.py`); stale runtime manifest (7 IPAS parquets).

## Still owner-gated (A0/A1 — self-modification guard blocked me, correctly)
The `Read` deny-list + git deny + `mcp__dail-tracker` allowlist — exact JSON below in §0b/§2. These widen my own permissions off a bare "continue", so they need your hand.

---

**Original plan (below) — Tier C items (extractors/dail_tracker_core/tools manifests, MEMORY.md trim, dead-CSS deletion) remain open.**
**Method:** 4 parallel audits (doc/module navigation · token whales · data retrieval · agent config & spend).
*(This file carries the proposed front-matter block as a live specimen — see §3.)*

---

## 0. The headline: we were optimising the wrong end

**The always-on budget is not the problem.** CLAUDE.md (~1.2k) + MEMORY.md (~5k) = **~6.1k tokens/session** — under 1% of the window, and it earns its keep.

**One recorded incident — the workflow `args` runaway — burned ~2.7M tokens.** That is **4.4× the entire always-on cost of 100 sessions**. Spend effort on *guardrails, routing and retrieval*, not on trimming the index.

**Two hypotheses I brought in were wrong, and the audit killed them:**
- *"MEMORY.md's long lines are bloated prose."* No — they are **shelves of 6–9 links**; 52% of the file is filenames. Splitting them into one-line hooks would **add ~190 tokens** and make the file 215 lines. **Do not do it.**
- *"A memory file is missing (`project_la_budgets_fact.md`)."* **False positive — it exists.** Verified on disk.

---

## 0b. 🚨 THE ONE TO FIX TODAY — an unguarded context detonation

**CLAUDE.md says "never Read data files (parquet/CSV)". These are `.txt` and `.html`, so they slip past the rule — and there is NO `deny` list in either settings file.** Verified:

| File | Tokens if `Read` |
|---|---|
| `doc/source_pdfs/_samples/eurostat_toc.txt` | **~496,000** |
| `data/bronze/lrc_classlist/classlist_*.html` (6 files) | **~175k–277k each** |
| `data/silver/sipo_candidate/_log_candidate_ocr.txt` | ~211,000 |
| `planning_rules/**/raw/dm_standards.{txt,html}` | ~164k–174k |
| `doc/source_pdfs/NOAC_LA_PerfInd_2024.txt` | ~161,000 |
| `pipeline_sandbox/council_minutes/samples/*.txt` | ~130k–134k |

One accidental Read of the first file **exceeds most context windows outright**. Nothing prevents it.

**Fix: ~10 lines of config.** Add to `.claude/settings.json`:
```jsonc
"permissions": { "deny": [
  "Read(doc/source_pdfs/**)",
  "Read(data/bronze/**)",
  "Read(data/silver/**)",
  "Read(planning_rules/**/raw/**)",
  "Read(pipeline_sandbox/**/samples/**)",
  "Read(pipeline_sandbox/**/corpus/**)",
  "Read(**/*.parquet)"
]}
```
Plus extend CLAUDE.md's rule from "parquet/CSV" to "any raw/sample/corpus text or HTML — grep it, never read it."
**Cost: minutes. Prevents a class of failure that costs an entire session.**

---

## 1. The five findings that matter

### 🔴 F1 — 55 domain MCP tools, **zero metadata tools**
The cheapest question an agent can ask — *"what columns does this fact have?"* — forces the **most expensive possible action**: a throwaway polars script, or a `Read` that blows the window.

And it is **free to fix**: `pl.scan_parquet().collect_schema()` reads only the footer —
| File | Size | Result | Time |
|---|---|---|---|
| `speeches_fact_full.parquet` | **213 MB** | 575,274 rows × 20 cols | **31 ms** |
| `speeches.parquet` | 211 MB | 575,274 × 12 | **1.2 ms** |

Worse: **the data already exists.** `data/_meta/output_baseline.json` holds `{rows, columns[]}` for **109 gold parquets** — and is read by nothing but a regression test.

### 🔴 F2 — the MCP server is gated behind a permission prompt, every time
**Zero `mcp__dail-tracker__*` entries in `settings.json` or `settings.local.json`** (verified). CLAUDE.md *mandates* MCP-first for data questions — so **the cheapest tool in the project sits behind the most frequent interruption**. Meanwhile `settings.local.json` has accreted **107 hyper-specific one-offs** (e.g. the firewall checker allowlisted for `committees.py` *and nothing else*).

### 🔴 F3 — 20 broken doc references, and 5 actively-lying docs
- **8 docs referenced from code/CI do not exist** (`doc/CRO_FINANCIAL_STATEMENTS_EXPLORATION.md`, `doc/PROCUREMENT_INVESTIGATION.md`, `doc/SIPO_OCR_INVESTIGATION.md`, …). Each = a failed read → fallback broad grep.
- **12 more** are cited as `doc/X.md` but live in `doc/archive/X.md`.
- **5 STALE docs whose headers contradict the repo** — and these are *the exact documents that already misled us*:
  - `LOCAL_AUTHORITY_ACCOUNTABILITY.md` → "Cork County absent (30/31)"; **truth is 31/31**
  - `PER_LA_AFS_BUILD_PLAN.md` → stale paths; **this doc-family produced the false "AFS is a permanent dead-end" memory**
  - `ENTITY_CROSSWALK_ORG_DOSSIER_DESIGN.md` → "design-only, no code written"; **`entity_xref` is a live pipeline chain**
  - `DISCLOSED_PO_INTEGRATION_PLAN.md` → "NOTHING here ingests"; **it does**
  - `CONTINUOUS_REFRESH.md` → superseded by `HYBRID_REFRESH_PLAN.md`
- Bonus: **`utility/README.MD` is a leftover ChatGPT prompt scratchpad** (contains a chatgpt.com URL). Worse than nothing.

### 🟠 F4 — the whales: navigable, not splittable
`shared_css.py` **72k tok** · `procurement.py` **59k** · `test_sql_views.py` 37k · `procurement_public_body_extract.py` 34k · plus 8 more >15k (incl. `mcp_server/server.py` 22k, `dail_tracker_core/queries/procurement.py` 20k, `utility/ui/components.py` 17k). **No index tells an agent how big a file is before it opens one.**

- **`shared_css.py` is the worst-navigable file in the repo**: 6,266 lines, ONE function, ONE `<style>` string (lines 20–6,249), and **zero section comments in the entire CSS body**.
- **Do NOT split it by prefix.** The families are *fragmented* — `.dt-*` appears in **5 separate runs**, `.cmt-*` at 4,918 *and* 6,214, `.leg-*` at 3,657 *and* 4,142. CSS is order-dependent (equal specificity = last wins), so a "one module per family" split would silently reorder rules and break the cascade across 20 pages. **This is exactly the class of change the defer-refactors rule exists to prevent.**
- **`procurement.py`: reject the split.** It would mean extracting the href/format helpers (lines 134–321) used at **~40 call sites**, untangling the `?tab=`/`?supplier=`/`?authority=` router, and re-registering the page contract — multi-hour surgery on the highest-traffic page for zero user benefit. It already has 12 `# ────` banners; it needs an *index*, not surgery.
- **16% of the CSS is dead**: **135 of 852 classes have zero references** anywhere. `utility/ui/components.py:491` even says so out loud — *"the `pay-totals-*` classes can be retired."* ~110 genuinely dead (~500 lines, 6–8k tokens). Beware false positives: `.st-key-*` is generated by Streamlit from widget keys, and some classes are built dynamically (`f"dt-badge-{kind}"`).
- **Split-brain CSS:** five pages carry their own injectors — `corporate.py` holds **727 lines of CSS inside the page file** (~8k of its 27k tokens), while `.con-*` *also* exists in shared_css. Two places to look for one rule.

**The fix is a `# ── SECTION MAP ──` header** (line-ranges in the first 60 lines) on every file >1,500 lines, enforced by `tools/section_map.py --check` (a sibling of the existing firewall checker). An agent then Reads 40 lines and jumps with `offset`/`limit`.
**Effect: ~380k tokens of unavoidable whale-reading → ~35k of targeted reading. A ~90% cut, with zero runtime risk and zero refactoring.**

*Optional follow-up (only if the CSS must stay editable long-term):* split `shared_css.py` into **order-preserving** fragments joined in the same sequence, guarded by a `sha256(joined) == golden_hash` test. Byte-identical output, provable in CI, zero cascade risk.

### 🟠 F5 — every custom agent inherits Opus and *all* tools
5 agents (`explore`, `reviewer`, `contract`, `data_view`, `streamlit_frontend`) declare **no `model:` and no `tools:`**. So `explore` — described as *"read-only agent that finds minimum relevant files"* — **runs on Opus and can Write and Edit.**

---

## 2. The plan, ranked by ROI

### Tier A — do first (~5 hours, near-zero risk, huge return)

| # | Action | Cost | Saves |
|---|---|---|---|
| **A0** | 🚨 **`Read` deny-list for raw/sample/corpus paths** (§0b) + extend the CLAUDE.md rule beyond parquet/CSV | **minutes** | **prevents a ~496k-token context detonation** |
| **A1** | **Allowlist `mcp__dail-tracker`** + de-pin the checker/pytest/ruff/git-read commands; add a **`deny`** list for `git push`/`git stash`/`git reset --hard` (makes 3 recorded rules unbreakable instead of hoping) | 15 min | every MCP call, every session |
| **A2** | **`utility/pages_code/MANIFEST.md`** — 28 rows **with a KB/token column** | 30 min, 750 tok | **10k–59k per avoided read.** One avoided `procurement.py` full-read repays it **78×** |
| **A3** | **Fix the 20 broken doc refs** + delete `utility/README.MD` | 45 min | 3k–15k per hunt; highest saving-per-minute in the audit |
| **A4** | **Add `model:` + `tools:` to the 5 agents** (`explore`→haiku/read-only, `reviewer`/`contract`/`streamlit_frontend`→sonnet, **`data_view` stays Opus** — it owns the never-union money-grain rules) | 20 min | recurring multiplier on every delegation |
| **A5** | **Section-map headers** on the ~10 files >1,500 lines + `tools/section_map.py --check` in CI (sibling of the firewall checker) | 2 h | **~380k → ~35k (a 90% cut)** |
| **A6** | **Delete the ~110 confirmed-dead CSS classes** (~500 lines) — `components.py` already documents `pay-totals-*` as retirable. Verify each family by rendering its page; skip `.st-key-*` and dynamic `f"dt-badge-{kind}"` forms. | 1 h | ~6k permanent + removes false leads |

### Tier B — the retrieval layer (~1 day, the structural fix)

| # | Action | Cost | Saves |
|---|---|---|---|
| **B1** | **`tools/build_fact_cards.py` → `data/_meta/fact_cards.json`** — union what already exists (`output_baseline` rows+cols, `gold_quality_baseline`, `runtime_data_manifest` size + `kept_because` reverse-index, `freshness`) and add the 3 things nobody records: **grain, year-span, never-sum class**. Encodes the 3-money-grain rule **as data**, not as prose an agent may not have read. | M | the keystone |
| **B2** | **MCP `describe_dataset` + `list_datasets`** (served from B1) | S | kills the #1 recurring cost |
| **B3** | **`tools/build_views_manifest.py`** → `views_manifest.json` + `VIEWS.md` + MCP `list_views`. **88% of the 231 SQL files already parse** with zero cleanup (204/204 header names match their `CREATE VIEW` — zero drift). Gives the reverse index free: `procurement_awards` is read by 16 views. | S | ends grepping 231 SQL files |
| **B4** | **`doc/INDEX.md`, generated from front-matter** (~1.7k tok, 44 live docs) + `key: TIER\|STATUS\|domain` line for one-grep filtering + CI check. **Generated, so it cannot rot** — which is the actual root cause of F3. | 2–3 h | 15k–40k per doc hunt |
| **B5** | **Archive 7 superseded docs** (114 KB ≈ 29k tok off the live surface) | 20 min | shrinks the grep surface ~10% |

### Tier C — worth doing, lower urgency
- `extractors/MANIFEST.md` — **auto-generatable**: the 101 extractors already have good one-line docstrings (`ast.get_docstring()`). ~1 h scripted.
- `dail_tracker_core/MANIFEST.md`, `tools/MANIFEST.md` (flags the 3 `patch_*.py` as **one-shot, already applied** — a *correctness* win: re-running one corrupts data), `data_access/MANIFEST.md` (states the **domain-triple** convention: `pages_code/X.py` ← `data_access/X_data.py` ← `queries/X.py` ← `sql_views/<domain>/X_*.sql`).
- **Memory whales**: `project_ministerial_diaries.md` = **53 KB ≈ 15k tok** — one recall costs **2.5× the entire always-on budget**. Split into decisions-head + appendix. Same for `siting_check_build`, `streamlit_uncoupling`.
- **MEMORY.md**: archive ~40 dated point-in-time audit entries to a non-auto-loaded `MEMORY_ARCHIVE.md` (they stay recallable via `description:`) — **~840 tok/session**. Trim the 3 ⭐ prose tails (~240 tok). *Do NOT split the link shelves.*
- **CLAUDE.md**: fix the stale parquet example — it names `speeches_fact` (24 MB) but **not `speeches_fact_full.parquet` (204 MB) or `speeches.parquet` (202 MB)**, the two files that would do the most damage. Add: *"before scanning, call `describe_dataset`."*

### Deliberately NOT doing
- **Splitting `shared_css.py` / `procurement.py`.** Standing rule: defer big refactors. Navigation (A5) gets ~95% of the benefit at ~0% of the risk.
- **A raw `run_sql` MCP tool.** Highest ceiling, but it **bypasses the privacy filters baked into all 55 hand-written tools** (personal-insolvency never-individual; the `public_display` gate). If ever built: SELECT-only, row cap, view allowlist — and a separate privacy review.
- **Splitting MEMORY.md's link shelves** — would add tokens (see §0).

---

## 3. The conventions this establishes

**Doc front-matter** (7 lines; the `key:` line enables one-grep filtering with no body read):
```yaml
tier: SPEC | CONTEXT | PLAN | RECORD
status: LIVE | STALE | SUPERSEDED
domain: money | procurement | local-gov | data | infra | ui | …
updated: 2026-07-14
supersedes: []
read_when: <the trigger — not a summary>
key: SPEC|LIVE|infra
```
→ `Grep "^key: SPEC\|LIVE\|money"` finds every live money spec **without reading a single body**.

**Big-file section map** — mandatory header on any file >1,500 lines, listing sections with line numbers.

**The three layers** (from [[feedback_promote_vs_context_gate]]): **`doc/` = library** (cited, auditable) · **memory = engine** (distilled, loads every session) · **app = product** (only what passes the gate).

---

## 4. The one-line finding

> **The project has 55 excellent *domain* tools and zero *metadata* tools — so the cheapest question forces the most expensive action. And the data to fix it has been sitting in `output_baseline.json` all along, read by nothing but a regression test.**
