# ETL Modularization & Reuse Plan

Scope: **ETL/backend only** (Streamlit UI and `pipeline_sandbox/` excluded). Derived from
the 2026-06-27 consolidation audit. Every item is rated by **maintenance win**, **risk**, and
the **test/guard** that keeps it behaviour-neutral.

## 0. Principles that constrain this plan
- **Logic firewall:** these files write gold facts. Any change that *could* alter output must be
  pinned by a value/parity test, not asserted by eye.
- **Deliberate divergence:** many look-alike parsers (money, PO-number, PDF readers) encode
  per-source rules. They are *not* accidental duplication — force-merging them changes which
  rows parse. Treat "looks identical" as a hypothesis to verify, not a fact.
- **Library split is intentional:** **Polars = ETL transform**, **DuckDB = SQL over the parquet
  warehouse (read/analytics)**, **pandas = UI** (plus a few deliberate pandas ETL scripts).
- **House style:** small, reversible PRs, each with its own test — the same discipline
  `dail_tracker_core/db.py` already documents for the `_sql_registry` duplication.

## 1. Architecture verdict — are we using Polars / DuckDB appropriately?

**Yes — the split is sound and nothing major is architecturally missing.** Polars does the
row-level extract/transform; DuckDB does set-based joins/aggregations over the gold/silver
parquet warehouse and returns pandas for the UI. That is the conventional, correct division.

Genuine gaps *within* that architecture (all addressed in the tiers below):

| Area | State | Gap (appropriate fix) |
|---|---|---|
| Polars UDFs | row-wise `map_elements`/`apply`/`iterrows` in ETL | replace with native expressions (`str.contains`, `when/then`, `cum_prod`, joins) — **T2.4** |
| Polars I/O | **eager** `read_parquet` in ~59 files vs **lazy** `scan_parquet` in 5 | use `scan_parquet().filter().select().collect()` (predicate/projection pushdown) for the **big facts only** (payments/procurement) — **T2.6** |
| pandas in ETL | 15 files import pandas | **leave** — intentional per project policy; not a conversion target |
| DuckDB read-layer | `_run(conn, sql, params)` duplicated in **24** query modules | one shared executor — **T1.3** |
| DuckDB connection | two builders: `analytics_loading.get_warehouse_connection` (uncached, `print()`s) vs `db.connect_with_views` (app-cached) | minor: dedupe/clarify, swap `print`→logging — **T2.7** |

Already healthy and *not* a problem: `save_parquet` adoption (101 files), `logging_setup` (64),
medallion path constants in `config.py`, view registration in `db.py`.

## 1b. Does each item actually decouple / materially help — or is it just syntax?

The honest test applied to every item: (A) does it **decouple** a concern / create a single
source of truth for something that matters? (B) does it make a **material** maintenance
difference (removes a real change-in-N-places tax or a bug class)? (C) does it improve
**stability**? Or is it (D) just **syntax/perf** churn that shuffles code without making the
codebase meaningfully better?

**Genuine architectural value (decouples + material):**
- **T1.3** `_run`→`run_query` — *real decoupling.* The read-layer's error/log **policy** (DuckDB
  failure → `unavailable`, what's logged, future: metrics/retry/error-typing) was coupled into 23
  domain modules; it now lives in one place, with only the domain `label` staying local. Evolving
  the policy is 1 edit, not 23. (Cost: one layer of indirection — honest but worth it.)
- **T1.1** SCHEMA_COLS const — *modest but real.* Single source of truth for the fact **contract**;
  removes a 5-way manual sync. (Build already guards set-drift, so it was a tax, not a silent bug.)
- **T2.1** `parse_amount` — *removes a real bug class* (silent money-parse divergence). Highest
  material value, highest risk.
- **T2.2** `pdf_text` adapter — *real separation of concerns* ("read PDF lines/words" vs "parse this
  source's records").
- **T2.3** `name_norm_str` — single source for the org join key (affects which rows match).
- **T2.8** refresh step-runner, **T2.5** family `_common` — decouple run/scaffolding from logic; moderate.
- **T3.1** `extractor_base` — *the* decoupling lever: ETL lifecycle vs parse logic across ~90 files.
- **T3.2** procurement scaffolding→`pbe`, **T3.3** `db.py`/`_sql_registry` collapse — real, structural.

**Perf, NOT maintainability** (worth doing for speed, but don't pretend they're decoupling):
- **T1.2** diary native expr (done) — removes a per-row callback over 110k rows; actually *increased*
  surface (two representations + a parity test). Pure perf/idiom.
- **T2.4** polars UDF rewrites, **T2.6** lazy `scan_parquet` — perf/memory only.

**Cosmetic / syntax-only** (single-source-of-truth for something trivial; ~zero stability impact —
do them for tidiness, do NOT pretend they make the codebase materially better):
- **T1.4** `save_json`, **T1.5** `hr()` banner, **T1.6** `_bbox_args`, **T1.7** `PROJECT_ROOT`,
  **T1.8** `QUARANTINE_DIR`, **T1.9** `BROWSER_UA`. Together ~10 files of churn, near-zero leverage.

**Headline:** the genuine "more stable, better codebase" payoff is concentrated in a *few* items —
and they are **not** in the easy tier. The easy tier is mostly cosmetic. The real levers
(`extractor_base`, the money parser, the PDF adapter, the `sql_registry` collapse) are exactly the
ones that cost effort. T1.3 is the one architectural win that *was* also cheap.

## 2. The plan (tiered by effort/risk)

### Tier 1 — easy, behaviour-neutral, do now
| ID | Change | Files | Maintenance win | Risk | Guard |
|---|---|---|---|---|---|
| T1.1 ✅ **DONE** | `SCHEMA_COLS` → module-level `pbe.PAYMENTS_FACT_SCHEMA_COLS` | 5 procurement parsers | one schema, no 5-way drift | low | existing fact/contract tests pass |
| T1.2 ✅ **DONE** | `diary_entry_classify` `map_elements`→native `when/then` | 1 | removes per-row Python callback | low | new expr↔scalar parity test |
| T1.3 ✅ **DONE** | `_run` → shared `run_query(…, label, log)` in `queries/__init__.py`; 23 modules keep a 1-line shim | 23 `dail_tracker_core/queries/*` | **biggest dedup**: one place for the error/log policy | low | 289 core tests pass; ruff clean; messages preserved via `label` |
| T1.4 | Adopt `services.storage.save_json` in the 3 pdf_infra pollers (hand-rolled `mkdir`+`json.dumps`) | 3 | one JSON-write convention | low | byte-equal output |
| T1.5 | Lift `hr()` console banner → `services/console.py:hr(width=…)` | **16** (10 refresh scripts ─×74 + 6 extractors ×70) | one banner | low | stdout only |
| T1.6 | Lift `_bbox_args` ArcGIS envelope query-param builder | 2 planning | one builder | low | byte-identical |
| T1.7 | Adopt `paths.PROJECT_ROOT` in `db.py` (stop recomputing `parents[1]`) | 1 | one root source | low | same path |
| T1.8 | Adopt `services.data_contracts.QUARANTINE_DIR` in `tools/quarantine_report.py` (precedent: `data_fidelity.py`) | 1 | one quarantine dir const | low | same path |
| T1.9 | Add `BROWSER_UA` const to `services/http_engine` | ~15 | one UA string | low | const only — **do NOT** route through the pooled session |
| ~~T1.x~~ | ❌ shared `slugify`/`patterns.py` — **dropped**: the 3 slug sites use *different* separators/char-classes (URL/filename outputs) → merging is **not** behaviour-neutral | — | — | — | see §4 |

### Tier 2 — real modularization, needs a parity test (not free)
| ID | Change | Scope | Maintenance win | Risk |
|---|---|---|---|---|
| T2.1 | `shared/parse_amount.py` — flag-gated `parse_euro_cell(s, *, paren_negative, suffix_multiplier, none_on_fail)`; each caller opts into its exact current behaviour | ~13 money parsers (`to_eur`/`parse_money`/`_num`) | kills the most dangerous silent-drift class | med (value-parity test per caller) |
| T2.2 | `shared/pdf_text.py` — `pdf_lines(doc, skip=…)` + `cluster_word_rows` adapter | ~10 procurement PDF sites | one PDF line/word reader | med (per-source skip/strip params) |
| T2.3 | `name_norm_str()` scalar beside `name_norm_expr`, sharing `LEGAL_SUFFIX_PATTERN` | ~14 org-norm clones | one org key rule | med (reconcile suffix lists first — joins ride on it) |
| T2.4 | Native Polars for remaining ETL UDFs (`str.contains` with `(?i)`, `cum_prod`, `when/then`) | ~11 sites | removes Python round-trips | med (value-parity; `(?i)` flag trap) |
| T2.5 | Family `_common` modules: TED (`load_raw`/`pull`/`build_rows`), pollers (`poll`/`_session`/`_get`) | 3 TED + 4 pollers | per-family scaffolding in one place | med |
| T2.6 | Lazy `scan_parquet` + pushdown for the big facts | payments/procurement reads | memory/perf | med (assert row-identical) |
| T2.7 | Dedupe the two DuckDB connection builders; `print`→logging | 2 | one connection contract | low–med |
| T2.8 | Lift the timed subprocess step-runner (`_subprocess`/`_module`) → `services/refresh_runner.py` | 8 refresh scripts (~60 LOC) | one run/timing/logging path for the refresh entrypoints | med (keep the `*args` superset) |
| T2.9 | Adopt `services.storage.output_exists` for the TED `_cache_is_fresh` guards | 2 (ted×2) | one staleness policy | med (print→log; days→hours mapping) |
| T2.10 | Lift the diary surname-key helper into the shared `_diary_minister` both files already import | 2 | one surname key | low–med (parity test) |
| T2.11 | Make org-norm stragglers ADOPT `name_norm` (`charity_normalise`, `diary_org_match`, `_squish` NOAC labeller) instead of local NFKD clones | ~4 | one org/label norm rule | med (join/label parity — reconcile suffixes) |
| T2.12 | `members_api_service` → `services.http_engine.fetch_json` | 1 | shared retry/validation | med — **NOT behaviour-neutral** (adds retry + schema validation) |

### Tier 3 — architectural (a project, biggest win)
| ID | Change | Scope | Maintenance win | Risk |
|---|---|---|---|---|
| T3.1 | `services/extractor_base.py` — a lifecycle runner: bootstrap preamble + standalone logging + standard `--refresh/--force` CLI + GREEN/AMBER `fidelity_check` shell + `save_parquet` + coverage-JSON write | ~90 extractors | each extractor shrinks to its parse logic; one run contract | high (bootstrap import-ordering trap; migrate incrementally) |
| T3.2 | Procurement parser scaffolding (`_confidence`/`summarise_outlier`/`build_coverage`) onto `pbe` | nphdb/nta/seai | parser scaffolding in one place | med — **add a SCHEMA_COLS order-pin test first** |
| T3.3 | Collapse `db.py` ↔ `utility/data_access/_sql_registry.py` duplication | read-layer | one view-registration path | med (already team-planned) |

## 2c. Bloat-strategy investigation (2026-06-28) — firewall / libraries / OO / bend-the-curve

Multi-agent pass (27 agents) over the UI + a 10-category library scan + an OO devil's-advocate.

**Streamlit firewall audit:** 38 candidate leaks raised, ~6 confirmed by an adversarial verify
pass (which correctly killed its own false positives, e.g. `corporate.py:1312/1293`). Real,
confirmed UI business-logic leaks to migrate to **DuckDB views**: `ui/vote_explorer.py:618`
(SQL-in-UI), `pages_code/ministerial_diaries.py:351` (pandas fact-derivation),
`procurement.py:1539/1659` (the recurring-charge `>=2` never-sum threshold, *duplicated*),
`your_councillors.py:230`, `statutory_instruments.py:528`, `election_2024.py:88`. 27/38 → view.
Low-sev display items (`scale_max` bar-sizing, `_spend_scale` sort key) correctly respect the
firewall — leave. Bounded, not systemic.

**Library scan:** the project is ALREADY library-disciplined (`flatten_json`,
`pandas.json_normalize`, `dateutil`, polars dates, **bs4 in 6+ extractors**, `shapely`,
`hashlib`, `lru_cache` = "already_using"). Only genuine adopt: **bs4 in ~5 more hand-rolled HTML
scrapers** (already a dep, ~60–120 LOC). The fuzzy/name/date/money "candidates" are TRAPS —
they're exact join keys / expected-value cross-checks; rapidfuzz/unidecode/dateutil-fuzzy would
silently change which rows match (firewall hazard). Bloat is STRUCTURAL, not reinvented-wheels.

**OO verdict:** the maintainer's instinct is right. Repo has ZERO behavioral inheritance (one
exception subclass aside) — already function + config-dict + frozen-dataclass, like dbt/polars/
Airflow. Light OO already earns its keep (`QueryResult`, `Deflator`, `Breaker`) — do NOT flatten.
Only arguable OO move: a Template-Method base over the 5 payment parsers, templating LIFECYCLE
ONLY (never `parse`) — would fix a real inconsistency (nphdb/nta enforce the `min_rows` row-floor;
**seai + hse_tusla silently omit it**). Even so, marginal since `pbe`-as-module already centralises
most of it.

**Bend-the-curve levers (ranked; with skepticism applied):**
1. **AST firewall-guard test** (low effort, high value) — no test enforces the firewall today; one
   that fails on `conn.execute`/raw-SQL/fact-derivation in `utility/pages_code|ui` PREVENTS the 26k-
   LOC UI re-accreting logic. Changes the trajectory. **Top pick.**
2. **Payment-parser lifecycle template** (med) — ~1,635 LOC; 6th publisher 120→30 lines + fixes the
   row-floor drift.
3. **Shared HTTP-retry helper** (ted_search + 4 wikidata copy the loop) + **bs4 in 5 scrapers** — bounded.
4. **Extend the contract engine with tuples, not a class DSL** (the live memory task).
- ⚠️ The synthesis's "finish paths.py migration, ~700–1,000 LOC" is **OVERSTATED**: the bootstrap
  ordering paradox blocks it for the ~226 subprocess-invoked scripts (you need `ROOT`+`sys.path.insert`
  *before* you can import `paths`). The real structural fix is **packaging** (editable install →
  preamble disappears), i.e. the parked reorg plan — not a 226-file edit.

## 3. Explicitly NOT doing (honest guardrails)
- ❌ A single mega "all regexes" file — incohesive and firewall-risky (see §4).
- ❌ Force-merging money/PO/PDF parsers — deliberate per-source divergence feeds money facts.
- ❌ Wholesale pandas→polars conversion — the split is intentional.
- ❌ Merging the SIPO OCR watchdogs — fragile off-box OCR, hand-tuned timing.
- ❌ Refactoring `pipeline_sandbox/` — deliberately sandboxed/fragile.
- ❌ `corporate_receiver_enrich` iterrows→explode — `_reference_topn_and_buckets` is a deliberate parity guard.

## 4. Regex policy — VERDICT (confirmed across 604 literals in the rest of the tree)
**No central `shared/patterns.py`.** A single "all regexes" module is *actively dangerous* here:
the inventory is dominated by per-source/per-document-format patterns (money parsers, PDF table
header/noise filters, Iris/SI legal-notice grammars, court-cause numbers, sector keyword
alternations) that look alike but diverge in load-bearing ways — co-locating them invites an
accidental "tidy" that changes which cells parse (a money-fact hazard). It would also create a
god-module every ETL imports, turning one regex edit into a whole-tree review.

Recommendation: **`small_shared_leaves_only`** — keep format-specific regexes local; make
stragglers ADOPT the homes that already exist (chiefly `shared/name_norm.py`).
- **Centralise/adopt (genuinely identical, cross-domain):** accent-fold (NFD + strip combining
  marks), whitespace-collapse, the legal-suffix alternation, HTML-tag strip. These mostly belong
  in `shared/name_norm` / `shared/text_encoding`, which already own them — point the clones there.
- **Keep local (per-source / firewall-sensitive):** euro amounts, PO/case/SI/cause numbers,
  legal-diary & Iris/SI grammars, sector/acronym keyword lists, PDF header/column classifiers,
  PDF total/subtotal noise filters, date/quarter grammars, DOM/scraper extractors, the TD
  name-key.
- ⚠️ Even **`slugify` is not a safe merge**: the 3 look-alike sites use different separators and
  char-classes and emit URL/filename components — unifying them changes emitted slugs/URLs.

## 5. Sequencing (re-prioritised by §1b material value, not by ease)
1. ✅ **T1.3** done — the one cheap *and* architectural win.
2. **Skip / batch-last the cosmetic tier** (T1.4–T1.9). Do them only opportunistically when a file
   is open for another reason; they do not justify dedicated PRs.
3. The real levers, in increasing effort: **T2.2** (pdf_text adapter) → **T2.8** (refresh runner) →
   **T2.1** (money parser, highest value/highest risk) → **T3.3** (sql_registry collapse) →
   **T3.1** (`extractor_base`, the big one).
4. Perf items (T2.4, T2.6) only when a real slowness/memory problem is measured.

One module per PR, each with a parity/equality test (or a corpus-level check like the diary one),
matching the existing reversible-PR style.

**Status (2026-06-27):** whole ETL tree now scanned (the procurement+corporate clusters deep-verified;
the other 12 production chunks easy-wins-verified). T1.1–T1.3 implemented and green. §4 regex verdict final.
