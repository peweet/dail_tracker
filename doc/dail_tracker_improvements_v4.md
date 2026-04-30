# dail_tracker — improvements roadmap (v4)

_Last revised: 2026-04-30. Supersedes `dail_tracker_improvements_v3.md`._

This is the live improvement roadmap. It is opinionated, dated, and reconciled with the project as it actually stands today.

**What this doc is.** A comprehensive list of improvements across the whole project — robustness, modelling, performance, UI, ops, distribution, trust, developer experience, and sustainability. It is meant to be useful as both an in-order to-do list and as occasional reading for thinking about where to push next.

**What this doc is not.** A catalogue of new datasets to add. That lives in `ENRICHMENTS.md` and is deliberately kept separate so this roadmap stays focused on the existing surface and how to make it solid.

---

## Table of contents

0. [Status snapshot](#0-status-snapshot-april-2026)
1. [Architectural principles still load-bearing](#1-architectural-principles-still-load-bearing)
2. [Honest readiness picture](#2-honest-readiness-picture)
3. [Auto-refresh — the biggest single unlock](#3-auto-refresh--the-biggest-single-unlock)
4. [Pipeline robustness](#4-pipeline-robustness)
5. [Tests, CI, dependency hygiene](#5-tests-ci-dependency-hygiene)
6. [Data modelling](#6-data-modelling)
7. [Performance](#7-performance)
8. [UI / UX maturity](#8-ui--ux-maturity)
9. [Observability and ops](#9-observability-and-ops)
10. [Security and licensing](#10-security-and-licensing)
11. [Distribution and citation](#11-distribution-and-citation)
12. [Trust and methodology](#12-trust-and-methodology)
13. [Developer experience](#13-developer-experience)
14. [Sustainability and bus factor](#14-sustainability-and-bus-factor)
15. [Hosting and cost](#15-hosting-and-cost)
16. [AI-assisted development discipline](#16-ai-assisted-development-discipline)
17. [Recommended 90-day sequence](#17-recommended-90-day-sequence)
18. [Reading list](#18-reading-list)
19. [What changed in this rev](#19-what-changed-in-this-rev)

---

## 0. Status snapshot (April 2026)

### What's shipped since earlier revisions

- **Pipeline orchestration is real.** `pipeline.py` runs end-to-end. Module-level execution has been removed. `pipeline_sandbox/` holds in-flight enrichments without polluting the main pipeline.
- **Medallion is implemented.** `data/bronze/`, `data/silver/`, `data/gold/parquet/` exist with stable outputs. `GOLD_LAYER_NOTES.md` documents the gold layer.
- **SQL-first analytical layer.** Around 30 registered views in `sql_views/` (attendance, payments, lobbying, votes, legislation, interests). DuckDB is the analytical engine.
- **Page contracts in place.** `dail_tracker_bold_ui_contract_pack_v5/` defines per-page YAML contracts, agents, and skills. The UI is being driven from contracts, not ad-hoc Python.
- **Streamlit is mostly thin.** Eight pages: attendance, member overview, votes, interests, payments, lobbying, legislation, committees. Heavy logic is in views.
- **Honest caveat documentation.** `DATA_LIMITATIONS.md` is unusually thorough and is the single biggest trust asset the project has.
- **A working bold-UI redesign loop.** Skills for `bold-redesign-page`, `civic-ui-review`, `shape`, `streamlit-frontend` exist and are being used.

### What's still missing — and matters

1. No public deployment. Nothing is live. There is no feedback loop with users.
2. No automated refresh. Every dataset is refreshed manually.
3. No CI. No GitHub Actions. No automated tests on push.
4. PDF parsers are unprotected by golden-file regression tests.
5. Lobbying ingestion is manual CSV — the most analytically valuable source has the most fragile path.
6. Fuzzy join keys are still primary in some paths; `match_method` and confidence are not first-class.
7. Provenance is documented in markdown but not surfaced in the UI from manifests.
8. Bus factor of one. No contributor onboarding doc; no scheduled refresh = no dead-man's-switch if attention lapses.

### What this rev is structured around

The asymmetry to internalise: **the analytical layer is materially closer to beta than the operational layer is.** The fastest readiness gains come from operationalising what already works, not from building new analytical surfaces. v4 is structured accordingly — robustness, ops, distribution, trust come first; new dataset ideas live in `ENRICHMENTS.md`.

---

## 1. Architectural principles still load-bearing

These have not changed and should not be relitigated. The corresponding skills and contracts already enforce most of them.

### 1.1 Thin Streamlit, SQL-heavy

> A Streamlit page should mostly contain `SELECT`, `filter`, `display`, `download`.

If a page is doing joins, name normalisation, deduplication, ranking, or metric definition, that logic belongs in SQL or the pipeline. The contract pack flags this with `business_logic_in_page: forbidden`; the `civic-ui-review` skill enforces it.

### 1.2 Page contracts are the AI-facing API

Each page is governed by a small YAML contract under `dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/`. AI page generators read the contract, not the warehouse. Token cost stays manageable; metric definitions don't drift.

A good contract names: source view, grain, primary key, allowed filters, display columns, charts, export options, provenance fields. A bad contract starts encoding join logic — that's a smell, push it into SQL.

### 1.3 Provenance is data, not prose

Provenance should live in tables (`meta_pipeline_runs`, `meta_dataset_registry`, `meta_dataset_lineage`, `meta_source_registry`) and per-mart manifests (`*.manifest.json`). The UI reads from manifests; it does not hard-code freshness, run IDs, or caveats.

This is partially done. Manifests are not yet emitted for every mart (§3.5).

### 1.4 What stays where

| Layer | Owns |
|---|---|
| Pipeline (Python/Polars) | Source ingestion, normalisation, entity resolution, parser logic |
| SQL views (DuckDB) | Joins, aggregations, metric definitions, ranking, marts |
| YAML contracts | Page structure, filters, charts, columns, provenance fields |
| Streamlit | Layout, widget placement, presentation, tabs, expanders |
| Skills + agents | Contract enforcement, UI review, pipeline-view boundary |

### 1.5 The pipeline-sandbox rule

`pipeline_sandbox/` is for new Python/Polars enrichment work. It graduates into `pipeline.py` only after a fixture test, a manifest writer, and a registered SQL view exist. New SQL views go directly to `sql_views/`. The core `pipeline.py`, `enrich.py`, `normalise_join_key.py` are change-controlled.

---

## 2. Honest readiness picture

| Layer | Maturity (1–5) | Confidence | Notes |
|---|---|---|---|
| Vision and scope | 4 | High | Civic mission, well-bounded; current Dáil only. |
| Data sources and coverage | 3 | Medium | Lobbying manual; otherwise solid. |
| Pipeline robustness | 2 | Low | No fixture tests, no schedule, no quarantine flow. |
| Analytical layer (SQL) | 4 | High | View structure is sound; ~30 registered views. |
| UI / dashboard | 3 | Medium | Actively refactoring under contract pack v5. |
| Trust and provenance docs | 4 | High | DATA_LIMITATIONS.md is a real asset. |
| Tests / CI / Ops | 1 | Low | Partial tests, no CI, no deploy. |
| Distribution / citation | 1 | Low | No release artefacts, no permalinks, no API. |
| Sustainability / bus factor | 1 | Low | Single maintainer, no onboarding. |

The asymmetry: vision and analytics are 4/5; everything operational is 1–2/5. v4 is built around closing that gap.

---

## 3. Auto-refresh — the biggest single unlock

Auto-refresh is the single largest readiness lift available right now. It transforms a hand-cranked artefact into something that survives weeks of inattention. The mechanics matter, so this section is concrete.

### 3.1 What "auto-refresh" must mean here

Four kinds of source:

| Source kind | Examples | Auto-refresh feasibility |
|---|---|---|
| Public REST/JSON API | Oireachtas API (members, legislation, questions, votes, debates) | High — no auth, stable schemas |
| Public PDF, predictable URL | Attendance, payments, interests | Medium — URLs stable but layout drift risk |
| Public PDF, varying URL | Some interests/payments PDFs | Medium — needs `pdf_endpoint_check` to discover |
| Manual CSV | lobbying.ie | Low without scraping work |

Auto-refresh = on a schedule, regenerate silver/gold parquet from upstream, write back into the deployed app, and keep a structured record of what happened.

### 3.2 Reference architecture (cheap)

```text
GitHub repo (main)
  └─ pipeline code, sql_views, page contracts, Streamlit app

GitHub Actions (cron + workflow_dispatch)
  ├─ pulls API + PDFs
  ├─ runs pipeline.py
  ├─ runs DuckDB SQL views to materialise parquet marts
  ├─ writes/updates per-mart manifest.json
  ├─ writes meta_pipeline_runs row
  └─ commits regenerated parquet to a `data` branch

Streamlit Community Cloud
  ├─ tracks `main` for app code
  └─ tracks `data` branch for parquet (or pulls at start)

Hugging Face Datasets (optional, for size headroom)
  └─ mirrors the parquet for size-bound deploys
```

The key separation: **app code on `main`, data on `data` branch.** This keeps git history readable, makes data-only rebuilds cheap, and lets a bad refresh be rolled back without touching code.

### 3.3 GitHub Actions skeleton

`.github/workflows/refresh.yml`:

```yaml
name: Refresh data

on:
  schedule:
    # Mondays at 04:00 UTC. Tighten cadence per source under §3.4.
    - cron: '0 4 * * 1'
  workflow_dispatch:
    inputs:
      sources:
        description: 'Comma-separated source slugs (default: all)'
        required: false
        default: 'all'

permissions:
  contents: write

jobs:
  refresh:
    runs-on: ubuntu-latest
    timeout-minutes: 60
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
          cache: 'pip'

      - run: pip install -e ".[dev,db]"

      - name: Run pipeline (refresh-only)
        env:
          DAIL_REFRESH_SOURCES: ${{ github.event.inputs.sources || 'all' }}
        run: python pipeline.py --refresh

      - name: Write run manifest
        run: python -m utility.tools.write_run_manifest

      - name: Commit refreshed data to data branch
        run: |
          git config user.name "dail-tracker-bot"
          git config user.email "dail-tracker-bot@users.noreply.github.com"
          git fetch origin data:data || git checkout --orphan data
          git checkout data
          git checkout main -- data/ sql_views/
          git add data/gold/parquet/*.parquet \
                  data/silver/parquet/*.parquet \
                  data/gold/parquet/*.manifest.json
          if git diff --staged --quiet; then
            echo "No data changes."
          else
            git commit -m "chore(data): scheduled refresh $(date -u +%Y-%m-%dT%H:%MZ)"
            git push origin data
          fi
```

The two referenced things — `pipeline.py --refresh` and `utility/tools/write_run_manifest.py` — don't exist yet and are the first concrete tasks (§17).

### 3.4 Per-source cadence and risk

| Source | Cadence | Risk | Notes |
|---|---|---|---|
| Oireachtas API (members, legislation, questions, votes, debates) | Daily | Low | Add proper pagination loops where `limit=…` is still used. |
| Attendance PDFs | Weekly | Medium | New PDFs publish ~weekly while Dáil sits. Schedule after publication day. |
| Payments PDFs (PSA) | Quarterly | Medium | Refresh just after each PSA publication; soft-fail if not yet up. |
| Interests PDFs | Annual | High | Layout drift between years; needs golden-file test before each annual run. |
| Lobbying.ie | Tri-annual + ad-hoc | High | Currently manual. Build a Playwright job that mimics the export form; commit CSVs to bronze. Treat as best-effort, not blocking. |

### 3.5 Manifests and freshness UI

Each mart writes a manifest beside its parquet:

```json
{
  "dataset_name": "mart_member_overview",
  "layer": "mart",
  "grain": "one row per member",
  "built_from": ["dim_member", "fact_attendance", "bridge_lobby_return_member"],
  "row_count": 174,
  "run_id": "2026_04_30_001",
  "git_commit": "c7a67de",
  "source_versions": {
    "oireachtas_api": "fetched 2026-04-30T04:12Z",
    "attendance_pdfs": "covers 2024-01..2026-04",
    "lobbying_ie": "manual export 2026-03-10"
  },
  "caveats": [
    "Lobbying counts include only returns where a member name resolved.",
    "Payments aggregate excludes quarantined rows; see payments_quarantined.parquet."
  ]
}
```

Every page renders a "Source and methodology" expander **populated from the manifest**, not hard-coded. ~30 lines of helper, reused across pages. This is the single biggest credibility upgrade for a journalist user.

### 3.6 Failure handling

A scheduled refresh that fails silently is worse than no refresh. The workflow must:

1. Fail loudly per-source, succeed in aggregate. A broken interests PDF parser must not block attendance refresh. Use try/except with a structured error log written into `data/_run_errors.json`.
2. Open a GitHub issue on failure (`gh issue create`).
3. Surface staleness in the UI. Freshness badge per page goes amber if a relevant source has not refreshed in N expected cycles.
4. Always emit a `meta_pipeline_runs` row including partial-failure status.

### 3.7 What auto-refresh does NOT solve

Schema drift. A new column appearing in the Oireachtas members API will not break the workflow but will silently fail to populate downstream tables. That is what schema validation (§5.5), row-count drift assertions (§4.3), and the quarantine flow (§4.5) are for. Add those at the silver write step before adding more sources.

---

## 4. Pipeline robustness

The pipeline runs but is unprotected. These items make it resilient to upstream change.

### 4.1 Schema validation at silver writes

Use pandera (or pydantic for less-tabular cases) at every silver-write step. Fail loudly when a column type drifts or a non-null column starts allowing nulls. Currently a drift would propagate silently into gold.

```python
from pandera.polars import DataFrameSchema, Column

ATTENDANCE_SILVER_SCHEMA = DataFrameSchema({
    "member_id": Column(str, nullable=False, unique=False),
    "sitting_date": Column("date", nullable=False),
    "present": Column(bool, nullable=False),
    "source_pdf": Column(str, nullable=False),
    "source_page": Column(int, nullable=False),
    "parser_version": Column(str, nullable=False),
})
ATTENDANCE_SILVER_SCHEMA.validate(df)
```

Schemas live next to the pipeline module that writes them, not in a central registry — that way drift causes a noisy local failure, not a global breakage.

### 4.2 Golden-file PDF regression tests

Pick one representative PDF per parser (attendance, payments, interests, sponsors). Commit it to `test/fixtures/`. Commit the expected silver output beside it. Run on every PR.

```python
def test_attendance_parser_2024_q1():
    out = parse_attendance(Path("test/fixtures/attendance_2024_q1.pdf"))
    expected = pl.read_parquet("test/fixtures/attendance_2024_q1.expected.parquet")
    pl.testing.assert_frame_equal(out, expected)
```

This is the single most valuable test in the project. It will catch parser regressions that no schema check can.

### 4.3 Row-count drift assertions

When a refresh produces ≥10% fewer rows than the prior run for any silver table, fail the run unless explicitly allowed. The threshold is tunable per source.

Stored beside each manifest:

```json
"row_count_history": [
  {"run_id": "...", "rows": 4221, "delta_pct": null},
  {"run_id": "...", "rows": 4334, "delta_pct": 2.7},
  {"run_id": "...", "rows": 3812, "delta_pct": -12.0}
]
```

That third row should hard-fail.

### 4.4 Source endpoint health checks

`pdf_endpoint_check.py` already exists. Extend it to check every endpoint the pipeline touches and run it as the first step of every refresh. A 404 on a known-good URL should not silently propagate to "0 rows extracted".

### 4.5 Quarantine flow for bad rows

Already partially exists for payments (`payments_quarantined.parquet`). Generalise:

- Every silver writer accepts a `quarantine` callback.
- Rows that fail validation go to `data/silver/_quarantine/<source>_<run_id>.parquet` with the failed-rule annotation.
- A nightly summary opens an issue if any source has more than N quarantined rows in a run.
- Quarantined rows never silently disappear.

### 4.6 Parser idempotency

Every parser should produce identical output for identical input. This is true today for most modules but is not asserted. A simple test:

```python
def test_payments_parser_idempotent():
    out_a = parse_payments(FIXTURE_PDF)
    out_b = parse_payments(FIXTURE_PDF)
    pl.testing.assert_frame_equal(out_a, out_b)
```

Idempotency is the precondition for safely re-running refreshes.

### 4.7 Manifest discipline

Every gold mart writes a manifest at build time. A page that loads a mart without a manifest should warn loudly, not silently render with stale provenance. This is enforceable via a small helper that the page contract loader calls.

---

## 5. Tests, CI, dependency hygiene

These are small individually and large in aggregate.

### 5.1 Minimum viable CI

`.github/workflows/ci.yml`:

- `ruff check .`
- `ruff format --check .`
- `pytest test/`
- Page-import smoke: import every module under `utility/pages_code/` and ensure no top-level error.
- Schema diff: re-read each gold parquet and compare column types against a committed `schemas/*.json`.

Half a day to set up. Pays back forever.

### 5.2 Test layering

Three layers, each with a clear role:

- **Unit.** Pure functions: name normalisation, fuzzy-key generation, date parsing. Fast, no I/O.
- **Fixture.** Parser tests against committed PDF/CSV fixtures (§4.2).
- **Smoke.** Import every page; load every gold mart; run every SQL view against the committed parquet.

Don't have integration tests pulling live data — those belong in the scheduled refresh, not on every PR.

### 5.3 Pin and lock dependencies

`pyproject.toml` should have version specifiers. A lockfile (`uv.lock` or `requirements.txt` from `pip-compile`) should be committed. Run `pip-audit` weekly.

This is also the prerequisite for reproducible Streamlit Community Cloud deploys.

### 5.4 Pre-commit hooks

Minimum:

- ruff
- ruff-format
- detect-secrets (or an equivalent — accidental secret commits are easy)
- yamllint for page contracts

`pre-commit run --all-files` in CI catches what local hooks miss.

### 5.5 Type hints + pydantic

Add type hints to every public function. Use pydantic for config schemas, page contracts, and source manifests. This pays off in editor support and in catching contract drift at load time, not at use time.

### 5.6 Page-import smoke test

Every page in `utility/pages_code/` should import cleanly without any data being present. The contract loader, the page function definition, and the helpers should all stand up. This catches the most common Streamlit failure mode: a missing import or typo that only surfaces when the page is opened.

---

## 6. Data modelling

The current model is medallion + ad-hoc gold marts. There is room for it to become more deliberate without becoming a dbt project.

### 6.1 Move toward dim/fact/bridge

Today there is a mix of "wide gold marts" (e.g. `enriched_td_attendance`) and "fact-shaped views" (e.g. `vote_member_detail`). The wide marts are convenient for one-off pages but become liabilities when a column needs to change.

Target shape:

- `dim_member`, `dim_party`, `dim_constituency`, `dim_government` — dimensions.
- `fact_attendance`, `fact_vote`, `fact_payment`, `fact_question`, `fact_bill_sponsorship` — facts at their natural grain.
- `bridge_lobby_return_member`, `bridge_member_committee` — many-to-many.
- `mart_*` views built on top of facts/dimensions for each page.

The migration is incremental — pick one wide mart per quarter and split it.

### 6.2 Replace fuzzy joins where canonical IDs exist

The Oireachtas API exposes a stable `pId` for each member. Use it as the primary key in `dim_member`. The sorted-character fuzzy key from `normalise_join_key.py` becomes a *fallback* match method, not the primary key, with confidence flag.

### 6.3 Match confidence as first-class

Every join that depends on name resolution should carry:

- `match_method`: `pid_exact` | `name_exact` | `fuzzy_sorted_char` | `manual`
- `match_confidence`: `high` | `medium` | `low`
- `match_evidence`: optional pointer to manual review note

Pages that count rows can filter by confidence. `low` matches go in a "review queue" view, not the main page.

### 6.4 Quarantine tables per source

§4.5 covers this in pipeline terms. The data-modelling consequence: every source has a paired `_quarantine` table that the page can expose under "data quality issues".

### 6.5 Slowly changing dimensions for member metadata

Members change parties, constituencies, and roles mid-Dáil. The current model implicitly snapshots latest state. For longitudinal analysis (especially once SIPO donations and historical voting are added), `dim_member_history` with valid-from / valid-to is the right shape. Add it before adding cross-cycle datasets, not after.

### 6.6 Surrogate keys vs natural keys

Use natural keys where they're stable (Oireachtas `pId`, lobbying.ie `primary_key`). Use surrogates only where natural keys are absent or unstable (judges, donors, charities). Document the choice per dimension in `meta_dataset_registry`.

---

## 7. Performance

Streamlit Community Cloud has constrained resources. This matters more than it should.

### 7.1 Push joins into DuckDB

Already mostly done. The remaining wide-mart pages (member overview, attendance overview) still do some joins in Python — finish moving them into views.

### 7.2 Pre-aggregate where it pays

If a chart is the same chart for every visitor, build it as a pre-aggregated view. The page filters columns; it does not recompute aggregations.

`payments_yearly_evolution.sql` is the right shape. `current_dail_vote_history.parquet` is too wide and gets re-aggregated by the votes page — that's a candidate for pre-aggregation.

### 7.3 Streamlit caching discipline

Two rules:

- `st.cache_resource` for the DuckDB connection. One per app.
- `st.cache_data(ttl=…)` for query results. TTL set to "as long as the data branch is unchanged" — typically 1 hour is fine.

Cached return values must be picklable. Returning a polars DataFrame is fine; returning a closure is not.

### 7.4 Parquet typing

Specify dtypes at write time. `int64` for counts, `int32` for years, `categorical` for low-cardinality columns (party, court, chamber). Compressed with zstd by default. This makes Streamlit page loads visibly faster and saves a meaningful chunk of repo size.

### 7.5 Limit page-time work

Rule of thumb: a page should do less than 500 ms of work between cached query and render. Anything more belongs in the SQL view.

---

## 8. UI / UX maturity

The contract pack and bold-redesign skill have done the architectural work; the remaining items are concrete user-facing fixes.

### 8.1 Provenance footer on every page (auto)

Helper: `render_provenance(manifest_path)`. Reads the manifest, renders an expander with: source mart, grain, last refresh, run ID, git commit, source versions, caveats. Wired into every page's last 3 lines.

### 8.2 Freshness badge per page

Top of every page: a small pill showing "Refreshed N days ago" with colour coding (green < 7d, amber 7–30d, red > 30d for sources expected to refresh more often than that).

### 8.3 Mobile responsiveness within Streamlit limits

Streamlit's mobile story is acceptable for narrow tables and metrics, painful for wide tables and side-by-side columns. Decisions:

- Wide tables: collapse to card layout under N pixels (existing card pattern in `shared_css.py`).
- Side-by-side columns: stack on narrow screens (Streamlit default is okay).
- Charts: explicit min-height; let Streamlit handle width.

This is a 2–3 day pass over all pages, not a structural change.

### 8.4 Accessibility audit

Run `axe-core` against the deployed app. The Streamlit baseline is decent but card patterns and custom CSS can break contrast and keyboard navigation. Track findings in an issue, fix highest-impact items first.

### 8.5 Page-mart-per-page rule

Every page reads from one named mart, ideally. Where a page reads two, that's a candidate for a unifying view. Where it reads three or more, the page is doing modelling work that belongs in SQL.

### 8.6 Cross-page navigation

A TD's name on the lobbying page should link to that TD on the member overview page. Today it doesn't. Cross-page navigation is the difference between "a dashboard" and "an explorer". Implementation: query-param-driven page state; helper for "this entity's profile URL".

### 8.7 Search across entities

A single global search box: TD names, lobbying organisations, bills, committees. Implemented as a small in-memory index built at app startup; click result → deep link to relevant page filtered to that entity.

### 8.8 Onboarding for first-time visitors

The first visit currently lands on the attendance page. That's not the strongest entry point. Either:

- A landing page that explains what the project is and links to two or three "good places to start" queries; or
- The member overview page as default with a short explainer banner.

### 8.9 Empty states and zero-result handling

Already partly done via `empty_state` helpers. Audit every page for: zero filter results, all-data-quarantined, source-currently-refreshing. Each should explain what happened and suggest what to do.

---

## 9. Observability and ops

Once the refresh is automated, the question becomes "is it healthy?" rather than "did I run it?".

### 9.1 Run summaries

Every refresh emits a single JSON document:

```json
{
  "run_id": "2026_04_30_001",
  "started_at": "2026-04-30T04:00:00Z",
  "finished_at": "2026-04-30T04:14:21Z",
  "git_commit": "c7a67de",
  "sources": {
    "oireachtas_api": {"status": "ok", "rows_added": 412},
    "attendance_pdfs": {"status": "ok", "rows_added": 0, "no_new_pdf": true},
    "lobbying_ie": {"status": "skipped", "reason": "manual"}
  },
  "errors": [],
  "warnings": ["lobbying_ie not refreshed in 51 days"]
}
```

Committed under `data/_run_summaries/run_*.json`. The UI surfaces the latest one on a "Pipeline status" page.

### 9.2 Structured logs with run_id

Every log line carries the `run_id`. Logs go to stdout (so GitHub Actions captures them) and to `data/_logs/run_*.jsonl`.

### 9.3 Error issue auto-creation

When a refresh has any error, the workflow opens a GitHub issue with the run summary attached. Triage from the issue.

### 9.4 Data freshness SLOs

Per source, a target cadence and a stale-warning threshold:

| Source | Target | Warn | Fail |
|---|---|---|---|
| Oireachtas API | Daily | 3 days | 7 days |
| Attendance PDFs | Weekly | 14 days | 30 days |
| Payments PDFs | Quarterly | 100 days | 180 days |
| Interests PDFs | Annual | 365 days | 540 days |
| Lobbying.ie | Tri-annual | 130 days | 200 days |

The freshness badge (§8.2) and the latest run summary use these thresholds.

### 9.5 A simple "Pipeline status" page

A dedicated Streamlit page showing: last run, per-source freshness, any open issues from the auto-creator, the last 30 days of runs as a sparkline. Targeted at the maintainer, but useful for journalists too — "is the data current as of when I'm writing my piece?".

---

## 10. Security and licensing

A civic-data project lives or dies on whether its handling of public data is defensible. None of these are heavy items; missing them is the risk.

### 10.1 Env vars and secrets

Move all environment-specific config to environment variables. Commit a `.env.example`. No secrets in the repo, no API keys in code.

### 10.2 Dependency scanning

`pip-audit` + Dependabot. Both run weekly.

### 10.3 Source licensing per dataset

`doc/source_licensing.md`: one row per source, with: licence type, attribution required, redistribution allowed, link to source's terms. The provenance footer (§8.1) reads from this.

### 10.4 GDPR-light considerations

The project handles public-record data about identified individuals (TDs). That is not a GDPR safe-harbour automatically; it relies on the public-interest journalism / democratic-accountability lawful bases. Document the position in `doc/data_protection_position.md`. Include:

- categories of personal data processed,
- lawful basis,
- data minimisation steps (e.g. addresses redacted from PDFs),
- subject rights handling,
- retention.

This is one document, not a project. A version of it should exist before the alpha is shared with a single user.

### 10.5 Robots.txt and ToS compliance

Each scrape job (lobbying.ie, Iris Oifigiúil, courts.ie) needs a documented check against the source's robots.txt and terms of service. The check goes into the source's manifest. If a source's terms exclude automated retrieval, that source does not get scraped — full stop.

### 10.6 Rate-limit and identify the bot

Every outbound request from the refresh sends a `User-Agent: dail-tracker-bot (https://github.com/...)` header and respects per-source rate limits. Don't be the project that gets a public records site to add a captcha.

---

## 11. Distribution and citation

Right now the project is a private dashboard. Distribution is what makes it useful to someone who isn't the maintainer.

### 11.1 Versioned data releases

Every Monday's refresh tags a release: `data-v2026.04.30`. The release contains:

- All gold parquets.
- All manifests.
- The run summary.
- A `RELEASE_NOTES.md` derived from `meta_pipeline_runs`.

This is what makes citations possible: "data as of release v2026.04.30" is a stable claim.

### 11.2 Permalinks per page state

Page state (filter values, sort order) lives in URL query params. A user can bookmark, share, and cite a specific view. Streamlit's `st.query_params` API handles this. Page contracts should declare which filters are URL-bindable.

### 11.3 Open data exposure

Three increasing levels:

1. **Parquet downloads.** Every gold mart is downloadable from the page footer. Already supported by Streamlit's CSV button; switch to parquet for typed downloads.
2. **DuckDB-WASM in the browser.** The whole gold dataset is small enough to ship to the browser. This unlocks "click here to query the data yourself" with no server.
3. **A read-only HTTP API.** Optional. Only worth doing if a user actually asks for it.

Levels 1 and 2 are cheap and high-leverage. Level 3 is not until someone needs it.

### 11.4 RSS / Atom for new events

A feed per dataset:

- New lobbying returns this week.
- New parliamentary questions this week.
- New attendance data published.
- (Once D.1 graduates) New judicial appointments.

Journalists who follow Irish politics can subscribe and skim. This is one of the highest leverage-to-effort ratios available.

### 11.5 Citation guidance

`doc/citation.md`: how to cite the project, including release version and access date. Crucial for academic and journalistic use; nobody cites what doesn't tell them how.

---

## 12. Trust and methodology

`DATA_LIMITATIONS.md` is engineer-quality. The trust gap is everything *between* that doc and the page.

### 12.1 Journalist-readable methodology

`doc/methodology.md`: one page per dataset. For each:

- what the numbers mean,
- what they don't mean,
- known caveats,
- worked example of a single record from source to chart.

Aimed at a journalist who has 10 minutes before filing.

### 12.2 Per-page source citations

Every chart and every table caption ends with a one-line source citation, drawn from the manifest. Currently most pages have an expander; that's good but not enough. The citation should be visible without expanding.

### 12.3 Caveat banners where data is partial

Where `DATA_LIMITATIONS.md` flags a known issue (e.g. office-holders' interests in §2.1, lobbying collective targets in §7.7), the relevant page shows a small inline banner. Not an expander. Not a footnote. A visible banner.

### 12.4 Update history per dataset

Each gold mart's page shows a small "data updates" log: "2026-04-30: refreshed; 2026-04-23: refreshed; 2026-04-16: parser fix for new attendance PDF layout". Built from `meta_pipeline_runs`. Three entries shown, link to a full history.

### 12.5 Public changelog

`CHANGELOG.md` at the repo root, kept in keepachangelog.com style. Every release entry. Every parser fix. Every new dataset. The bar for entry: "would a user notice?".

### 12.6 Methodology review by an external reader

Before the alpha goes to a journalist, hand `methodology.md` and the DATA_LIMITATIONS doc to one independent reader who hasn't worked on the project. Their first 10 questions are gold.

---

## 13. Developer experience

The fastest way to make progress sustainable is to lower the cost of every change.

### 13.1 One-command bootstrap

`make bootstrap` (or `just bootstrap`):

1. Creates venv.
2. Installs dependencies (locked).
3. Pulls a small fixture-only data bundle.
4. Runs the smoke test suite.

A new contributor (or future-you after a break) is productive in five minutes, not five hours.

### 13.2 Contribution guide

`CONTRIBUTING.md`: how to set up, where to put new code (sandbox vs core), how to run tests, how to run the dashboard locally, how to add a page contract. Short.

### 13.3 Module size discipline

The largest current modules (`pipeline.py`, `enrich.py`) are doing a lot. Target: no module over 600 lines. Split by responsibility, not by size. The contract pack already imposes some of this; carry it through the rest of the codebase.

### 13.4 Tests as documentation

A new contributor reading `test/` should be able to learn what each parser is supposed to do. This is a writing-style thing: test names describe behaviour, fixtures are realistic, assertions are specific.

### 13.5 Smaller, scoped PRs

Even as a solo project, PRs are better than direct-to-main commits because they create a review surface for tomorrow-you. CI runs on PRs. A revert is a one-click action.

### 13.6 Reduce constants/mapping file sprawl

`select_drop_rename_cols_mappings.py` and friends are convenient but have grown. Split by domain (`mappings/attendance.py`, `mappings/payments.py`, ...). Smaller modules are AI-friendly: smaller context, fewer accidental edits.

---

## 14. Sustainability and bus factor

Solo civic-data projects rot when the maintainer steps back. These items stretch the half-life.

### 14.1 Tribal knowledge capture

The PDF parsers in particular embed knowledge about layout quirks ("payments PDFs since 2022 wrap the description column at 67 chars"). That knowledge currently lives in commit messages and the maintainer's head. Capture it in module docstrings and the methodology doc.

### 14.2 Refresh calendar doc

`doc/refresh_calendar.md`: when each source refreshes, what the typical lag is, who at the source to contact if it breaks. One row per source, one paragraph each.

### 14.3 First-contributor experience

Someone clones the repo cold. Can they:

- Run the test suite? (yes after §13.1)
- Run the dashboard? (yes if `data` branch is fetchable)
- Add a small feature? (yes if §13.2 exists and contracts are explained)
- Refresh data locally? (yes after §17 lands `--refresh`)

If any answer is "no", that's the next sustainability item.

### 14.4 Handover note

`doc/handover.md`: the document a new maintainer reads if you stopped tomorrow. Includes:

- secrets and access locations,
- the GitHub Actions schedule,
- known fragile parsers,
- open issues by priority,
- the "start here" sequence for the first day.

You will never read this; it exists for someone else.

### 14.5 Monthly rebuild from clean state

Once a month, blow away local data, clone fresh, run `make bootstrap` and `make refresh`. Anything that breaks is a sustainability bug. This is the early-warning system for "the project still works".

### 14.6 Funding / grants

A small civic-data project in Ireland has access to: Enterprise Ireland innovation vouchers, Open Knowledge Ireland community, certain academic micro-grants (TCD/UCD/DCU politics depts), and Journalism Funds (small). Not life-changing money, but enough to fund hosting + occasional contract help. Not urgent; flag for when the project is public.

---

## 15. Hosting and cost

Mostly unchanged from earlier revisions; included for completeness.

### 15.1 Recommended stack

1. **Streamlit Community Cloud** for the app. Free, GitHub-connected, auto-rebuild on push.
2. **GitHub Actions cron** for the refresh (§3).
3. **Hugging Face Datasets** as an optional secondary publication target if parquet sizes outgrow Community Cloud's comfort.

### 15.2 What not to do yet

- No custom React frontend.
- No backend API service unless a real user asks.
- No live joins in Streamlit.
- No continuous AI regeneration of metric logic.
- No heavyweight orchestrator (Dagster, Prefect) — GitHub Actions is plenty for this scale.

### 15.3 Cost model

At current scale, total monthly cost is plausibly £0–10. Mostly free tiers. The cost trap is "one paid dependency at a time" — keep the bar high.

---

## 16. AI-assisted development discipline

The contract pack and skills make AI-assisted development viable for this project. The discipline is what keeps it that way.

### 16.1 Skills as enforcement

The skills (`bold-redesign-page`, `civic-ui-review`, `pipeline-view`) are not optional — they're the contract enforcement layer. New pages should always go through them. Drift creeps in when shortcuts are taken under deadline; the skills are designed for "use even when in a hurry".

### 16.2 Contract pack as token discipline

An AI generating a page should read the page contract and the column dictionary, not the full repo. If a generation prompt includes more than ~30k tokens of context, the prompt is wrong, not the AI. The contract pack v5 is sized to keep prompts small.

### 16.3 What AI must not own

- Source joins.
- Fuzzy matching logic.
- Metric definitions.
- Provenance logic.
- Data-grain decisions.
- Anything in `pipeline.py` or `enrich.py` core.

These are change-controlled. AI can suggest, the maintainer commits.

### 16.4 Diff review discipline

Every AI-generated change is reviewed as a diff before merge. Even when the AI seems confident. Especially when the AI seems confident.

### 16.5 Memory hygiene

Save user/feedback memories that capture *why* a decision was made (not just *what*). When a memory becomes stale (pattern changed, file moved), update or remove it — don't just ignore it.

---

## 17. Recommended 90-day sequence

Concrete, in priority order. Each item is small enough to ship in a sitting.

### Weeks 1–2 — operational baseline

1. Add `pipeline.py --refresh` flag and the GitHub Actions workflow in §3.3.
2. Stand up the `data` branch and a Streamlit Community Cloud deploy from `main` + `data`.
3. Add the first per-mart manifest writer (`utility/tools/write_run_manifest.py`).
4. Add `render_provenance(manifest_path)` and wire it on three pages.
5. Add the freshness badge helper (§8.2) and wire it on the same three pages.

### Weeks 3–4 — protect what exists

6. Add CI: ruff + ruff-format + pytest + page-import smoke + schema diff.
7. Add one golden-file PDF test per parser (attendance, payments, interests).
8. Add row-count drift assertions in silver writers.
9. Add the lobbying.ie auto-export job — Playwright if DevTools XHR isn't workable. Best-effort, not blocking.
10. Pin dependencies and commit a lockfile.

### Weeks 5–6 — distribution and trust

11. Versioned data releases (§11.1).
12. `methodology.md` first draft (§12.1).
13. Per-page caveat banners where DATA_LIMITATIONS flags a gap (§12.3).
14. `CHANGELOG.md` started (§12.5).
15. RSS feed for new events (§11.4) — at least one feed.

### Weeks 7–8 — UI and ops polish

16. Cross-page navigation (§8.6).
17. Global search (§8.7).
18. Pipeline status page (§9.5).
19. Run summaries committed per refresh (§9.1).
20. Issue auto-creation on refresh failure (§9.3).

### Weeks 9–12 — first real user

21. Hand the alpha to one named journalist or researcher.
22. Whatever they ask for first becomes the next priority (likely SIPO donations from `ENRICHMENTS.md` §A.1, or judicial appointments from §D.1).
23. Iterate on whatever broke under their use.
24. Refresh calendar doc and handover note (§14.2, §14.4).

This sequence assumes evening-and-weekend pacing. None of it is research-grade work; it's all operationalising what's already designed.

---

## 18. Reading list

Streamlit:

- Streamlit Community Cloud — https://docs.streamlit.io/deploy/streamlit-community-cloud
- Multipage apps — https://docs.streamlit.io/develop/concepts/multipage-apps/overview
- Caching — https://docs.streamlit.io/develop/concepts/architecture/caching
- `st.cache_data` — https://docs.streamlit.io/develop/api-reference/caching-and-state/st.cache_data
- Query params — https://docs.streamlit.io/develop/api-reference/caching-and-state/st.query_params

DuckDB / Parquet:

- DuckDB Parquet overview — https://duckdb.org/docs/current/data/parquet/overview.html
- DuckDB Python API — https://duckdb.org/docs/current/clients/python/overview.html
- DuckDB metadata functions — https://duckdb.org/docs/current/sql/meta/duckdb_table_functions.html
- DuckDB-WASM — https://duckdb.org/docs/api/wasm/overview
- MotherDuck — *Why Semantic Layers Matter* — https://motherduck.com/blog/semantic-layer-duckdb-tutorial/

CI / testing:

- pytest getting started — https://docs.pytest.org/en/stable/getting-started.html
- GitHub Actions Python build/test — https://docs.github.com/en/actions/tutorials/build-and-test-code/python
- pandera schemas — https://pandera.readthedocs.io/

Schema / data quality:

- pandera — https://pandera.readthedocs.io/
- Great Expectations (heavier alternative) — https://greatexpectations.io/

Hosting / distribution:

- Streamlit Community Cloud — https://docs.streamlit.io/deploy/streamlit-community-cloud
- Hugging Face Spaces — https://huggingface.co/docs/hub/spaces-overview
- Hugging Face Datasets — https://huggingface.co/docs/hub/datasets-overview
- Keep a Changelog — https://keepachangelog.com/

Civic-data inspiration:

- TheyWorkForYou — https://github.com/mysociety/theyworkforyou
- HowTheyVote.eu — https://howtheyvote.eu/
- OpenKamer (Dutch parliamentary scraper) — https://github.com/openkamer/openkamer

For dataset enrichment ideas, see `ENRICHMENTS.md`.

---

## 19. What changed in this rev

vs v3:

- Renamed and rewritten from scratch as v4.
- Removed the dataset-enrichment catalogue entirely; that now lives in `ENRICHMENTS.md`. v4 stays focused on the existing surface.
- Reorganised into 18 discrete sections covering the full project surface — robustness, modelling, performance, UI, ops, security, distribution, trust, DX, sustainability — not just the original "operating model + page contracts" axis.
- Status snapshot updated to April 2026: contract pack v5, ~30 SQL views, 8 pages, skills system, sandbox pattern.
- §3 auto-refresh kept and refined with concrete YAML.
- §4–5 pipeline robustness expanded with schema validation, golden files, drift assertions, quarantine flow.
- §6 data modelling adds explicit dim/fact/bridge target, match-confidence as first-class, SCDs.
- §8 UI maturity is new — covers cross-page nav, search, mobile, accessibility, onboarding.
- §9 observability is new — run summaries, freshness SLOs, status page.
- §10 security and licensing is new — explicit ToS, GDPR-light, robots.txt.
- §11 distribution is new — versioned releases, permalinks, RSS, DuckDB-WASM.
- §12 trust expanded — methodology doc, caveat banners, public changelog.
- §13–14 DX and sustainability are new — bootstrap, contributor experience, handover, monthly clean-rebuild.
- §16 AI discipline made explicit — skills as enforcement, contract pack as token discipline, what AI must not own.
- §17 90-day sequence rewritten to be evening-pace realistic, not multi-quarter.

vs prior attempts at this kind of doc:

- Stops trying to be a textbook. The architectural philosophy is in §1; everything else is concrete improvements to the existing project.
- Separates data enrichment ideas (`ENRICHMENTS.md`) from project improvements (this doc). Each is more useful when not interleaved with the other.
- Acknowledges that the analytical layer is materially closer to beta than the operational layer is, and structures priorities accordingly.
