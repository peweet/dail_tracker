# Dáil Tracker — Architecture & Roadmap

**Last updated: 13 Apr 2026**

## Table of Contents

1. [What This Codebase Does](#what-this-codebase-does)
2. [Current Pipeline](#current-pipeline)
3. [Resolved Issues (Log)](#resolved-issues-log)
4. [Active Bugs & Issues](#active-bugs--issues)
5. [Lobbying Pipeline — Current State & Gaps](#lobbying-pipeline--current-state--gaps)
6. [Medallion Architecture Redesign](#medallion-architecture-redesign)
7. [Proposed Folder Structure](#proposed-folder-structure)
8. [Database Recommendation (DuckDB)](#database-recommendation-duckdb)
9. [Data Quality Testing (pytest)](#data-quality-testing-pytest)
10. [Additional Data Sources & Enrichment](#additional-data-sources--enrichment)
11. [Timeliness & Temporal Analysis](#timeliness--temporal-analysis)
12. [Dashboard & Frontend Options](#dashboard--frontend-options)
13. [Portfolio Enhancements](#portfolio-enhancements)
14. [Action Items Checklist (with time estimates)](#action-items-checklist-with-time-estimates)
15. [Experimental — Senior-Level Improvements](#experimental--senior-level-improvements)
16. [API Parameter Reference](#api-parameter-reference)
17. [Patterns From Comparable Civic-Tech Projects](#patterns-from-comparable-civic-tech-projects)

---

## What This Codebase Does

This project ingests open-source data published by the Oireachtas (Irish parliament) and other public bodies, collating it into a unified dataset for civic transparency purposes.

API link: https://api.oireachtas.ie/

**Data sources currently ingested:**

- **TD Attendance** — Extracted from PDF reports published on data.oireachtas.ie using PyMuPDF (`fitz`). Counts sitting days and other days per TD. Coverage: 7 PDFs spanning Jan 2023 → Feb 2026.
- **TD Members** — Fetched from the Oireachtas REST API (`/v1/members`), filtered by chamber (34th Dáil). Flattened to CSV with party, constituency, committee, and ministerial office data.
- **Bills/Legislation** — Fetched from the Oireachtas legislation API (`/v1/legislation`), per TD using concurrent requests.
- **Parliamentary Questions** — Fetched from `/v1/questions`, per TD using concurrent requests.
- **Parliamentary Standard Allowance Payments** — Extracted from monthly payment PDFs published on data.oireachtas.ie. Covers Jan 2024 → Feb 2026.
- **Register of Members' Interests** — Extracted from the annual declaration PDF. Captures occupations, shares, directorships, land, gifts, travel facilities, remunerated positions, and contracts per TD.
- **Lobbying Register** — Ingested from CSV exports from lobbying.ie. Includes lobbying organisation details, DPO (Designated Public Official) targets, lobbying activities, subject matter, and intended results.
- **Enriched Dataset** — Joins attendance, member metadata (party, constituency, committees), payments, and member interests via a sorted-character fuzzy name key (`normalise_join_key.py`).

Some notes on the data pipeline: data extracted from the PDF files is inherently brittle and may contain data gaps. The data is extracted from 2024-2026 periods and some TDs are simply missing from the PDFs for reasons that cannot be surmised. The sources are provided and can be tested in Swagger. You must do some of your own investigation prior to blindly trusting the data. Every best effort has been made to create a high quality piece of work however some gaps may be present.

---

## Current Pipeline

```
 INGESTION (Bronze)
 ──────────────────
 1. oireachtas_api_service.py       → Fetches all TDs by chamber        → members/members.json
                                    → Fetches legislation per TD        → bills/legislation_results.json
                                    → Fetches questions per TD          → bills/questions_results.json
 2. pdf_enpoint_check.py            → Health-checks all PDF/API URLs

 TRANSFORMATION (Silver)
 ───────────────────────
 3. flatten_members_json_to_csv.py  → Flattens members.json             → members/flattened_members.csv
 4. attendance.py                   → PyMuPDF extracts PDF tables        → members/aggregated_td_tables.csv
 5. payments.py                     → PyMuPDF extracts payment PDFs      → members/aggregated_payment_tables.csv
                                                                        → members/top_tds_by_payment_2024.csv
 6. member_interest.py              → PyMuPDF + regex extracts interests → members/member_interests_grouped.csv
 7. lobbying_processing.py          → Sanitises lobbying.ie CSV exports  → lobbyist/combined_lobbying_data.csv
                                    → Cleans lobby org registry          → lobbyist/cleaned_output.csv
                                    → Explodes DPO targets + activities  → lobbyist/lobby_break_down_by_politician.csv
                                                                        → lobbyist/most_lobbied_politicians.csv
                                                                        → lobbyist/lobby_count_details.csv

 ENRICHMENT (Silver → Gold)
 ──────────────────────────
 8. enrich.py                       → Joins attendance + members         → members/enriched_td_attendance.csv
 9. flatten_service.py              → Flattens bills JSON                → bills/drop_cols_flattened_bills.csv
```

### Key technical decisions

- **PyMuPDF (fitz)** replaced camelot for PDF extraction after benchmarking (16.9s vs 34.2s on 244 pages). PyMuPDF's `find_tables()` handles page-spanning records natively — no orphan fragment stitching needed.
- **JSON** replaced pickle for inter-script data transfer (security + portability).
- **pandas** used for JSON flattening (`flatten_json` + `pd.DataFrame`) in `flatten_service.py`, `flatten_members_json_to_csv.py`, and `attendance.py`. pandas handles deeply nested JSON → flat record conversion more naturally than polars, and given dataset sizes (~1200 rows), the overhead is negligible.
- **polars** used in `enrich.py`, `payments.py`, `member_interest.py`, `lobbying_processing.py`, and `oireachtas_api_service.py`. polars is faster for the tabular filtering/joining steps.
- **Sorted-character fuzzy join key** (`normalise_join_key.py`) — NFD-normalises, lowercases, strips accents/apostrophes/whitespace, then sorts characters alphabetically. This creates a join key that matches "Ó Súilleabháin" to "O'Sullivan" across datasets with different formatting conventions.
- **Logging** implemented in `oireachtas_api_service.py` using Python's `logging` module with both console and file output (`pipeline.log`).
- **Unified API service** — `member_api_request.py`, `bills_by_td.py`, and `question_api.py` consolidated into `oireachtas_api_service.py` with parameterised scenario handling (`legislation`, `questions`).
- **Column configuration centralised** in `utility/select_drop_rename_cols_mappings.py` — drop lists, rename dicts, and select lists for members, bills, and lobbying data. `utility/constants.py` holds PDF download links and reference API URLs.
- **`pyproject.toml`** added for dependency management. Core dependencies: polars, pandas, numpy, PyMuPDF, requests, flatten-json. Optional groups for OCR (`ocrmypdf`), database (`duckdb`), and development (`pytest`, `ruff`).
- **`if __name__ == "__main__"` guards** added to scripts so they can be imported as modules without triggering side effects. This enables the future entry-point system defined in `pyproject.toml`.

---

## Resolved Issues (Log)

All original bugs documented in the first version of this file have been fixed:

| File | Issue | Status |
|---|---|---|
| `bills_by_td.py` | `try/except/else` printed "Failed" on success | **Fixed** — `else` on `if`, not `try` |
| `bills_by_td.py` | `break` on JSONDecodeError killed entire loop | **Fixed** → `continue` |
| `bills_by_td.py` | `bill_source` had `%20` (double-encoded by requests) | **Fixed** → `"Private Member"` |
| `bills_by_td.py` | Redundant `f.close()` inside `with` | **Fixed** — removed |
| `member_api_request.py` | File mode `"a"` corrupted JSON on re-run | **Fixed** → `"w"` |
| `member_api_request.py` | `strip_head` iterated dict keys not results | **Fixed** → `.get("results", [])` |
| `member_api_request.py` | `date_start` baked into URL string | **Fixed** → moved to `params` dict |
| `question_api.py` | File mode `"r+"` when only `"r"` needed | **Fixed** → `"r"` |
| `question_api.py` | Variable named `questions` holding member data | **Fixed** → `members_data` |
| `question_api.py` | Pickle for inter-script data | **Fixed** → JSON |
| `attendance_2024.py` | Entire file rewritten — old camelot approach replaced | **Replaced** with PyMuPDF pipeline (now `attendance.py`) |
| Garbled CSVs | Orphan fragments from page-spanning records | **Eliminated** — PyMuPDF handles natively |
| Inflated totals | Name regex failed on Irish names → wrong TD accumulation | **Fixed** — structural marker detection |
| `main.py` | Prototype duplicating `attendance_2024.py` | **Retired** |
| `first_line.py` | Scratch script with duplicated functions | **Retired** |
| `enrich.py` | Join direction: PDF was driving table, TDs missing from PDF were lost | **Fixed** — API master list is now the left table |
| `enrich.py` | Typo `arge_df = large_df.unique(...)` — dead variable | **Fixed** → `large_df = large_df.unique(...)` |
| `bills_by_td.py` | `{uri}` interpolated full tuple into URL | **Fixed** → `{uri[0]}` |
| `bills_by_td.py` | `member_id` pre-encoded then double-encoded | **Fixed** → plain URL |
| `bills_by_td.py` | `if uri is not None` always True (tuples are never None) | **Fixed** → `if uri[0] is not None` |
| `bills_by_td.py` | `return URLS` inside `for` loop — returned after first TD | **Fixed** — dedented |
| `bills_by_td.py` | Migrated to `concurrent.futures.ThreadPoolExecutor` | **Done** |
| `.gitignore` | JSON files not excluded from repo | **Fixed** |
| Scripts | No `__main__` guards — code executed on import | **Fixed** — guards added |
| Dependencies | No `pyproject.toml` or dependency management | **Fixed** — `pyproject.toml` created with core + optional groups |

---

## Active Bugs & Issues

### 1. Missing TD — 127 of 128 detected in attendance

The attendance PDF contains 128 TDs but `aggregated_td_tables.csv` only captures 127. The join direction fix in `enrich.py` mitigates this for the enriched output — all TDs from the API master list now appear regardless of PDF parsing. However, the root cause in `attendance.py` PDF parsing still needs investigation.

**Estimate:** ~1 hour to add diagnostic logging per page and identify the missing TD.

### 2. TDs with 0 attendance dropped incorrectly

TDs with `0` in attendance fields are dropped by `dropna()` logic because `0` values are treated as empty. The join direction fix ensures these TDs appear with null attendance, but the parsing logic should preserve legitimate `0` values.

**Estimate:** ~30 minutes — change `dropna()` to only drop genuinely empty strings, not `0`.

### 3. `flatten_members_json_to_csv.py` — overly wide CSV and logic bug

The `flatten_json` library creates hundreds of columns (most "Null") from nested committee arrays. Additionally, the `os.path.exists()` cleanup has a logic bug where `'string' or os.path.exists(...)` short-circuits on the truthy string.

**Estimate:** ~2 hours to narrow the flattened schema and fix the cleanup logic.

### 4. `lobbying_processing.py` — `split_df` uses wrong DataFrame (line 86)

The variable `df` at line 86 refers to the **last CSV file** processed in the `for file in os.listdir()` loop (line 45-51), not the combined `lobbying_df`. This means the DPO explode only operates on one batch file's data rather than the full combined dataset.

**Fix:** Change `df` to `lobbying_df` at line 86.

**Estimate:** ~5 minutes to fix, ~30 minutes to verify counts match expectations.

### 5. `lobbying_processing.py` — double-join inflates counts (line 119)

```python
combined = segmented.join(total, on="full_name")
most_lobbied_politician = combined.join(total, on="full_name")  # joins total AGAIN
```

The `total_count` column is joined twice, producing `total_count` and `total_count_right`. This is noted in the existing comment `#big bug on counts (look at test.py)`.

**Estimate:** ~10 minutes — remove the redundant second join.

### 6. `lobbying_processing.py` — no DPO name cleaning or banned names filtering

The raw `dpo_lobbied` field contains garbage entries like "All TDs", "Members of Government", and corporate names. These are counted as real lobbying targets, inflating the "most lobbied politician" results. The `position` field (TD, Minister, Senator, etc.) is extracted but not used for filtering.

Comparison with robmcelhinney/lobbyieng shows they handle this with a `BANNED_NAMES` list, title-stripping (`Minister `, `Mr `, `Dr `, `Dep `), and canonical name mapping for known variants (e.g. "Aengus Ó Snodaigh" / "Aengus O Snoidigh").

**Estimate:** ~2 hours to implement banned names list, title stripping, and position filtering. See `dpo_integration_snippet.py` for a proposed implementation.

### 7. `endpoint_checker` returns inside loop (pdf_enpoint_check.py, line 60)

The `return False` at line 60 is inside the `for` loop, so the function only ever checks the first URL. All remaining URLs are never tested.

**Estimate:** ~10 minutes — dedent the return statement.

### 8. Hardcoded Windows paths throughout codebase

`C:\\Users\\pglyn\\PycharmProjects\\dail_extractor\\` appears in `oireachtas_api_service.py`, `lobbying_processing.py`, `member_interest.py`, `payments.py`, and `attendance.py`. These prevent the project from running on any other machine or OS.

`doc/config.py` already defines relative paths via `pathlib.Path` but is not yet imported by any consuming script.

**Estimate:** ~2 hours to replace all hardcoded paths with imports from `config.py` and test on a clean checkout.

### 9. `oireachtas_api_service.py` — member_api_request() called twice

`member_api_request()` is called at line 48 (during module setup) and again at line 104 (before the pipeline loop). The first call is now guarded by `__main__`, but this should be verified.

**Estimate:** ~10 minutes.

### 10. Unicode encoding failures in API URLs

4 TDs whose `member_id` URIs contain fada characters (Ó, é, á, etc.) fail when interpolated into `urllib.request` URLs. Failures are logged but silently dropped from results — no legislation or questions data collected for these TDs.

**Fix:** Switch `urllib.request.urlopen` to `requests.get` (which handles encoding), or percent-encode fada characters with `urllib.parse.quote()`.

**Estimate:** ~30 minutes.

---

## Lobbying Pipeline — Current State & Gaps

The lobbying pipeline (`lobbying_processing.py`) is the most unique aspect of this project. No other Irish civic-tech project systematically joins lobbying register data with member interests and parliamentary activity. Here is the current state and what's needed to unlock its full potential.

### What's working

- CSV export ingestion from lobbying.ie (manual download in batches of 1000)
- Lobby organisation registry cleaned and linked via constructed URLs
- DPO field exploded into individual `full_name | position | chamber` records
- Lobbying activities exploded into `action | delivery | members_targeted` records
- Most-lobbied-politician counts (with caveats — see bugs #4, #5, #6)
- Most-prolific-lobbyist counts with org details (CRO number, website, activities)

### What's missing (compared with lobbyieng)

| Feature | lobbyieng | dail_tracker | Effort to add |
|---|---|---|---|
| Banned names / garbage DPO filtering | Yes — explicit list + title stripping | No | ~2 hours |
| Canonical name mapping for known variants | Yes — dict of variants per person | No (relies on fuzzy key) | ~1 hour |
| Relational DB with foreign keys (record → DPO + activities) | Yes — SQLAlchemy/SQLite | No — flat CSVs | ~4 hours |
| Period-over-period "biggest movers" | Yes — SQL delta query | No | ~1 hour |
| Degree centrality (network analysis) | Yes — SQL degree count | No | ~30 minutes |
| Shared lobbyists between official pairs | Yes — SQL self-join | No | ~30 minutes |
| Keyword extraction with stemming | Yes — custom tokeniser + stopwords | No | ~2 hours |
| Lobbying method breakdown (email, meeting, phone) | Yes — filterable in UI | Extracted but not persisted separately | ~30 minutes |
| Top policy areas per period | Yes — SQL group-by | No | ~30 minutes |
| Web frontend with search, filtering, pagination | Yes — Next.js + SQLite | No | ~20+ hours |
| Chord diagram comparing two officials | Yes — D3.js | No | ~8 hours |
| Force-directed network graph | Yes — react-force-graph-2d | No | ~8 hours |

### What dail_tracker has that lobbyieng doesn't

lobbyieng is **lobbying-only**. It has zero data on attendance, payments, member interests, parliamentary questions, legislation, committee memberships, or party/constituency info. The cross-referencing potential is the differentiator:

- **Conflict-of-interest detection** — match lobbying org `main_activities_of_organisation` against TD declared interests (shares, directorships, occupations). Flag cases where a TD was lobbied by an organisation in the same sector as their financial interests.
- **Influence chain tracing** — lobby → parliamentary question → bill → vote. Did a TD ask questions or sponsor legislation on the same topic they were lobbied on?
- **Payment context** — do TDs who receive higher parliamentary allowances also get lobbied more? Is there a correlation between ministerial office and lobbying volume?
- **Attendance vs lobbying** — are the most-lobbied TDs also the most-absent?

---

## Medallion Architecture Redesign

The [medallion architecture](https://www.databricks.com/glossary/medallion-architecture) organises data into three layers:

```
┌─────────────────────────────────────────────────────────────────────┐
│                        BRONZE (Raw)                                  │
│  Untouched data exactly as received from source                      │
│  • Raw PDF files (attendance, payments, member interests)            │
│  • Raw API JSON responses (members, legislation, questions)          │
│  • Raw CSV exports from lobbying.ie                                  │
│  • PyMuPDF extracted tables (no transformations)                     │
├─────────────────────────────────────────────────────────────────────┤
│                        SILVER (Cleaned)                              │
│  Validated, deduplicated, schema-enforced data                       │
│  • Names parsed and normalised via join_key                          │
│  • Columns renamed with meaningful names                             │
│  • Data types enforced (dates as DATE, counts as INT)                │
│  • Members flattened to a clean narrow schema                        │
│  • Lobbying DPOs split into normalised records                       │
│  • Member interests categorised and cleaned                          │
├─────────────────────────────────────────────────────────────────────┤
│                        GOLD (Analytical)                             │
│  Business-ready tables, joined and aggregated                        │
│  • td_profile: party, constituency, committees, ministerial office   │
│  • td_attendance: one row per TD with attendance counts              │
│  • td_payments: payment amounts by TD and period                     │
│  • td_interests: declared interests per TD per category              │
│  • td_lobbying: lobbying contacts per TD with org details            │
│  • td_bills: one row per bill with sponsoring TD                     │
│  • td_questions: one row per question with TD and topic              │
│  • td_enriched: master table joining all of the above                │
│  SQL views / materialised tables for dashboards                      │
└─────────────────────────────────────────────────────────────────────┘
```

**Estimate:** ~8 hours for the full directory restructure, script refactoring, and DuckDB loading.

---

## Proposed Folder Structure

```
dail_tracker/
├── pyproject.toml               # Dependencies, entry points, tool config
├── doc/
│   ├── README.MD
│   ├── REARCHITECTURE.md        # This file
│   ├── DATA_LIMITATIONS.md      # Known data quality issues
│   └── config.py                # Centralised paths, URLs, constants
│
├── oireachtas_api_service.py    # API ingestion (members, legislation, questions)
├── attendance.py                # PDF → attendance CSV
├── payments.py                  # PDF → payment CSV
├── member_interest.py           # PDF → member interests CSV
├── lobbying_processing.py       # lobbying.ie CSV → cleaned lobbying data
├── flatten_members_json_to_csv.py
├── flatten_service.py           # Bills JSON → flat CSV
├── enrich.py                    # Joins attendance + members
├── normalise_join_key.py        # Fuzzy name normalisation
├── pdf_enpoint_check.py         # URL health checker
├── tear_down.py                 # Cleanup intermediary files
├── read_scan_pdf.py             # OCR for scanned PDFs
│
├── utility/
│   ├── constants.py             # PDF URLs, API URLs, reference links
│   └── select_drop_rename_cols_mappings.py
│
├── test/
│   └── test.py                  # Placeholder — to be expanded
│
├── lobbyist/
│   └── test.py
│
├── members/                     # Output CSVs (gitignored — regenerated by pipeline)
├── bills/                       # Output CSVs
└── lobbyist/raw/                # Raw CSV exports from lobbying.ie
```

**Future (medallion) structure** when ready to restructure:

```
dail_tracker/
├── bronze/                      # Raw data — .gitignored, re-creatable
│   ├── pdf/                     # Downloaded PDFs by category
│   ├── api/                     # Raw JSON from API
│   └── lobbying/                # Raw CSV exports
├── silver/                      # Cleaned data — .gitignored
│   ├── attendance/
│   ├── members/
│   ├── payments/
│   ├── interests/
│   ├── lobbying/
│   └── bills/
├── gold/
│   ├── dail.duckdb              # Analytical database
│   └── metadata.json            # Pipeline run metadata
└── notebooks/                   # Exploratory analysis
```

---

## Database Recommendation (DuckDB)

| Feature | SQLite | DuckDB |
|---|---|---|
| Setup | Zero config, built into Python | `pip install duckdb`, zero config |
| Best for | Transactional / CRUD apps | Analytical queries (GROUP BY, JOIN, aggregations) |
| Direct CSV/Parquet read | No (must load first) | Yes (`read_csv_auto()`, `read_parquet()`) |
| Polars integration | Manual | Native (`pl.read_database()`) |
| SQL dialect | Standard SQL | PostgreSQL-compatible SQL |

**Recommendation: DuckDB** for the gold analytical layer. Can use both: DuckDB for analytics, SQLite if a web frontend is built later (as lobbyieng does).

**Estimate:** ~3 hours to set up DuckDB, load all silver CSVs, and create initial analytical views.

---

## Data Quality Testing (pytest)

### Recommended structure

```
test/
├── conftest.py              # Shared fixtures (paths, sample data)
├── test_attendance.py       # TD count, max totals, Irish name coverage
├── test_payments.py         # Payment amounts valid, no missing TDs
├── test_member_interest.py  # All 9 interest categories present
├── test_lobbying.py         # DPO counts, no banned names, position filtering
├── test_enrich.py           # Join completeness, no duplicates
└── test_normalise.py        # Fuzzy key produces expected matches
```

**Estimate:** ~4 hours for initial test suite covering the most critical assertions. Expand incrementally.

---

## Additional Data Sources & Enrichment

### 1. Parliamentary Questions (`/v1/questions`) — ✅ IN PIPELINE, needs refinement

Questions are now fetched via `oireachtas_api_service.py`. Remaining: add `date_start` to scope to 34th Dáil (reduces payload by 60-80% for veteran TDs), implement pagination loop for TDs with >1000 questions.

**Estimate:** ~1 hour.

### 2. Divisions / Votes (`/v1/divisions`) — **HIGHEST VALUE ENRICHMENT**

Each division record contains individual TD votes (Tá/Níl/Staon). This enables party loyalty scores, rebellion detection, absenteeism-on-votes analysis, and cross-party alliance mapping. Join via `member_uri → unique_member_code`.

**Estimate:** ~4 hours (mirrors existing concurrent fetch pattern + new flattening logic).

### 3. Debates / Speeches (`/v1/debates`)

Contains individual speeches per TD per debate. Enables speaking frequency, word count, and topic analysis per TD.

**Estimate:** ~4 hours.

### 4. CRO (Companies Registration Office) Enrichment

The `company_registration_number` in the lobby org data can be looked up against the CRO API (`services.cro.ie`) to pull company directors, shareholders, and financial filings. This traces ownership connections between lobbyists and TD financial interests.

**Estimate:** ~6 hours (API integration, rate limiting, matching logic).

### 5. Constituency Demographics (CSO)

CSO PxStat API for constituency-level population, unemployment, and age data. Answers: "Do TDs from high-unemployment constituencies attend more?"

**Estimate:** ~3 hours.

### 6. Historical Attendance PDFs (year-over-year)

2022 and 2023 PDFs enable trend analysis. The pipeline already handles multiple PDFs via glob — just drop them in the directory.

**Estimate:** ~1 hour (download PDFs + verify parsing).

### 7. Conflict-of-Interest Detection (lobbying × member interests)

Match `main_activities_of_organisation` from lobby org data against `interest_description_cleaned` from member interests using keyword overlap or embeddings. Flag cases where a TD holds financial interests aligned with their lobbyists.

**Estimate:** ~6 hours for keyword-based matching, ~12 hours if using embeddings.

### 8. Seanad Expansion

Same APIs and lobbying data cover Senators. Parameterise `chamber_id` to include both houses. Roughly doubles the dataset with minimal code changes.

**Estimate:** ~3 hours.

---

## Timeliness & Temporal Analysis

The attendance dates in `aggregated_td_tables.csv` are individual `dd/mm/yyyy` values — they can drive rich time-series analysis:

- **Attendance gaps** — days between consecutive attendances per TD
- **Weekly/monthly patterns** — do some TDs attend mainly on certain days?
- **Session coverage** — what % of sitting days did each TD attend?
- **Change-over-time** — year-over-year attendance trends per TD
- **Temporal joins** — attendance dates vs division dates vs debate dates vs lobbying return periods

For lobbying specifically, the `lobbying_period` field enables the "biggest movers" analysis — which TDs saw the biggest spike or drop in lobbying contacts between consecutive reporting periods.

**Estimate:** ~4 hours for core temporal analysis (gap analysis, monthly patterns, biggest movers).

---

## Dashboard & Frontend Options

### Recommended path

```
Start here                For more learning              For production
───────────              ──────────────────              ──────────────
Streamlit          →     FastAPI + HTMX + Chart.js  →   FastAPI + React/Vue
(~4 hours)               (~12 hours)                    (~40+ hours)
```

**Streamlit** gets something visual up fast. A single `app.py` with DuckDB queries, searchable TD table, attendance-by-party bar chart, and lobbying-contacts-per-TD ranking.

**FastAPI + HTMX** for a proper web app with paginated API endpoints, filtering, and server-rendered HTML. Similar architecture to lobbyieng but using Python instead of Next.js.

---

## Portfolio Enhancements

- **Data lineage / metadata table** — track source, timestamp, row count, and checksum per pipeline stage (~1 hour)
- **Parquet outputs** alongside CSV — columnar, compressed, type-preserving, DuckDB-native (~30 minutes — 1 line per output)
- **Entity resolution** — `rapidfuzz` for cross-source name matching as an alternative/complement to sorted-character key (~2 hours)
- **GitHub Actions CI/CD** — automated weekly pipeline run + pytest (~2 hours)
- **Polished README with badges** and architecture diagram (~2 hours)
- **Data quality HTML report** — automated output showing coverage, missing data %, schema validation (~3 hours)
- **`DATA_LIMITATIONS.md`** — already exists, keep updated with new findings (~ongoing)

---

## Action Items Checklist (with time estimates)

Estimates are rough — actual time will vary. "Effort" is implementation time assuming familiarity with the codebase.

### Critical Fixes (do these first)

| Item | Effort | Notes |
|---|---|---|
| Fix `lobbying_processing.py` `df` → `lobbying_df` at line 86 | 5 min | Using wrong DataFrame for DPO explode |
| Fix double-join on `most_lobbied_politician` (line 119) | 10 min | Inflates count columns |
| Fix `endpoint_checker` return inside loop | 10 min | Only checks first URL |
| Fix Unicode encoding in API URLs (4 TDs) | 30 min | Switch urllib to requests or quote fadas |
| Replace hardcoded Windows paths with `config.py` imports | 2 hours | Blocks portability |

### Lobbying Pipeline (your differentiator)

| Item | Effort | Notes |
|---|---|---|
| Add banned DPO names list + position filtering | 2 hours | See `dpo_integration_snippet.py` |
| Join lobbying DPO data onto `enriched_td_attendance.csv` | 1 hour | Via normalised join_key |
| Period-over-period "biggest movers" analysis | 1 hour | Compare consecutive lobbying periods |
| Shared lobbyists between TD pairs (network analysis) | 30 min | Self-join on lobbyist_name |
| Degree centrality per TD | 30 min | Count distinct lobbyists per TD |
| Lobbying method breakdown (email, meeting, phone) | 30 min | Already extracted, just persist |
| Keyword extraction from subject_matter / intended_results | 2 hours | Stemming + stopwords + frequency |
| Conflict-of-interest detection (lobbying × member interests) | 6 hours | Keyword matching on org activities vs declared interests |

### Data Enrichment

| Item | Effort | Notes |
|---|---|---|
| Add `date_start` to questions URL + pagination loop | 1 hour | Prevents silent truncation |
| Add `bill_status` filter to legislation URL | 10 min | Reduces payload server-side |
| Add divisions (votes) endpoint | 4 hours | Highest-value new data source |
| Add debates (speeches) endpoint | 4 hours | Speaking frequency + topic analysis |
| CRO company lookup for lobby orgs | 6 hours | Traces lobbyist ownership |
| CSO constituency demographics | 3 hours | Cross-reference with attendance |
| Historical attendance PDFs (2022, 2023) | 1 hour | Year-over-year trends |
| Seanad expansion | 3 hours | Parameterise chamber_id |

### Architecture & Infrastructure

| Item | Effort | Notes |
|---|---|---|
| Wire `config.py` into all scripts | 2 hours | Already defined, not yet imported |
| Create `pipeline.py` orchestrator with subcommands | 3 hours | `--demo` flag for quick testing |
| Medallion directory restructure (bronze/silver/gold) | 4 hours | Rename + move + update imports |
| DuckDB gold layer setup | 3 hours | Load CSVs, create views |
| Parquet outputs alongside CSV | 30 min | 1 line per output file |
| `Makefile` with install, run-all, test targets | 1 hour | Developer ergonomics |

### Testing

| Item | Effort | Notes |
|---|---|---|
| `test_attendance.py` — TD count, max totals, Irish names | 1 hour | Most critical assertions |
| `test_lobbying.py` — DPO counts, banned names, positions | 1 hour | Validates lobbying pipeline |
| `test_normalise.py` — fuzzy key edge cases | 1 hour | Fadas, apostrophes, hyphens |
| `test_enrich.py` — join completeness, no duplicates | 1 hour | Validates enrichment |
| `conftest.py` with shared fixtures | 30 min | Paths, sample DataFrames |

### Dashboard

| Item | Effort | Notes |
|---|---|---|
| Streamlit MVP with TD search + attendance charts | 4 hours | Fastest path to visual output |
| Lobbying explorer page (searchable, filterable) | 4 hours | Per-TD lobbying breakdown |
| Conflict-of-interest flagging in UI | 4 hours | Requires interest × lobbying join first |

### Portfolio Polish

| Item | Effort | Notes |
|---|---|---|
| Polished README with badges and architecture diagram | 2 hours | First impression for visitors |
| `pipeline_metadata` table for data lineage | 1 hour | Source, timestamp, row count per stage |
| GitHub Actions CI/CD (weekly pipeline + pytest) | 2 hours | Automated quality checks |
| `gold/metadata.json` after each pipeline run | 30 min | Version, counts, failures |
| Wikidata SPARQL for TD photos/social links | 2 hours | Nice-to-have for dashboard |

---

## Experimental — Senior-Level Improvements

These are the gaps between the current codebase and what a senior data engineer would produce. Not ordered by priority — pick what interests you.

### 1. Abstraction layer — reusable API client

The same "build URL, call API, parse JSON, handle errors" pattern is repeated. A single `OireachtasClient` class with retry logic, session reuse, and consistent error handling would collapse this into composable methods.

**Estimate:** ~3 hours.

### 2. Schema contracts (Pandera)

No validation that the output of one stage matches the input expectations of the next. A column rename in `attendance.py` silently produces nulls in `enrich.py`. Pandera schemas catch this at the point of failure.

**Estimate:** ~3 hours for initial schemas on the critical pipeline stages.

### 3. Idempotency and incremental processing

Every run re-fetches everything. A manifest table tracking `{member_code, endpoint, last_fetched, etag}` would enable incremental refresh — only fetch what's changed.

**Estimate:** ~4 hours.

### 4. Type hints and dataclasses

Functions accept and return untyped dicts and DataFrames. Type hints + dataclasses enable IDE autocompletion and `mypy` static analysis.

**Estimate:** ~4 hours for core pipeline functions.

### 5. Rate limiting and backoff

The `ThreadPoolExecutor` fires 5 concurrent requests with no throttle. Exponential backoff on failures (`requests.adapters.HTTPAdapter` with `Retry`) prevents 429s.

**Estimate:** ~1 hour.

### 6. Star schema for DuckDB

Restructure into dimension tables (`dim_td`, `dim_committee`, `dim_date`) and fact tables (`fact_attendance`, `fact_bill`, `fact_vote`, `fact_lobbying`). This is the Kimball modelling pattern used in data warehouses and commonly asked about in interviews.

**Estimate:** ~4 hours.

### 7. SCD Type 2 for party changes

TDs change party (e.g., Brian Stanley left Sinn Féin). The API already has party date ranges in the member JSON — preserving these enables slowly changing dimension tracking.

**Estimate:** ~2 hours.

---

## API Parameter Reference

See the original REARCHITECTURE.md §16 for the full parameter inventory. Key quick-win changes:

| Priority | Change | Impact | Effort |
|---|---|---|---|
| **High** | Add `date_start` to questions URL (scope to 34th Dáil) | 60-80% payload reduction for veteran TDs | 1 line |
| **High** | Add `bill_status` filter to legislation URL | Eliminates irrelevant bills server-side | 1 line |
| **High** | Implement `skip`-based pagination loop | Prevents silent data truncation | ~10 lines |
| **Medium** | Add `divisions` scenario to API service | Unlocks voting records | Mirrors existing pattern |
| **Medium** | Add `debates` scenario | Unlocks speech data | Mirrors existing pattern |
| **Medium** | Fetch `/v1/houses` for dynamic Dáil term dates | Eliminates hardcoded dates | Single API call |
| **Low** | Fetch `/v1/constituencies` as reference table | Cleaner constituency dimension | Single API call |
| **Low** | Fetch `/v1/parties` as reference table | Cleaner party dimension | Single API call |

---

## Patterns From Comparable Civic-Tech Projects

Key architectural lessons from comparable projects (full analysis in previous version of this document):

| Project | Country | Key lesson for dail_tracker |
|---|---|---|
| **robmcelhinney/lobbyieng** | Ireland | Relational DB for lobbying data, precomputed insights, web frontend, data-limitations page |
| **openkamer** | Netherlands | CLI subcommands with `--demo` flag, JSON dumps for reproducibility, Wikidata enrichment |
| **unitedstates/congress** | USA | Strict cache/output separation, per-entity directory structure, dual output formats |
| **mysociety/theyworkforyou** | UK | Separate parser repo from frontend, explicit data-limitations documentation |
| **okfn-brasil/serenata-de-amor** | Brazil | Three-component architecture, research notebooks alongside production code, versioned datasets |

**Highest-impact actions inspired by these projects:**

| Action | Effort | Priority |
|---|---|---|
| Create `pipeline.py` with subcommands + `--demo` flag | 3 hours | **High** |
| Separate bronze (gitignored cache) from gold (outputs) | 4 hours | **High** |
| Add `--force` and `--since` flags for incremental runs | 1 hour | **Medium** |
| Create `gold/metadata.json` after each pipeline run | 30 min | **Medium** |
| Add `Makefile` with install, run-all, test targets | 1 hour | **Medium** |
| Output Parquet alongside CSV | 30 min | **Low** |
| Add `notebooks/` directory for exploration | 30 min | **Low** |
| Precompute expensive aggregations as materialised DuckDB tables | 2 hours | **Low** (after DuckDB setup) |
