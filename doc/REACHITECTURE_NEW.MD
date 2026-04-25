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
18. [Next Major Data Expansion: Votes, Debates, or Bills?](#next-major-data-expansion-votes-debates-or-bills)

---
ideas:


#### Combination joins — the real power

The strongest analyses come from joining 3+ tables:

| Join | Question answered |
|---|---|
| `td + bills + divisions` | Did the sponsoring TD's party vote for their own bill? |
| `td + attendance + divisions` | If a TD was "present" (PDF), did they actually vote that day? |
| `td + questions + debates` | Does a TD who asks questions on housing also speak in housing debates? |
| `td + bills + questions` | Do TDs ask questions about bills they didn't sponsor? (scrutiny metric) |
| `td + committees + bills` | Do committee members sponsor bills related to their committee's remit? |


### The code has no abstraction layer

The same pattern — "build params, call API, check status, parse JSON, append to list" — is copy-pasted across `member_api_request.py`, `question_api.py`, and `bills_by_td.py` with slight variations. A senior engineer writes one reusable API client:

```python
class OireachtasClient:
    BASE_URL = "https://api.oireachtas.ie/v1"

    def __init__(self, max_retries=3, backoff_factor=0.5):
        self.session = requests.Session()
        self.session.mount("https://", HTTPAdapter(
            max_retries=Retry(total=max_retries, backoff_factor=backoff_factor)
        ))

    def get(self, endpoint: str, params: dict) -> dict:
        response = self.session.get(f"{self.BASE_URL}/{endpoint}", params=params)
        response.raise_for_status()
        return response.json()

    def get_members(self, party: str) -> list[dict]: ...
    def get_bills(self, member_uri: str) -> list[dict]: ...
    def get_questions(self, member_uri: str) -> list[dict]: ...
```

Three scripts collapse to one class. Every endpoint gets retry logic, session reuse, and consistent error handling for free. This is the single biggest gap — senior code is **composable**, not copy-pasted.

### No schema contracts between pipeline stages

If `attendance_2024.py` changes a column name from `sitting_days_attendance` to `sitting_attendance`, `enrich.py` silently produces nulls. Nothing validates that the output of one stage matches the input expectations of the next.

Senior approach — define explicit schemas using **Pandera**:

```python
import pandera as pa

attendance_schema = pa.DataFrameSchema({
    "first_name": pa.Column(str, nullable=False),
    "last_name": pa.Column(str, nullable=False),
    "sitting_days_attendance": pa.Column(str, nullable=True),
    "other_days_attendance": pa.Column(str, nullable=True),
})

attendance_schema.validate(df)  # raises if schema violated
```

This is commonly called a **data contract**. It catches breaking changes at the point they happen, not three stages downstream.

### No idempotency or incremental processing

Every run re-fetches all 175 TDs, re-parses the entire PDF, re-flattens all JSON. If half the API calls fail, you start from scratch. Senior pipelines:

- **Track what's been fetched** — e.g. a manifest table: `{member_code, endpoint, last_fetched, etag}`
- **Only fetch what's changed** — use HTTP `If-Modified-Since` or compare checksums
- **Are idempotent** — running the same stage twice produces the same output without duplication
- **Support partial recovery** — if 100/175 API calls succeed, the next run only retries the 75 failures



### No type hints or data classes

Every function accepts and returns untyped dicts, DataFrames, and bare strings. Senior code uses types as documentation:

```python
from dataclasses import dataclass

@dataclass
class TDRecord:
    first_name: str
    last_name: str
    member_code: str
    party: str
    constituency: str

def fetch_members(party: str) -> list[TDRecord]: ...
```

Self-documenting, enables IDE autocompletion, and catches bugs with `mypy` before runtime.

### No configuration management

Party codes, date ranges, file paths, API URLs, and PDF URLs are scattered as literals across files. `C:\\Users\\pglyn\\...` appears in multiple places. Senior approach:

```python
# config.py
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
API_BASE = "https://api.oireachtas.ie/v1"
PARTY_CODES = ["Social_Democrats", "Sinn_Féin", ...]
DATE_RANGE = ("2024-01-01", "2099-01-01")
```

Or for environment-specific overrides, use **pydantic-settings** or `python-dotenv`.

### No dependency pinning or reproducible environment

No `requirements.txt`, `pyproject.toml`, or lock file. Someone cloning the repo has to guess which packages to install:

```toml
# pyproject.toml
[project]
dependencies = [
    "polars>=1.0",
    "PyMuPDF>=1.24",
    "requests>=2.31",
    "flatten-json>=0.1.14",
]
```

Or a `Dockerfile` for full reproducibility.

### No rate limiting or backoff on API calls

The `ThreadPoolExecutor` fires 5 concurrent requests with no throttle. If the Oireachtas API has rate limits (most do), you'll get 429s or IP bans. Senior code adds:

- **Exponential backoff** on failures (the `Retry` adapter in tip #1)
- **Rate limiting** — e.g. a semaphore limiting concurrent calls
- **Circuit breaker** — after N consecutive failures, stop hitting the API entirely

### The pipeline has no orchestration

Scripts must run in a specific order and nothing enforces this. Out-of-order execution causes silent failures or stale data.

Options from simple to heavy:
- **Simple:** A `Makefile` or `pipeline.py` with explicit stage dependencies (already proposed in the medallion section)
- **Medium:** `Prefect` or `Dagster` — Python-native orchestrators with DAGs, retries, and a UI
- **Heavy:** `Airflow` — industry standard but overkill here

Even a basic dependency check helps:

```python
if not Path("members/flattened_members.csv").exists():
    raise FileNotFoundError("Run flatten_members_json_to_csv.py first")
```

### No containerisation

Hardcoded Windows paths mean this can't run on another machine. A `Dockerfile` makes it portable:

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml .
RUN pip install .
COPY . .
CMD ["python", "pipeline.py"]
```




### CI/CD with GitHub Actions

Automate the pipeline on a schedule:

```yaml
# .github/workflows/pipeline.yml
name: Dáil Data Pipeline
on:
  schedule:
    - cron: '0 6 * * 1'  # Every Monday at 6am
  workflow_dispatch:       # Manual trigger

jobs:
  run-pipeline:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - run: pip install -r requirements.txt
      - run: python pipeline.py
      - run: pytest tests/
```

###**Recess-adjusted rate** — Only count days when the Dáil was actually sitting

### Data Versioning

Store timestamped snapshots of bronze data so you can track changes:

```
bronze/api/members/2024-01-15_members.json
bronze/api/members/2024-03-30_members.json
```


### Temporal Joins

Join attendance dates against:

- **Division dates** — Was the TD present for key votes?
- **Debate dates** — Was the TD present when their topic was discussed?
- **Bill progression dates** — Was the sponsoring TD present when their bill was debated?

### Change-Over-Time Metrics

If you extract multiple years (2022–2025):

- **Trend lines** — Is each TD attending more or less over time?
- **New TD vs veteran** — Do first-term TDs attend more?
- **Post-election effect** — Does attendance change after elections?


This project ingests open-source data published by the Oireachtas (Irish parliament) and other public bodies, collating it into a unified dataset for civic transparency purposes.


**Read:** [Method Chaining in Pandas — Tom Augspurger](https://tomaugspurger.net/posts/method-chaining/)

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
### Scalability & Streamlit Hosting

- **Streamlit is single-process, per-session:** Each user session runs in its own Python process/thread. For public hosting (Streamlit Cloud, Streamlit Community Cloud), there are limits on concurrent sessions (typically 1–5 for free/community tiers, higher for paid/enterprise).
- **Concurrency:** Streamlit can handle a handful of concurrent users, but is not designed for high-traffic, multi-user analytics. Each session loads its own copy of the data, so memory usage scales with user count.
- **Biggest bottlenecks:**
  - **Data loading:** Reading large CSVs on every session start is slow and memory-intensive. Parquet mitigates this by being much faster and smaller.
  - **PDF extraction:** If any PDF parsing is triggered on-demand, it will be slow and block the UI. All extraction should be precomputed.
  - **Complex joins/groupbys:** Large groupby/aggregation operations in Pandas/Polars can be slow for 100k+ rows. Use DuckDB for heavy analytics.
  - **Session state:** Streamlit's session state is per-user and not shared. Caching (`@st.cache_data`) helps, but cache size is limited on hosted platforms.
- **Mitigations:**
  - Use Parquet for all dashboard data loads.
  - Precompute all analytics in the pipeline; serve only ready-to-query tables/views.
  - Use DuckDB for any on-demand queries that can't be precomputed.
  - For higher concurrency, consider deploying on a scalable backend (e.g., Streamlit on Kubernetes, or migrate to a FastAPI backend with a React frontend for true multi-user scale).

### Summary Table
| Bottleneck         | Impact                | Mitigation                |
|--------------------|-----------------------|---------------------------|
| CSV data loading   | Slow, high memory     | Use Parquet, cache loads  |
| PDF extraction     | Very slow, blocks UI  | Precompute, never on-demand |
| Groupby/joins      | Slow for large data   | Use DuckDB, pre-aggregate |
| Streamlit session  | Per-user, not shared  | Use @st.cache_data, limit session data |
| Hosting limits     | Few concurrent users  | Paid Streamlit, or migrate backend |

**Bottom line:** For civic-scale usage (dozens of users), Streamlit + Parquet + DuckDB is sufficient. For media-scale (hundreds+), migrate to a backend API and static frontend.

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


### 4. CRO (Companies Registration Office) Enrichment

The `company_registration_number` in the lobby org data can be looked up against the CRO API (`services.cro.ie`) to pull company directors, shareholders, and financial filings. This traces ownership connections between lobbyists and TD financial interests.

**Estimate:** ~6 hours (API integration, rate limiting, matching logic).

### 5. Constituency Demographics (CSO)

CSO PxStat API for constituency-level population, unemployment, and age data. Answers: "Do TDs from high-unemployment constituencies attend more?"

**Estimate:** ~3 hours.

### 7. Conflict-of-Interest Detection (lobbying × member interests)

Match `main_activities_of_organisation` from lobby org data against `interest_description_cleaned` from member interests using keyword overlap or embeddings. Flag cases where a TD holds financial interests aligned with their lobbyists.

**Estimate:** ~6 hours for keyword-based matching, ~12 hours if using embeddings.

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


---

##Taken from other files consolidation ideas

### Star schema for DuckDB

Restructure into dimension tables (`dim_td`, `dim_committee`, `dim_date`) and fact tables (`fact_attendance`, `fact_bill`, `fact_vote`, `fact_lobbying`). This is the Kimball modelling pattern used in data warehouses and commonly asked about in interviews.

**Estimate:** ~4 hours.

### SCD Type 2 for party changes

TDs change party (e.g., Brian Stanley left Sinn Féin). The API already has party date ranges in the member JSON — preserving these enables slowly changing dimension tracking.

**Estimate:** ~2 hours.

**Read:** [Refactoring Guru — Design Patterns](https://refactoring.guru/design-patterns)
The Strategy and Template Method entries have concrete Python examples.

The Template Method one is worth considering if you refactor `_politician_profile` and

| **Template Method** | Each profile view (politician / lobbyist) follows the same skeleton: select entity → filter → show stats → show table → export |

`_lobbyist_profile` — they share a structure (search box → selectbox → stats strip → filtered
table → export) but differ in which entity and columns they target.



## API Parameter Reference

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




```python
# run_pipeline.py
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=[
        "members", "legislation", "questions", "flatten", "attendance", "enrich", "all"
    ])
    parser.add_argument("--demo", action="store_true", help="Process only 10 TDs for quick testing")
    args = parser.parse_args()
    # dispatch to the appropriate module
```

The `--demo` flag is especially valuable — openkamer's `create_demo_data` lets contributors test the full pipeline in 10 minutes instead of 24 hours. For your project, `--demo` could limit to 10 TDs and 1 PDF instead of 175 TDs and 7 PDFs.

**b) JSON dump/restore for reproducibility**

openkamer provides database dumps at `openkamer.org/database/dumps/` so anyone can get a working dataset without running the scraper. This means:

- New contributors are productive in 5 minutes
- If the external API is down, development continues
- CI/CD tests run against a known dataset

**Applicable improvement:** After each successful pipeline run, dump your enriched CSVs + a metadata file (run timestamp, row counts, API response codes) into a `snapshots/` directory. Version them with the date: `snapshots/2026-04-07/`. This also gives you rollback capability if a new pipeline run produces bad data.

**c) Wikidata enrichment for politician photos and metadata**

openkamer pulls politician photos, birth dates, and social media links from Wikidata. The Oireachtas API doesn't provide photos, but Wikidata has entries for most Irish TDs.

**Applicable improvement:** A single SPARQL query to Wikidata can return photo URLs, Twitter handles, and birth dates for all Dáil members:

```sparql
SELECT ?person ?personLabel ?image ?twitter WHERE {
  ?person wdt:P39 wd:Q654291 .  # Q654291 = Teachta Dála
  OPTIONAL { ?person wdt:P18 ?image . }
  OPTIONAL { ?person wdt:P2002 ?twitter . }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
}
```

This isn't core to your pipeline but would add a "photos" column for any future dashboard and demonstrates external API integration beyond the Oireachtas ecosystem.

**d) Cron-driven incremental updates**

openkamer runs cron jobs every 5 minutes via `django-cron` to check for new data. This is overkill for your batch pipeline, but the principle matters: separate "first-time full load" from "incremental refresh."

**Applicable improvement:** Add a `--since` flag to your pipeline that only fetches bills/questions/divisions after a given date. Combined with the `date_start` parameter from §16.2, this means daily refreshes only pull the delta:

```
python run_pipeline.py legislation --since 2026-04-01
```

---

#### 17.2 From unitedstates/congress (US Congress — Python scrapers)

**Source:** https://github.com/unitedstates/congress — 1k stars, community-maintained, CC0 public domain.

A Python scraper suite that collects bills, amendments, votes, and nominations from the US Congress. Key patterns:

**a) `cache/` and `data/` directory separation**

unitedstates/congress maintains a strict split:

```
cache/       # raw downloaded files, never committed to git
data/        # processed output (JSON + XML per bill), committed
```

**Applicable improvement:** This maps directly to your medallion plan but with clear git rules:

```
bronze/      # raw API responses, raw PDFs — .gitignored
silver/      # flattened CSVs, cleaned data — .gitignored (too large)
gold/        # final enriched outputs, DuckDB file — committed (or versioned separately)
```

The key insight is that `cache/` (bronze) should be cheaply re-creatable from the API and never committed. Your current `members/members.json` and `bills/legislation_results.json` are cache, not output.

**b) One output-per-entity directory structure**

unitedstates/congress stores data organized by entity:

```
data/117/bills/hr/hr1/data.json     # House Resolution 1, 117th Congress
data/117/bills/hr/hr1/data.xml
data/117/votes/2021/h123/data.json  # House vote 123, 2021
```

**Applicable improvement:** Instead of one giant flat CSV, consider also producing per-TD JSON profiles in gold:

```
gold/td/Ivana-Bacik.D.2021-07-09/profile.json    # all data for this TD
gold/td/Ivana-Bacik.D.2021-07-09/legislation.json
gold/td/Ivana-Bacik.D.2021-07-09/questions.json
```

This is useful for any future per-TD dashboard page and makes it trivial to serve static JSON (like robmcelhinney/OireachtasVote does).

**c) CLI runner with `--force` and `--log` flags**

```
usc-run bills --congress=117 --force --log=info
```

`--force` bypasses cache and re-downloads. `--log` controls verbosity. These are cheap to add and make the pipeline self-documenting.

**Applicable improvement:** Your logging is already in place. Add `--force` to bypass any cached JSON files and `--log` level control:

```python
parser.add_argument("--force", action="store_true", help="Re-fetch from API, ignore cached JSON")
parser.add_argument("--log", choices=["debug", "info", "warning", "error"], default="info")
```

**d) Dual output format (JSON + XML)**

unitedstates/congress outputs both JSON and XML for every entity for maximum compatibility. For your project, the equivalent is outputting both CSV (for pandas/Excel users) and Parquet (for DuckDB/Spark). Parquet is 5-10x smaller and preserves column types:

```python
df.to_parquet("gold/enriched_td_attendance.parquet")
```

DuckDB reads Parquet natively and faster than CSV.

---

#### 17.3 From mysociety/theyworkforyou (UK Parliament — PHP/Perl)

**Source:** https://github.com/mysociety/theyworkforyou — 247 stars, actively maintained (commits last week), BSD license.

The gold standard for parliamentary monitoring — everything MPs say in Hansard, plus Lords, Scottish Parliament, and Northern Ireland Assembly. Key patterns:

**a) Separate parser repo from presentation repo**

theyworkforyou has two repos:
- `parlparse` — the data parser/scraper (Perl/Python)
- `theyworkforyou` — the web frontend (PHP)

They are entirely decoupled. The parser writes to a MySQL database that the frontend reads.

**Applicable improvement:** Your data pipeline and any future dashboard should not share code. Keep `dail_extractor/` as the pipeline. If you ever build a Streamlit/Dash/React frontend, it should be a separate project that reads from `gold/` outputs or a DuckDB file. This prevents "just one more UI feature" from creeping into your ETL scripts.

**b) Data limitations page**

theyworkforyou has an explicit `/data-limitations` page documenting what's missing, what's delayed, and what's known-incorrect. robmcelhinney's lobbyieng project does the same.

**Applicable improvement:** Add a `DATA_LIMITATIONS.md` to your repo documenting:
- The 4 TDs with fada characters that fail API calls (and which ones)
- The 1 TD missing from PDF extraction (which one and why)
- Attendance data only covers sitting days, not committee attendance
- Bill sponsorship vs co-sponsorship ambiguity in the API response
- Questions data has no date bounds currently (§16.2)

This is both honest engineering and interview-impressive — it shows you understand your data's boundaries.

**c) Codespaces / one-click setup**

theyworkforyou offers a GitHub Codespaces devcontainer that sets up the full environment. Their `scripts/quick-populate` seeds the database with minimal data in ~1 hour.

**Applicable improvement:** A `devcontainer.json` for your project would let anyone clone and run in VS Code with zero local setup. Even without Codespaces, a `Makefile` or `justfile` with common commands is valuable:

```makefile
install:
    pip install -r requirements.txt

run-all:
    python run_pipeline.py all

run-demo:
    python run_pipeline.py all --demo

test:
    python -m pytest tests/
```

---

#### 17.4 From okfn-brasil/serenata-de-amor (Brazil — Python + AI)

**Source:** https://github.com/okfn-brasil/serenata-de-amor — 4.6k stars, 105 contributors, MIT license.

Uses ML to flag suspicious congressperson expenses. Key patterns:

**a) Three-component architecture: Rosie + Jarbas + Toolbox**

- **Rosie** — the analysis engine (ML classifiers for expense anomalies)
- **Jarbas** — the Django web UI for browsing flagged expenses
- **serenata-toolbox** — a pip-installable data access package

**Applicable improvement:** If your project grows, consider extracting your data access layer (API fetching, PDF parsing, CSV reading) into a small `dail_toolbox` package. This means any future notebook, dashboard, or analysis script can `from dail_toolbox import fetch_members` without importing your entire pipeline. Even just putting shared functions in a proper Python package with `__init__.py` exports is a step toward this.

**b) Research notebooks alongside production code**

serenata-de-amor maintains a separate `notebooks/` repo for exploratory analysis, distinct from production code. Contributors experiment in notebooks, then graduate working code into the main repo.

**Applicable improvement:** Add a `notebooks/` directory for your DuckDB SQL practice queries (from §11), exploratory visualizations, and data quality checks. Keep them separate from pipeline code. This also gives you material for the portfolio section of REARCHITECTURE.md — embedded notebook outputs make excellent README screenshots.

**c) Versioned datasets via toolbox**

"A few times per year, we upload versioned datasets accessible via the toolbox." This means consumers can pin to a known dataset version while the scrapers evolve.

**Applicable improvement:** Tag your gold outputs with a version or date when you produce them. Even a simple `gold/metadata.json`:

```json
{
  "pipeline_version": "0.2.0",
  "run_date": "2026-04-07T14:30:00",
  "members_count": 175,
  "bills_count": 1170,
  "questions_count": null,
  "source_pdfs": 7,
  "failed_api_calls": 4,
  "notes": "4 TDs with fada encoding errors excluded"
}
```

---

#### 17.5 From robmcelhinney/lobbyieng (Ireland lobbying data — Next.js/Python/SQLite)

**Source:** https://github.com/robmcelhinney/lobbyieng — the most architecturally polished Irish civic project.

**a) SQLite as intermediate analytical store**

lobbyieng uses `parser.py` → SQLite → Next.js API routes. Tables are dropped and recreated on each parse run (idempotent). Indexes are created post-load.

**Applicable improvement:** This validates your DuckDB plan. The pattern is: Python parser populates the DB, the DB serves queries. Your equivalent:

```
oireachtas_api_service.py → bronze JSON
flatten + enrich scripts → silver CSVs
load into DuckDB → gold/dail.duckdb
```

One insight from lobbyieng: **create indexes after bulk-loading**, not during. DuckDB does this automatically for most queries, but explicit indexes on join keys (`unique_member_code`, `bill_id`) will speed up multi-table queries.

**b) API caching and precomputed insights**

lobbyieng's latest commit is literally "perf: add SQLite tuning, API caching, and precomputed explore insights." They precompute expensive aggregations at build time rather than query time.

**Applicable improvement:** After loading data into DuckDB, run your most expensive aggregation queries (party summaries, per-TD bill counts, committee network) and materialize them as tables. When your future dashboard needs them, it reads a precomputed table instead of re-aggregating 1170 bills.

**c) Data limitations page built into the app**

lobbyieng has a dedicated `/data-limitations` route documenting coverage gaps, update cadence, and known issues.

**Applicable improvement:** Reinforces §17.3b above — document your data boundaries. Do this in your README or a separate file. Interviewers will ask "what are the limitations of your data?" and having a documented answer is better than improvising.

---

#### 17.6 Summary — concrete actions from comparable projects

| Action | Inspired by | Priority | Effort |
|---|---|---|---|
| Create `run_pipeline.py` with subcommands + `--demo` flag | openkamer, unitedstates/congress | **High** | ~50 lines |
| Separate `bronze/` (gitignored cache) from `gold/` (outputs) | unitedstates/congress | **High** | Directory restructure |
| Add `--force` and `--since` flags for incremental runs | openkamer, unitedstates/congress | **Medium** | ~20 lines |
| Create `gold/metadata.json` after each pipeline run | serenata-de-amor | **Medium** | ~15 lines |
| Write `DATA_LIMITATIONS.md` documenting known gaps | theyworkforyou, lobbyieng | **Medium** | Documentation only |
| Add `Makefile` with `install`, `run-all`, `run-demo`, `test` targets | theyworkforyou | **Medium** | ~15 lines |
| Output Parquet alongside CSV for DuckDB performance | unitedstates/congress dual-format pattern | **Low** | 1 line per output |
| Add `notebooks/` directory for DuckDB SQL exploration | serenata-de-amor | **Low** | Directory + notebooks |
| Extract shared utilities into installable `dail_toolbox` package | serenata-de-amor | **Low** | Package restructure |
| Wikidata SPARQL query for TD photos/social links | openkamer | **Low** | Single API call |
| Create `devcontainer.json` for one-click dev setup | theyworkforyou | **Low** | Config file |
| Precompute expensive aggregations as materialised DuckDB tables | lobbyieng | **Low** | After DuckDB setup |
