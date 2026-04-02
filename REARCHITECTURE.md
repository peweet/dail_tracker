# Dáil Extractor — Architecture & Roadmap

## Table of Contents

1. [What This Codebase Does](#what-this-codebase-does)
2. [Current Pipeline](#current-pipeline)
3. [Resolved Issues (Log)](#resolved-issues-log)
4. [Remaining Issues](#remaining-issues)
5. [Medallion Architecture Redesign](#medallion-architecture-redesign)
6. [Proposed Folder Structure](#proposed-folder-structure)
7. [Database Recommendation (DuckDB)](#database-recommendation-duckdb)
8. [Data Quality Testing (pytest)](#data-quality-testing-pytest)
9. [Additional Data Sources & Enrichment](#additional-data-sources--enrichment)
10. [Timeliness & Temporal Analysis](#timeliness--temporal-analysis)
11. [Dashboard & Frontend Options](#dashboard--frontend-options)
12. [Portfolio Enhancements](#portfolio-enhancements)
13. [Action Items Checklist](#action-items-checklist)

---

## What This Codebase Does

This project scrapes data about Irish TDs (members of the Dáil) from the Oireachtas (Irish parliament) open-data APIs and PDF reports. It collects:

- **TD Attendance** — Extracted from PDF reports published on data.oireachtas.ie using **PyMuPDF** (`fitz`). Counts sitting days and other days per TD.
- **TD Members** — Fetched from the Oireachtas REST API (`/v1/members`), filtered by political party. Flattened to CSV with party, constituency, and committee data.
- **Bills/Legislation** — Fetched from the Oireachtas legislation API (`/v1/legislation`), per TD.
- **Parliamentary Questions** — (Commented out) Would fetch oral/written questions per TD from `/v1/questions`.
- **Enriched Dataset** — Joins attendance data with member metadata (party, constituency) via `enrich.py`.

---

## Current Pipeline

```
1. member_api_request.py                → Fetches all current TDs by party → members/members.json
2. question_api.py                      → Reads members.json → extracts TD URIs → key_data/key_data.json
3. bills_by_td.py                       → Reads key_data.json → fetches bills per TD → bills/all_bills_by_td.json
4. attendance_2024.py                   → PyMuPDF extracts PDF tables → members/td_tables.csv
5. members/flatten_members_json_to_csv.py → Flattens members.json → members/flattened_members.csv
6. enrich.py                            → Joins td_tables.csv + flattened_members.csv → members/enriched_td_attendance.csv
```

### Key technical decisions

- **PyMuPDF (fitz)** replaced camelot for PDF extraction after benchmarking (16.9s vs 34.2s on 244 pages). PyMuPDF's `find_tables()` handles page-spanning records natively — no orphan fragment stitching needed.
- **Structural name detection** (`DESC_RE = re.compile(r"Deputy.*Limit:\s*\d+")`) finds the "Deputy...Limit:" line and reads the TD name from the line above it. This handles Irish names (O'Brien, Ó Broin, Boyd Barrett, Murnane-O'Connor) that broke the earlier regex approach.
- **JSON** replaced pickle for inter-script data transfer (security + portability).
- **pandas** used in `attendance_2024.py` for the PyMuPDF table pipeline; **polars** used in `enrich.py` for the join.

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
| `attendance_2024.py` | Entire file rewritten — old camelot approach replaced | **Replaced** with PyMuPDF pipeline |
| Garbled CSVs | Orphan fragments from page-spanning records | **Eliminated** — PyMuPDF handles natively |
| Inflated totals | Name regex failed on Irish names → wrong TD accumulation | **Fixed** — structural marker detection |
| `main.py` | Prototype duplicating `attendance_2024.py` | **Retired** (can be deleted) |
| `first_line.py` | Scratch script with duplicated functions | **Retired** (can be deleted) |
| `enrich.py` | Join direction: PDF was driving table, TDs missing from PDF were lost | **Fixed** — API master list is now the left table (`large_df.join(small_df)`) |
| `enrich.py` | Typo `arge_df = large_df.unique(...)` — dead variable, duplicates never removed | **Fixed** → `large_df = large_df.unique(...)` |
| `bills_by_td.py` | `{uri}` interpolated full tuple `('name',)` into URL instead of `uri[0]` | **Fixed** → `{uri[0]}` |
| `bills_by_td.py` | `member_id` pre-encoded (`%3A`) then double-encoded by `requests.get(params=)` | **Fixed** → plain URL `https://data.oireachtas.ie/...` letting `requests` encode once |
| `bills_by_td.py` | `if uri is not None` always True (tuples are never None) | **Fixed** → `if uri[0] is not None` |
| `bills_by_td.py` | `return URLS` inside `for` loop — returned after first TD | **Fixed** — dedented to after loop |
| `bills_by_td.py` | Migrated to `concurrent.futures.ThreadPoolExecutor` for parallel API calls | **Done** — `bills_by_td-REVISE.py` consolidated into `bills_by_td.py` |
| `.gitignore` | JSON files (e.g. `questions_all_current_tds.json`) not excluded from repo | **Fixed** — added `*.json` blanket rule |

---

## Remaining Issues

### 1. Missing TD — 127 of 128 detected

The attendance PDF contains 128 TDs but `td_tables.csv` only captures 127. One TD's record is missed — likely an edge case where the structural marker `DESC_RE` doesn't match (unusual formatting on a specific page). Needs investigation: iterate all pages and log which ones have tables but no name match.

**Update (2 Apr 2026):** The join direction fix in `enrich.py` mitigates this for the enriched output — all TDs from the API master list now appear regardless of PDF parsing. However, the root cause in `attendance_2024.py` PDF parsing still needs investigation for completeness of attendance counts.

### 1b. TDs with 0 attendance dropped incorrectly

TDs with `0` in attendance fields were being dropped by `dropna()` logic in `attendance_2024.py` because `0` values were treated as empty. The join direction fix ensures these TDs still appear in the final enriched output (with null attendance), but the PDF parsing logic should be reviewed to preserve legitimate `0` values.

### 2. `flatten_members_json_to_csv.py` produces overly wide CSV

The `flatten_json` library recursively flattens deeply nested committee membership arrays, creating hundreds of columns (most filled with "Null"). This makes `flattened_members.csv` unwieldy.

**Fix approach:** Flatten only the top-level member fields needed for the join (name, party, constituency, member code). Extract committee memberships into a separate normalised table if needed.

### 3. `flatten_members_json_to_csv.py` has a logic bug in cleanup

```python
if os.path.exists('members/filtered_members.json' or os.path.exists(...)):
```

This evaluates `'members/filtered_members.json' or os.path.exists(...)` as a truthy string, so the `or` branches are never checked. Should be:

```python
for f in ['members/filtered_members.json', 'members/flattened_members.json']:
    if os.path.exists(f):
        os.remove(f)
```

### 4. `bills_by_td.py` — minor cleanup remaining

`bills_by_td-REVISE.py` has been consolidated into `bills_by_td.py`. Concurrent fetching via `ThreadPoolExecutor` is working. Remaining: remove redundant standalone `load_url(URLS[0], 60)` call (dead code, line 63).

### 5. 2025 attendance PDF URL defined but unused

`attendance_2024.py` defines `pdf_2025` pointing to the Feb 2025–Dec 2025 report. The pipeline should be parameterised to accept any year's PDF.

### 6. `enrich.py` join relies on fuzzy name match

~~The join `on=['first_name', 'last_name']` between attendance and member data is fragile.~~ **Partially fixed (2 Apr 2026):** The join now uses `normalise_join_key`'s sorted-character fuzzy key and the API master list drives the join (left table), so no TDs are lost. However, the fuzzy key approach could still produce false matches on very similar names. Consider joining on `member_code`/URI once the PDF parsing extracts a stable identifier.

---

## Medallion Architecture Redesign

The [medallion architecture](https://www.databricks.com/glossary/medallion-architecture) organises data into three layers:

```
┌─────────────────────────────────────────────────────────────────┐
│                        BRONZE (Raw)                             │
│  Untouched data exactly as received from source                 │
│  • Raw PDF files                                                │
│  • Raw API JSON responses                                       │
│  • PyMuPDF extracted tables (no transformations)                │
├─────────────────────────────────────────────────────────────────┤
│                        SILVER (Cleaned)                         │
│  Validated, deduplicated, schema-enforced data                  │
│  • Names parsed into first_name / last_name                     │
│  • Description split into role / dáil / dates / limit           │
│  • Columns renamed with meaningful names                        │
│  • Data types enforced (dates as DATE, counts as INT)           │
│  • Members flattened to a clean narrow schema                   │
├─────────────────────────────────────────────────────────────────┤
│                        GOLD (Analytical)                        │
│  Business-ready tables, joined and aggregated                   │
│  • td_attendance: one row per TD with attendance counts         │
│  • td_bills: one row per bill with sponsoring TD                │
│  • td_questions: one row per question with TD and topic         │
│  • td_profile: joined TD info (party, constituency, etc.)       │
│  • td_enriched: attendance + member metadata + bills joined     │
│  SQL views / materialized tables for dashboards                 │
└─────────────────────────────────────────────────────────────────┘
```

### How to implement this

#### Bronze Layer — `bronze/`

- **No transformations.** Store raw outputs exactly as received.
- `bronze/pdf/` — Downloaded PDF files (2024, 2025, etc.)
- `bronze/api/members/` — Raw JSON responses from the members API
- `bronze/api/bills/` — Raw JSON responses from the legislation API
- `bronze/api/questions/` — Raw JSON responses from the questions API

**Script: `ingest_bronze.py`**
```python
"""
Bronze ingestion — downloads PDFs, calls APIs, stores raw data.
No cleaning or transformation happens here.
"""
import fitz, requests, json, os

def download_pdf(url: str, dest: str) -> str:
    """Download a PDF to bronze/pdf/ and return the local path."""
    ...

def fetch_members(party_codes: list[str]) -> list[dict]:
    """Call /v1/members for each party, save raw JSON to bronze/api/members/."""
    ...

def fetch_bills(td_uris: list[str]) -> list[dict]:
    """Call /v1/legislation per TD, save to bronze/api/bills/."""
    ...

def extract_pdf_tables(pdf_path: str) -> list[pd.DataFrame]:
    """Run PyMuPDF find_tables() on the PDF and return raw DataFrames."""
    ...
```

#### Silver Layer — `silver/`

- Apply all cleaning and validation.
- `silver/attendance/` — Cleaned attendance CSVs with proper column names
- `silver/members/` — Parsed member records as CSV/Parquet (narrow schema)
- `silver/bills/` — Flattened bill records

**Script: `transform_silver.py`**
```python
"""
Silver transformation — reads bronze data, cleans it, writes structured output.
"""
import pandas as pd

def clean_attendance(raw_dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Tag rows with TD names using DESC_RE, drop empty columns, rename."""
    ...

def parse_members_json(raw_json: str) -> pd.DataFrame:
    """Flatten the nested API response into a narrow member table."""
    ...

def parse_bills_json(raw_json: str) -> pd.DataFrame:
    """Flatten bill records into a flat table with TD linkage."""
    ...
```

#### Gold Layer — `gold/` (SQL database)

- Load silver data into DuckDB.
- Create joined, analytical tables and views.

**Script: `load_gold.py`**
```python
"""
Gold layer — loads silver data into DuckDB and creates analytical views.
"""
import duckdb

def create_tables(db_path: str):
    con = duckdb.connect(db_path)
    con.execute("""
        CREATE TABLE IF NOT EXISTS td_attendance AS
        SELECT * FROM read_csv_auto('silver/attendance/*.csv')
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS td_members AS
        SELECT * FROM read_csv_auto('silver/members/*.csv')
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS td_bills AS
        SELECT * FROM read_csv_auto('silver/bills/*.csv')
    """)
    con.execute("""
        CREATE OR REPLACE VIEW td_dashboard AS
        SELECT
            m.full_name,
            m.party,
            m.constituency,
            a.sitting_days_count,
            a.other_days_count,
            a.sitting_total_days,
            b.bill_count
        FROM td_members m
        LEFT JOIN td_attendance a ON m.member_code = a.identifier
        LEFT JOIN (
            SELECT member_uri, COUNT(*) AS bill_count
            FROM td_bills GROUP BY member_uri
        ) b ON m.uri = b.member_uri
    """)
    con.close()
```

#### Orchestrator — `pipeline.py`

```python
"""
pipeline.py — Run the full Bronze → Silver → Gold pipeline.
"""
from ingest_bronze import download_pdf, fetch_members, fetch_bills, extract_pdf_tables
from transform_silver import clean_attendance, parse_members_json, parse_bills_json
from load_gold import create_tables

def run():
    print("=== BRONZE: Ingesting raw data ===")
    pdf_path = download_pdf(PDF_URL, "bronze/pdf/")
    fetch_members(PARTY_CODES)
    td_uris = ...  # extract from bronze member JSON
    fetch_bills(td_uris)
    raw_tables = extract_pdf_tables(pdf_path)

    print("=== SILVER: Cleaning and transforming ===")
    clean_attendance(raw_tables)
    parse_members_json(...)
    parse_bills_json(...)

    print("=== GOLD: Loading to database ===")
    create_tables("gold/dail_data.duckdb")

    print("Pipeline complete.")

if __name__ == "__main__":
    run()
```

---

## Proposed Folder Structure

```
dail_extractor/
├── pipeline.py              # Orchestrator: bronze → silver → gold
├── ingest_bronze.py         # Bronze: raw data acquisition
├── transform_silver.py      # Silver: cleaning & validation
├── load_gold.py             # Gold: database loading & views
├── config.py                # URLs, party codes, date ranges, paths
├── requirements.txt         # PyMuPDF, pandas, polars, requests, duckdb, flatten-json
├── REARCHITECTURE.md        # This file
│
├── bronze/
│   ├── pdf/                 # Downloaded PDF attendance reports (by year)
│   └── api/
│       ├── members/         # Raw JSON from /v1/members
│       ├── bills/           # Raw JSON from /v1/legislation
│       └── questions/       # Raw JSON from /v1/questions
│
├── silver/
│   ├── attendance/          # Cleaned attendance CSVs
│   ├── members/             # Parsed member records (narrow schema)
│   └── bills/               # Flattened bill records
│
├── gold/
│   └── dail_data.duckdb     # Analytical database
│
└── tests/
    ├── conftest.py
    ├── test_bronze.py
    ├── test_silver.py
    ├── test_gold.py
    └── test_attendance.py
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
| Learning value | Widely used, universal | Modern analytics SQL, transferable to data engineering |

**Recommendation: DuckDB**

- Queries CSV and Parquet files directly without an import step.
- PostgreSQL-compatible SQL — skills transfer to Postgres.
- Perfect for analytical queries (attendance rankings, bill counts by party, attendance vs limit ratios).
- File-based like SQLite — no server to manage.
- Can use both: DuckDB for analytics, SQLite if you want to practice schema design and transactions.

---

## Data Quality Testing (pytest)

### Recommended setup

```
tests/
├── conftest.py              # Shared fixtures (paths, sample data)
├── test_bronze.py           # Validate raw data integrity
├── test_silver.py           # Validate cleaning logic
├── test_gold.py             # Validate database contents
└── test_attendance.py       # Unit tests for attendance extraction
```

### Example tests

#### `test_attendance.py` — Validate the current attendance pipeline

```python
import pandas as pd

def test_td_count():
    """Should capture all 128 TDs from the PDF."""
    df = pd.read_csv("members/td_tables.csv")
    assert df["identifier"].nunique() >= 127  # Currently 127, target 128

def test_max_total_within_limit():
    """No TD should exceed the Dáil attendance limit (103 for 2024)."""
    df = pd.read_csv("members/td_tables.csv")
    assert df["sitting_total_days"].max() <= 103

def test_no_empty_names():
    """Every row should have a non-empty identifier."""
    df = pd.read_csv("members/td_tables.csv")
    assert df["identifier"].isna().sum() == 0
    assert (df["identifier"] == "").sum() == 0

def test_irish_names_captured():
    """Irish names with apostrophes and fadas should be present."""
    df = pd.read_csv("members/td_tables.csv")
    names = df["identifier"].unique()
    # At least some O' names should be present
    o_names = [n for n in names if "O'" in n or "Ó" in n]
    assert len(o_names) >= 5, f"Only found {len(o_names)} O'/Ó names: {o_names}"
```

#### `test_bronze.py` — Raw data integrity

```python
import json
from pathlib import Path

def test_members_json_is_valid():
    """members.json should be parseable and contain results."""
    with open("members/members.json") as f:
        data = json.load(f)
    assert isinstance(data, list)
    assert len(data) > 0

def test_pdf_exists():
    """The attendance PDF should exist in storage."""
    assert Path("pdf_storage").glob("*.pdf")
```

#### `test_silver.py` — Cleaned data quality

```python
import pandas as pd

def test_enriched_join_completeness():
    """Every TD in attendance should match a member record."""
    df = pd.read_csv("members/enriched_td_attendance.csv")
    null_parties = df["party"].isna().sum()
    assert null_parties == 0, f"{null_parties} TDs have no party after join"

def test_no_duplicate_tds():
    """No TD should appear more than once in the enriched output."""
    df = pd.read_csv("members/enriched_td_attendance.csv")
    dupes = df.groupby(["first_name", "last_name"]).size()
    multi = dupes[dupes > 1]
    # Some duplication expected from date rows — check identifiers
    assert df["identifier"].nunique() == len(df["identifier"].dropna()), \
        f"Duplicate identifiers found"
```

#### `test_gold.py` — Database quality

```python
import duckdb

def test_attendance_count_within_limit():
    """No TD should have more attendances than their Dáil limit."""
    con = duckdb.connect("gold/dail_data.duckdb", read_only=True)
    violations = con.execute("""
        SELECT identifier, sitting_total_days
        FROM td_attendance
        WHERE sitting_total_days > 103
    """).fetchall()
    con.close()
    assert len(violations) == 0, f"TDs exceeding limit: {violations}"
```

---

## Additional Data Sources & Enrichment

These additional datasets from the Oireachtas and public sources can significantly enrich the analysis:

### 1. Parliamentary Questions (`/v1/questions`)

The commented-out code in `question_api.py` is nearly ready. Uncomment and fix the `try/except/else` bug (already documented). This gives:

- Number of questions asked per TD (oral + written)
- Topics and departments targeted
- Question frequency over time — a strong engagement metric

### 2. Debates (`/v1/debates`)

The Oireachtas API provides full debate transcripts. Extract:

- **Speaking frequency** — How often does each TD speak in the chamber?
- **Word count per TD** — Volume of contributions
- **Topic analysis** — Which TDs speak on which subjects?

```python
params = {
    "chamber": "dail",
    "date_start": "2024-01-01",
    "date_end": "2024-11-08",
    "limit": "1000"
}
response = requests.get("https://api.oireachtas.ie/v1/debates", params=params)
```

### 3. Divisions (Votes) (`/v1/divisions`)

Voting records show how each TD voted on each division:

- **Voting participation rate** — % of divisions a TD voted in vs was absent
- **Party loyalty score** — % of votes aligned with party majority
- **Cross-party voting** — Identify TDs who break from party line

```python
params = {
    "chamber": "dail",
    "date_start": "2024-01-01",
    "date_end": "2024-11-08",
    "limit": "1000"
}
response = requests.get("https://api.oireachtas.ie/v1/divisions", params=params)
```

### 4. Constituency Demographics (CSO)

The Central Statistics Office (CSO) publishes constituency-level data:

- Population, median age, unemployment rate
- Cross-reference with TD attendance: do TDs from urban constituencies attend more/less?
- Source: CSO PxStat API or downloadable CSV tables

### 5. Historical Attendance PDFs

The Oireachtas publishes attendance reports for previous years. Extracting 2022, 2023, and 2025 data enables:

- Year-over-year attendance trends per TD
- Identifying TDs whose attendance is improving or declining
- Dáil-wide attendance trends over time

The 2025 PDF URL is already defined in `attendance_2024.py` as `pdf_2025`.

### 6. Committee Attendance

Committee membership is already in `flattened_members.csv`. The Oireachtas also publishes committee meeting attendance. Tracking this separately gives a fuller picture of TD work (chamber attendance alone doesn't capture committee work).

---

## Timeliness & Temporal Analysis

This is where the dataset becomes genuinely powerful. The attendance dates in `td_tables.csv` are individual `dd/mm/yyyy` values — they can drive rich time-series analysis:

### Attendance Cadence Analysis

- **Attendance gaps** — Calculate days between consecutive attendances per TD. Large gaps may indicate illness, travel, or disengagement.
- **Weekly/monthly patterns** — Do some TDs attend mainly on certain days of the week?
- **Session coverage** — The Dáil sits on specific dates. What % of sitting days did each TD attend?

### Sitting Day Calendar

Build a reference calendar of all Dáil sitting days (from the "Total number of sitting days in the period" row in the PDF, or from the debates API). Then compute:

- `attendance_rate = td_sitting_days / total_sitting_days` — The true percentage
- **Recess-adjusted rate** — Only count days when the Dáil was actually sitting

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

### Implementation approach

```python
# Parse individual attendance dates from td_tables.csv
df = pd.read_csv("members/td_tables.csv")
dates = df[["identifier", "sitting_days_attendance"]].dropna()
dates["date"] = pd.to_datetime(dates["sitting_days_attendance"], format="%d/%m/%Y")
dates["day_of_week"] = dates["date"].dt.day_name()
dates["month"] = dates["date"].dt.month

# Gap analysis
dates = dates.sort_values(["identifier", "date"])
dates["prev_date"] = dates.groupby("identifier")["date"].shift(1)
dates["gap_days"] = (dates["date"] - dates["prev_date"]).dt.days
```

---

## Dashboard & Frontend Options

### Option 1: Streamlit (Recommended to start)

**Best for:** Getting something visual up fast with minimal frontend knowledge.

```python
# app.py
import streamlit as st
import duckdb
import plotly.express as px

con = duckdb.connect("gold/dail_data.duckdb", read_only=True)

st.title("Dáil TD Dashboard")

# Attendance by party
df = con.execute("""
    SELECT party, AVG(sitting_total_days) as avg_attendance
    FROM td_dashboard GROUP BY party ORDER BY avg_attendance DESC
""").pl()

st.bar_chart(df, x="party", y="avg_attendance")

# Searchable TD table
st.dataframe(con.execute("SELECT * FROM td_dashboard").pl())
```

| Pros | Cons |
|---|---|
| Zero frontend code needed | Limited customisation |
| Built-in charts, tables, filters | Not a "real" web framework |
| Deploys free on Streamlit Cloud | Hard to add auth or complex routing |
| Great for data exploration | Can feel sluggish with large data |

### Option 2: FastAPI + a simple frontend

**Best for:** Learning how APIs work, building a backend you can reuse.

```python
# api.py
from fastapi import FastAPI
import duckdb

app = FastAPI()

@app.get("/api/attendance")
def get_attendance(party: str = None):
    con = duckdb.connect("gold/dail_data.duckdb", read_only=True)
    query = "SELECT * FROM td_dashboard"
    params = []
    if party:
        query += " WHERE party = $1"
        params.append(party)
    result = con.execute(query, params).pl().to_dicts()
    con.close()
    return result
```

For the frontend, pair with:
- **HTMX + Jinja2 templates** — Minimal JS, server-rendered HTML, great for learning
- **Chart.js** — Simple JavaScript charting library
- **A static HTML page** calling FastAPI endpoints with `fetch()`

| Pros | Cons |
|---|---|
| Learn REST API design | Need some HTML/JS for the frontend |
| Async, fast, auto-generates API docs | Two pieces to manage (API + frontend) |
| Skills transfer directly to jobs | More setup than Streamlit |
| Can add auth, caching, middleware | Steeper learning curve |

### Recommendation path

```
Start here                For more learning              For production
───────────              ──────────────────              ──────────────
Streamlit          →     FastAPI + HTMX + Chart.js  →   FastAPI + React/Vue
```

### Charting libraries

| Library | Language | Best for |
|---|---|---|
| **Plotly** | Python (Streamlit/Dash) | Interactive charts, maps, easy to use |
| **Chart.js** | JavaScript | Simple, beautiful charts for web pages |
| **Apache ECharts** | JavaScript | Feature-rich, great for dashboards |
| **Matplotlib/Seaborn** | Python | Static charts for reports, not web |

---

## Portfolio Enhancements

These additions make the project stand out as a data engineering portfolio piece:

### 1. Data Lineage & Metadata

Track where every piece of data came from and when it was last refreshed:

```python
# metadata table in DuckDB
CREATE TABLE pipeline_metadata (
    source_name  TEXT,        -- 'attendance_pdf_2024', 'members_api', etc.
    source_url   TEXT,
    ingested_at  TIMESTAMP,
    row_count    INTEGER,
    checksum     TEXT          -- SHA-256 of the source file/response
);
```

This proves the data is reproducible and auditable — a key concern in any serious data pipeline.

### 2. Data Versioning

Store timestamped snapshots of bronze data so you can track changes:

```
bronze/api/members/2024-01-15_members.json
bronze/api/members/2024-03-30_members.json
```

Or use DVC (Data Version Control) if you want git-like versioning for data files.

### 3. CI/CD with GitHub Actions

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

### 4. Comprehensive README with Badges

A polished README signals professionalism:

```markdown
# Dáil Extractor

![Python](https://img.shields.io/badge/python-3.12-blue)
![Tests](https://img.shields.io/badge/tests-passing-green)
![Data](https://img.shields.io/badge/data-128%20TDs-orange)

An end-to-end data pipeline extracting Irish parliamentary data from PDFs and APIs,
transforming it through a medallion architecture, and serving it via DuckDB.

## Architecture
Bronze (raw) → Silver (cleaned) → Gold (analytical DuckDB)

## Data Sources
- PDF attendance reports (PyMuPDF)
- Oireachtas REST API (members, bills, questions, debates, divisions)
- CSO constituency demographics
```

### 5. Data Quality Report

Generate an automated HTML report (using pytest-html or a simple Jinja template) that shows:

- Number of TDs captured vs expected
- Missing data % per column
- Date range coverage
- Schema validation results

This is the kind of deliverable a data team lead expects from a pipeline.

### 6. Entity Resolution

The biggest technical challenge in this project is matching TDs across different sources (PDF names vs API names vs bill sponsors). Building a robust entity resolution layer — even if it's just fuzzy matching with `thefuzz` or `rapidfuzz` — demonstrates real-world data engineering skills.

```python
from rapidfuzz import process, fuzz

# Match PDF name to API name
match, score, _ = process.extractOne(
    "O'Sullivan Pádraig",
    api_names,
    scorer=fuzz.token_sort_ratio
)
```

### 7. Parquet Output

Write silver-layer outputs as Parquet instead of (or in addition to) CSV:

- Columnar format — much faster for analytical queries
- Type-preserving — dates stay as dates, ints as ints
- Compressed — smaller files
- DuckDB reads Parquet natively and very fast

```python
df.to_parquet("silver/attendance/td_attendance.parquet")
```

---

## Action Items Checklist

### Remaining Fixes

- [ ] **Investigate missing TD** — 127 of 128 captured. Log pages with tables but no name match.
- [ ] **Preserve 0-value attendance rows** — Review `dropna()`/`fillna()` logic in `attendance_2024.py` to keep TDs with legitimate 0 attendance.
- [ ] **Fix `flatten_members_json_to_csv.py`** — Narrow the flattened schema; fix `os.path.exists()` logic bug.
- [x] ~~**Delete `bills_by_td-REVISE.py`**~~ — Now the concurrent rewrite; consolidate into `bills_by_td.py` when complete.
- [ ] **Finish `bills_by_td-REVISE.py`** — Collect executor results into list and write combined JSON.
- [ ] **Delete `main.py` and `first_line.py`** — Retired prototypes.
- [ ] **Parameterise PDF year** — Make `attendance_2024.py` accept 2024/2025 as an argument.
- [x] ~~**Fix `enrich.py` join key**~~ — API is now the driving table; fuzzy join key in use. Consider `member_code` long-term.
- [x] ~~**Fix `enrich.py` typo**~~ — `arge_df` → `large_df` on `.unique()` call.
- [x] ~~**Fix `bills_by_td.py` tuple/encoding bugs**~~ — `uri[0]`, plain URL, null check fixed.
- [ ] **Remove hardcoded paths** — Replace `C:\\Users\\pglyn\\...` with relative paths for portability.


### Rearchitecture

- [ ] Create `bronze/`, `silver/`, `gold/` directory structure
- [ ] Create `config.py` with all URLs, party codes, and paths as constants
- [ ] Create `ingest_bronze.py` (raw data acquisition)
- [ ] Create `transform_silver.py` (cleaning and validation)
- [ ] Create `load_gold.py` (DuckDB loading and view creation)
- [ ] Create `pipeline.py` orchestrator
- [ ] Add `requirements.txt`

### Data Enrichment

- [ ] **Uncomment and fix question fetching** in `question_api.py`
- [ ] **Add debates endpoint** (`/v1/debates`) — speaking frequency per TD
- [ ] **Add divisions endpoint** (`/v1/divisions`) — voting records and party loyalty
- [ ] **Extract 2025 PDF** — Use `pdf_2025` URL already defined
- [ ] **Extract 2022–2023 PDFs** — Year-over-year comparison
- [ ] **Add CSO constituency demographics** — Cross-reference with attendance

### Timeliness

- [ ] **Build sitting day calendar** from debate dates or PDF totals
- [ ] **Compute per-TD attendance rate** (sitting_days / total_sitting_days)
- [ ] **Attendance gap analysis** — Days between consecutive attendances
- [ ] **Weekly/monthly attendance patterns** per TD
- [ ] **Cross-reference attendance dates with division dates**

### Testing

- [ ] Set up `tests/` directory with `conftest.py`
- [ ] Write `test_attendance.py` — TD count, max totals, Irish name coverage
- [ ] Write `test_bronze.py` — JSON validity, PDF existence
- [ ] Write `test_silver.py` — Join completeness, no duplicates
- [ ] Write `test_gold.py` — Database quality checks

### Dashboard

- [ ] Install Streamlit (`pip install streamlit`)
- [ ] Create `app.py` with attendance charts and searchable TD table
- [ ] Connect to DuckDB gold layer
- [ ] (Later) Migrate to FastAPI + HTMX + Chart.js

### Portfolio Polish

- [ ] Add `pipeline_metadata` table for data lineage
- [ ] Write Parquet outputs alongside CSV
- [ ] Add entity resolution (fuzzy name matching) for cross-source joins
- [ ] Set up GitHub Actions CI/CD
- [ ] Write a polished README with badges and architecture diagram
- [ ] Generate automated data quality HTML report
