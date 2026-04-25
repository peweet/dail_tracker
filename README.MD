# Dáil Tracker

[![Python](https://img.shields.io/badge/python-%3E%3D3.11-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/peweet/dail_tracker/blob/main/pyproject.toml)
[![Status](https://img.shields.io/badge/status-alpha-orange.svg)](https://github.com/peweet/dail_tracker)
[![DuckDB](https://img.shields.io/badge/DuckDB-analytical%20layer-yellow.svg)](https://duckdb.org/)
[![Polars](https://img.shields.io/badge/Polars-dataframes-lightgrey.svg)](https://pola.rs/)
[![Streamlit](https://img.shields.io/badge/Streamlit-dashboard-red.svg)](https://streamlit.io/)

A civic-data pipeline for Irish parliamentary transparency.

This project is heavily inspired by They Work For You 

- https://github.com/mysociety/theyworkforyou

and

- https://howtheyvote.eu/

Dáil Tracker ingests, joins, and analyses public data from the Oireachtas and related public bodies. The goal is to make parliamentary activity — attendance, legislation, questions, votes, payments, declared interests, and lobbying contacts — easier to search and cross-reference.

Only the current Dáil and Seanad are in scope. See `doc/DATA_LIMITATIONS.md` for the rationale.

**Status:** active development. Ingestion, flattening, enrichment, and dashboard work are progressing for the current parliamentary term.

---

## Why this project exists

The data is public, but fragmented.

A TD's parliamentary activity, declared financial interests, attendance records, payments, votes, and lobbying contacts are published across different systems, formats, and update schedules.

This project joins those sources into one analytical pipeline so questions like these become easier to answer:

- Which TDs are lobbied most, and by whom?
- Do lobbying topics appear alongside parliamentary questions or votes?
- What financial interests are declared by elected representatives?
- How do attendance, payments, votes, and committee activity relate to other public records?

---

## Data sources

| Source | Used for | Format |
|---|---|---|
| Oireachtas Open Data API | Members, legislation, questions, votes, parliamentary metadata | API / JSON |
| data.oireachtas.ie | Attendance, parliamentary standard allowance, register of interests | PDF |
| lobbying.ie | Lobbying returns | CSV export |

Source links:

- Oireachtas API: `https://api.oireachtas.ie/`
- Oireachtas data portal: `https://data.oireachtas.ie/`
- Register of Members' Interests: `https://www.oireachtas.ie/en/members/register-of-members-interests/`
- Record of Attendance: `https://www.oireachtas.ie/en/publications/?q=&topic%5B%5D=record-of-attendance`
- Register of Lobbying: `https://www.lobbying.ie/app/home/search`

All upstream data remains subject to the terms and licences of the publishing bodies.

---

## Dataset coverage

| Dataset | Source | Coverage | Main script |
|---|---|---|---|
| Members | Oireachtas API | Current Dáil / Seanad scope | `members_api_service.py` |
| Legislation | Oireachtas API | Per member / bill metadata | `legislation.py` |
| Parliamentary questions | Oireachtas API | Per member | `members_api_service.py` |
| Votes / divisions | Oireachtas API | Division and vote records | `transform_votes.py` |
| Attendance | Oireachtas PDFs | Published attendance records | `attendance.py` |
| Parliamentary Standard Allowance | Oireachtas PDFs | Published payment records | `payments.py` |
| Register of Members' Interests | Oireachtas PDFs | Published declarations | `member_interests.py` |
| Lobbying returns | lobbying.ie CSV exports | Downloaded register returns | `lobby_processing.py` |

---

## Pipeline overview




The project follows a medallion-style layout:

```text
data/
├── bronze/   raw API, PDF, and CSV inputs
├── silver/   cleaned and flattened intermediate outputs
└── gold/     joined analytical outputs
```



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
Pipeline stages:

```text
1. Check source endpoints
2. Download source PDFs and API data
3. Flatten member, legislation, vote, and lobbying records
4. Extract structured tables from PDFs
5. Normalise names and join keys
6. Enrich silver outputs into analytical datasets
7. Build gold outputs for SQL and dashboard use
```

Run the pipeline:

```bash
python pipeline.py
```

`pipeline.py` is the orchestration entrypoint and should be treated as the current source of truth for execution order.

---

## Join strategy

The source datasets do not share a reliable universal person identifier.

The pipeline therefore creates an intermediate fuzzy join key for names:

1. Unicode-normalise names
2. Lowercase
3. Strip accents, apostrophes, punctuation, and honorifics
4. Remove whitespace
5. Sort characters alphabetically

This is implemented in:

```text
normalise_join_key.py
```

This approach is useful for early-stage joins where name formatting differs between sources, but it is intentionally treated as a pipeline aid rather than a permanent identifier. Collisions and missed matches remain possible and are documented as data limitations.

---

## Technology rationale

### DuckDB

DuckDB is used for the analytical layer because it gives the project local SQL analytics without requiring a database server. It works well for reproducible joins, aggregations, and gold-layer queries over local files.

### Polars

Polars is used where fast, memory-efficient DataFrame transformations are useful. It is a good fit for repeated cleaning, reshaping, and validation of larger intermediate datasets.

### Pandas

Pandas remains useful for compatibility with the wider Python data ecosystem, especially where existing libraries or extraction workflows return pandas-friendly structures.

### SQL

SQL is used for analytical transformations that benefit from being explicit, reviewable, and easy to reason about. Joins, aggregations, and gold-layer marts are easier to audit when expressed declaratively.

### Streamlit

Streamlit is kept as a thin presentation layer. The dashboard should read prepared outputs and handle display concerns only: filtering, sorting, formatting, navigation, and export. Transformation logic belongs in the pipeline, SQL, or gold layer.

---

## Installation

Requires Python 3.11+.

```bash
git clone https://github.com/peweet/dail_tracker.git
cd dail_tracker
pip install -e .
```

Optional development and analytical dependencies:

```bash
pip install -e ".[db]"
pip install -e ".[dev]"
```

---

## Dashboard

Run the Streamlit dashboard:

```bash
streamlit run utility/app.py
```

Dashboard pages currently include:

- Interests
- Committees
- Attendance
- Payments
- Lobbying
- Votes

The dashboard is designed for exploration and interpretation of prepared datasets, not for core data transformation.

---

## Testing

Testing is work in progress.

Current tests focus on:

- join-key normalisation
- silver/gold output shape checks
- basic data-quality checks where generated outputs exist
- lightweight Streamlit smoke tests

Run tests:

```bash
pip install -e ".[dev]"
pytest test/ -v
```

Run linting:

```bash
ruff check .
```

Some integration-style tests require generated `data/silver/` or `data/gold/` outputs. Where those files are missing, tests may skip rather than fail.

---

## Repository structure

```text
dail_tracker/
├── .streamlit/                         Streamlit configuration
├── avatar/                             Static dashboard assets
├── data/
│   ├── bronze/                         Raw downloaded inputs
│   ├── silver/                         Cleaned and flattened outputs
│   └── gold/                           Analytical outputs
│
├── doc/                                Project notes, limitations, reviews, refactor plans
├── experimental/                       Scratch work not required for normal pipeline use
├── services/                           Service-layer modules for API and storage work
├── sql_queries/                        SQL used for analytical transformations
├── test/                               pytest tests and test notes
├── utility/
│   ├── app.py                          Streamlit app entrypoint
│   ├── constants.py                    Source URLs and constants
│   ├── pages_code/                     Dashboard page implementations
│   ├── page_contracts/                 Page-level UI/data contracts
│   ├── styles/                         Dashboard styling
│   └── select_drop_rename_cols_mappings.py
│
├── attendance.py                       Attendance PDF extraction
├── payments.py                         Payment PDF extraction
├── member_interests.py                 Register of Members' Interests extraction
├── lobby_processing.py                 Lobbying CSV processing
├── legislation.py                      Legislation processing
├── transform_votes.py                  Vote transformation
├── flatten_members_json_to_csv.py      Member JSON flattening
├── flatten_service.py                  Bill/service flattening
├── enrich.py                           Cross-dataset enrichment
├── normalise_join_key.py               Name normalisation and fuzzy join key logic
├── pdf_endpoint_check.py               Source endpoint checks
├── pdf_downloader.py                   PDF downloader
├── config.py                           Paths, directories, logging, and constants
├── pipeline.py                         Pipeline orchestration
├── tear_down.py                        Cleanup utilities
├── pyproject.toml                      Build metadata and dependency groups
└── README.MD
```

---

## Data limitations

This project depends on public sources that change format, structure, and publication cadence.

Known limitations include:

- PDF tables can change layout between publications.
- API fields can change or silently omit edge cases.
- Name-based joins can collide or miss matches.
- Manual CSV exports depend on the export date and selected filters.
- Current scope is limited to the current Dáil and Seanad.

See:

```text
doc/DATA_LIMITATIONS.md
```

---

## Contributing

Open an issue or pull request.

For data issues, include:

- source link
- expected value
- observed value
- affected dataset or script

