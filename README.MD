# Dáil Tracker

[![Python](https://img.shields.io/badge/python-%3E%3D3.11-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/peweet/dail_tracker/blob/main/pyproject.toml)
[![Status](https://img.shields.io/badge/status-alpha-orange.svg)](https://github.com/peweet/dail_tracker)
[![DuckDB](https://img.shields.io/badge/DuckDB-analytical%20layer-yellow.svg)](https://duckdb.org/)
[![Polars](https://img.shields.io/badge/Polars-dataframes-lightgrey.svg)](https://pola.rs/)
[![Streamlit](https://img.shields.io/badge/Streamlit-dashboard-red.svg)](https://streamlit.io/)

A civic-data pipeline for Irish parliamentary transparency.

Dáil Tracker ingests, joins, and analyses public records about elected representatives from the Oireachtas and related public bodies. The goal is to make parliamentary activity — attendance, legislation, questions, votes, payments, declared interests, and lobbying contacts — easier to search and cross-reference in one place.

The strategic emphasis is on **cross-source linkage**: declared interests joined against the Companies Registration Office, charities register, and (in progress) Iris Oifigiúil notices, so that questions like *"is this TD's declared portfolio consistent with the public record?"* can be answered programmatically rather than by hand.

Only the current Dáil and Seanad are in scope. See [`doc/DATA_LIMITATIONS.md`](doc/DATA_LIMITATIONS.md) for the rationale.

> **Status:** Active development. The dashboard, the medallion pipeline, and the registered SQL view layer are working. Several enrichers (Iris Oifigiúil, CRO, charities register) are implemented in `pipeline_sandbox/` and not yet wired into the main pipeline. See [Project status](#project-status) below.

---

## Table of contents

- [Project status](#project-status)
- [Project inspiration](#project-inspiration)
- [Why this project exists](#why-this-project-exists)
- [Data sources](#data-sources)
- [Dataset coverage](#dataset-coverage)
- [Pipeline overview](#pipeline-overview)
- [Join strategy](#join-strategy)
- [Technology rationale](#technology-rationale)
- [Installation](#installation)
- [Dashboard](#dashboard)
- [Testing](#testing)
- [Repository structure](#repository-structure)
- [Data limitations](#data-limitations)
- [Competitive landscape](#competitive-landscape)
- [Contributing](#contributing)

---

## Project status

| Surface | Status |
|---|---|
| Medallion pipeline (bronze → silver → gold) | Working |
| Registered SQL view layer (`sql_views/`) | Working — 40+ views |
| Streamlit dashboard | Working — 11 pages, 3 explicitly marked POC |
| Logic firewall (UI reads only via registered views) | Enforced |
| Provenance / source-PDF expanders | On every primary page |
| Oireachtas API ingestion (members, votes, debates, questions) | Working |
| Attendance PDF extraction | Working |
| Payments PDF extraction | Working |
| Register of Members' Interests PDF extraction | Working |
| Lobbying.ie ingestion | Working — manual CSV export |
| Iris Oifigiúil parser | Implemented in main + sandbox; not in `pipeline.py` |
| CRO companies normaliser | Implemented in `pipeline_sandbox/`; not integrated |
| Charities register normaliser | Implemented in `pipeline_sandbox/`; not integrated |
| Cross-source unified Member Overview profile | Page exists; `unique_member_code` join key not fully populated across all source views |
| Tests (silver / gold / SQL view shape) | Partial — many tests skip when fixtures are missing |
| Public deployment | None — local Streamlit only |

---

## Project inspiration

This project is heavily inspired by the following civic transparency and parliamentary data projects:

- [TheyWorkForYou](https://github.com/mysociety/theyworkforyou)
- [How They Vote EU](https://howtheyvote.eu/)
- [OpenKamer — Dutch Parliamentary Scraper](https://github.com/openkamer/openkamer)

For an audit of how Dáil Tracker compares to existing Irish civic-tech projects, see [`doc/COMPETITIVE_LANDSCAPE.md`](doc/COMPETITIVE_LANDSCAPE.md).

---

## Why this project exists

The data is public, but fragmented.

A TD's parliamentary activity, declared financial interests, attendance records, payments, votes, and lobbying contacts are published across different systems, formats, and update schedules. Existing Irish civic-tech tools focus on one source each (KildareStreet on debates and votes, Lobbyieng on lobbying, Gript Datahub on payments). No tool joins them.

This project joins those sources into one analytical pipeline so questions like these become easier to answer:

- Which TDs are lobbied most, and by whom?
- Do lobbying topics cluster around the parliamentary questions or votes that follow?
- What financial interests are declared by elected representatives, and are those declarations consistent with the public CRO record?
- How do attendance, payments, votes, and committee activity relate across the same person and timeframe?
- (In progress) When a minister signs a statutory instrument in Iris Oifigiúil, what lobbying contacts preceded it?

---

## Data sources

| Source | Used for | Format |
|---|---|---|
| [Oireachtas Open Data API](https://api.oireachtas.ie/) | Members, legislation, questions, votes, parliamentary metadata | API / JSON |
| [data.oireachtas.ie](https://data.oireachtas.ie/) | Attendance, Parliamentary Standard Allowance, Register of Members' Interests | PDF |
| [lobbying.ie](https://www.lobbying.ie/app/home/search) | Lobbying returns | CSV export (manual) |
| [Companies Registration Office (CRO)](https://cro.ie/) | Director / shareholding cross-reference *(sandbox)* | Bulk export |
| [Charities Regulator — Public Register](https://www.charitiesregulator.ie/) | Trustee cross-reference *(sandbox)* | XLSX |
| [Iris Oifigiúil](https://www.irisoifigiuil.ie/) | Statutory instruments, public appointments, official notices *(sandbox)* | PDF (twice weekly) |

### Additional source links

- [Register of Members' Interests](https://www.oireachtas.ie/en/members/register-of-members-interests/)
- [Record of Attendance](https://www.oireachtas.ie/en/publications/?q=&topic%5B%5D=record-of-attendance)
- [Register of Lobbying](https://www.lobbying.ie/app/home/search)

All upstream data remains subject to the terms and licences of the publishing bodies.

---

## Dataset coverage

| Dataset | Source | Coverage | Main script | Status |
|---|---|---|---|---|
| Members | Oireachtas API | Current Dáil / Seanad | `members_api_service.py` | Production |
| Legislation | Oireachtas API | Per member / bill metadata | `legislation.py` | Production |
| Parliamentary questions | Oireachtas API | Per member | `members_api_service.py`, `questions.py` | Production |
| Votes / divisions | Oireachtas API | Division and vote records | `transform_votes.py` | Production |
| Attendance | Oireachtas PDFs | Published attendance records | `attendance.py` | Production |
| Parliamentary Standard Allowance | Oireachtas PDFs | Published payment records | `payments.py` | Production |
| Register of Members' Interests | Oireachtas PDFs | Published declarations | `member_interests.py`, `iris_memeber_interests.py` | Production |
| Lobbying returns | lobbying.ie CSV exports | Downloaded register returns | `lobby_processing.py` | Production (manual ingest) |
| Iris Oifigiúil | irisoifigiuil.ie | Twice-weekly state gazette | `iris_oifiguil_etl.py`, `iris_oifiiguil_downloader.py` | **Sandbox** |
| CRO companies | CRO bulk export | Director / company / address normalised | `pipeline_sandbox/cro_normalise.py` | **Sandbox** |
| Charities register | Charities Regulator XLSX | Register, trustees, annual reports | `pipeline_sandbox/charity_normalise.py` | **Sandbox** |

**Sandbox** datasets are implemented and produce silver outputs but are not yet executed by `pipeline.py`. Their integration plans live in [`CRO/INTEGRATION_PLAN.md`](CRO/INTEGRATION_PLAN.md) and `pipeline_sandbox/*.md` notes files.

---

## Pipeline overview

The project follows a medallion-style layout:

```text
data/
├── bronze/   raw API, PDF, CSV, XLSX inputs
├── silver/   cleaned and flattened intermediate outputs
└── gold/     joined analytical outputs (CSV + Parquet)
```

### Medallion architecture

```text
┌─────────────────────────────────────────────────────────────────┐
│                        BRONZE (Raw)                             │
│  Untouched data exactly as received from source                 │
│  • Raw PDF files (Oireachtas, Iris Oifigiúil)                   │
│  • Raw API JSON responses                                       │
│  • Raw CSV / XLSX exports (lobbying.ie, CRO, Charities)         │
│  • PyMuPDF extracted tables, with no transformations            │
├─────────────────────────────────────────────────────────────────┤
│                        SILVER (Cleaned)                         │
│  Validated, deduplicated, schema-enforced data                  │
│  • Names parsed into first_name / last_name                     │
│  • Description split into role / dáil / dates / limit           │
│  • Columns renamed with meaningful names                        │
│  • Data types enforced (DATE, INT, etc.)                        │
│  • Members flattened to a clean narrow schema                   │
│  • Parquet outputs alongside CSV for fast DuckDB reads          │
├─────────────────────────────────────────────────────────────────┤
│                        GOLD (Analytical)                        │
│  Business-ready tables, joined and aggregated                   │
│  • td_attendance: one row per TD with attendance counts         │
│  • td_bills: one row per bill with sponsoring TD                │
│  • td_questions: one row per question with TD and topic         │
│  • td_profile: joined TD info, party, constituency, etc.        │
│  • td_enriched: attendance + member metadata + bills joined     │
│  • Registered as DuckDB views in sql_views/ for the dashboard   │
└─────────────────────────────────────────────────────────────────┘
```

### Pipeline stages

```text
1. Download source PDFs and API data
2. Flatten member, legislation, vote, and lobbying records
3. Extract structured tables from PDFs
4. Normalise names and join keys
5. Enrich silver outputs into analytical datasets
6. Build gold outputs and register SQL views
```

### Run the pipeline

```bash
python pipeline.py
```

`pipeline.py` is the orchestration entrypoint and should be treated as the current source of truth for execution order. It runs each step as a subprocess so that a single failing source does not abort the whole run; the exit code reports overall success.

The Iris, CRO, and charities enrichers in `pipeline_sandbox/` are not currently invoked by `pipeline.py`. Each is self-contained and can be run directly.

---

## Join strategy

The source datasets do not share a reliable universal person identifier.

The pipeline therefore creates an intermediate fuzzy join key for names:

1. Unicode-normalise names.
2. Lowercase.
3. Strip accents, apostrophes, punctuation, and honorifics.
4. Remove whitespace.
5. Sort characters alphabetically.

This is implemented in:

```text
normalise_join_key.py
```

Downstream, the project uses a stable `unique_member_code` (Oireachtas member code) as the cross-page join key wherever the source carries it. Some views still need this column added — these are flagged in code with `TODO_PIPELINE_VIEW_REQUIRED`.

The fuzzy join key is intentionally treated as a pipeline aid rather than a permanent identifier. Collisions and missed matches remain possible and are documented as data limitations.

---

## Technology rationale

### DuckDB

[DuckDB](https://duckdb.org/) is used for the analytical layer because it gives the project local SQL analytics without requiring a database server.

It works well for reproducible joins, aggregations, and gold-layer queries over local files. The dashboard reads exclusively through registered DuckDB views; UI Python code is restricted to `SELECT / WHERE / ORDER BY / LIMIT` against those views.

### Polars

[Polars](https://pola.rs/) is the default for new pipeline / ETL code where fast, memory-efficient DataFrame transformations matter — particularly for the larger silver intermediates (votes, lobbying, Iris).

### Pandas

[Pandas](https://pandas.pydata.org/) is used in the Streamlit layer for compatibility with the wider Python data ecosystem and DuckDB's `.df()` output.

### SQL

SQL is used for analytical transformations that benefit from being explicit, reviewable, and easy to reason about.

Joins, aggregations, and gold-layer marts are easier to audit when expressed declaratively. View definitions live in `sql_views/`.

### Streamlit

[Streamlit](https://streamlit.io/) is kept as a thin presentation layer.

The dashboard reads prepared outputs and handles display concerns only:

- filtering
- sorting
- formatting
- navigation
- export

Transformation logic belongs in the pipeline, SQL, or gold layer. The page modules in `utility/pages_code/` are restricted to retrieval-only SQL against registered views.

---

## Installation

Requires Python 3.11+.

```bash
git clone https://github.com/peweet/dail_tracker.git
cd dail_tracker
pip install -e .
```

### Optional development and analytical dependencies

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

### Pages

Production pages:

- **Attendance** — plenary attendance records, year-by-year hall of fame / shame, per-TD timeline
- **Member Overview** — unified per-politician accountability profile drawing from multiple sources
- **Votes** — division records, party breakdown, per-TD voting patterns
- **Interests** — Register of Members' Interests, browseable by year and category
- **Payments** — Parliamentary Standard Allowance per TD, annual evolution
- **Lobbying** — three-path investigative lookup (politician / organisation / policy area)
- **Legislation** — bills, sponsors, debates, stages, related documents
- **Committees** — committee assignments

POC / experimental pages (currently visible in the sidebar; pending consolidation):

- **Lobbyist (POC)** — alternative lobbying lookup focused on individual lobbyists
- **Legislation (POC)** — alternative legislation surface
- **SI (POC)** — statutory instruments view (placeholder pending Iris integration)

The dashboard is designed for exploration and interpretation of prepared datasets, not for core data transformation.

### Page contracts

Each page has a YAML contract specifying its data dependencies, permitted SQL operations, and known data quality issues. Contracts and runbooks are maintained in [`dail_tracker_bold_ui_contract_pack_v5/`](dail_tracker_bold_ui_contract_pack_v5/).

---

## Testing

Testing is work in progress.

Current tests focus on:

- join-key normalisation
- silver / gold output shape checks
- basic data-quality checks where generated outputs exist
- SQL view registration and shape
- lightweight Streamlit smoke tests

### Run tests

```bash
pip install -e ".[dev]"
pytest test/ -v
```

### Run linting

```bash
ruff check .
```

Some integration-style tests require generated `data/silver/` or `data/gold/` outputs. Where those files are missing, tests currently skip rather than fail. Tightening this to fail-on-missing-fixture is a known follow-up.

---

## Repository structure

```text
dail_tracker/
├── .streamlit/                              Streamlit configuration
├── avatar/                                  Static dashboard assets
├── data/
│   ├── bronze/                              Raw downloaded inputs
│   ├── silver/                              Cleaned and flattened outputs
│   └── gold/                                Analytical outputs (CSV + Parquet)
│
├── doc/                                     Project notes, limitations, audits, plans
│   ├── DATA_LIMITATIONS.md
│   ├── COMPETITIVE_LANDSCAPE.md
│   ├── ENRICHMENTS.md
│   └── …
│
├── sql_views/                               DuckDB view definitions consumed by the dashboard
├── sql_queries/                             Ad-hoc analytical SQL
│
├── services/                                Service-layer modules (API, logging, storage)
├── utility/
│   ├── app.py                               Streamlit app entrypoint
│   ├── config.py
│   ├── constants.py
│   ├── pages_code/                          Dashboard page implementations
│   ├── data_access/                         Per-page DuckDB connection + view loading
│   ├── ui/                                  Shared UI components, avatars, links, exports
│   ├── shared_css.py                        Shared dashboard CSS
│   └── select_drop_rename_cols_mappings.py
│
├── dail_tracker_bold_ui_contract_pack_v5/   Page YAML contracts and runbooks
│
├── pipeline_sandbox/                        Experimental enrichers not yet wired into pipeline.py
│   ├── cro_normalise.py                     CRO companies register normaliser
│   ├── charity_normalise.py                 Charities register normaliser
│   ├── iris_oifigiuil_etl_polars.py         Polars rewrite of Iris parser
│   ├── lobbying_bootstrap.py                Lobbying.ie ingestion automation
│   └── …
│
├── experimental/                            Scratch work not required for normal pipeline use
├── CRO/                                     CRO integration plan
├── test/                                    pytest tests and test notes
│
├── attendance.py                            Attendance PDF extraction
├── payments.py                              Payment PDF extraction
├── member_interests.py                      Register of Members' Interests extraction
├── iris_memeber_interests.py                Member-interest extraction from Iris notices
├── iris_oifiguil_etl.py                     Iris Oifigiúil PDF parser (production copy)
├── iris_oifiiguil_downloader.py             Iris Oifigiúil downloader
├── lobby_processing.py                      Lobbying CSV processing
├── legislation.py                           Legislation processing
├── transform_votes.py                       Vote transformation
├── flatten_members_json_to_csv.py           Member JSON flattening
├── enrich.py                                Cross-dataset enrichment
├── normalise_join_key.py                    Name normalisation and fuzzy join key logic
├── pdf_endpoint_check.py                    Source endpoint checks
├── pdf_downloader.py                        PDF downloader
├── pdf_backfill_scraper.py                  Historical PDF backfill
├── generate_interests_ranking.py            Interests ranking helper
├── members_api_service.py                   Oireachtas API client
├── questions.py                             Parliamentary questions
├── analytics_loading.py                     Analytics helpers
├── manifest.py                              Pipeline run manifest
├── config.py                                Paths, directories, logging, constants
├── pipeline.py                              Pipeline orchestration
├── tear_down.py                             Cleanup utilities
├── pyproject.toml                           Build metadata and dependency groups
└── README.MD
```

---

## Data limitations

This project depends on public sources that change format, structure, and publication cadence.

Known limitations include:

- PDF tables can change layout between publications.
- API fields can change or silently omit edge cases.
- Name-based joins can collide or miss matches.
- Manual CSV exports (lobbying.ie) depend on the export date and selected filters.
- Lobbying register data quality before 2020 is inconsistent and is currently filtered out at the gold layer.
- Iris Oifigiúil page-1 layouts vary; some giant single-document PDFs (fisheries, appointments catalogues) bypass the standard split logic and are quarantined.
- Current scope is limited to the current Dáil and Seanad.

See:

```text
doc/DATA_LIMITATIONS.md
```

---

## Competitive landscape

For an audit of how Dáil Tracker compares to existing Irish civic-tech tools (KildareStreet, Lobbyieng, Gript Datahub, Oireachtas Connect, etc.), and the strategic gaps Dáil Tracker is built to fill, see [`doc/COMPETITIVE_LANDSCAPE.md`](doc/COMPETITIVE_LANDSCAPE.md).

The short version: source-by-source competitors exist for votes/debates, lobbying, and payments. **Cross-source linkage** (Iris × CRO × charities × declared interests × lobbying) is not currently offered by any other Irish tool, and that is where this project's strategic value sits.

---

## Contributing

Open an issue or pull request.

For data issues, include:

- source link
- expected value
- observed value
- affected dataset or script
