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

> **Status:** Active development. The dashboard, the medallion pipeline, and the registered SQL view layer are working. The Iris Oifigiúil parser, CRO companies normaliser, charities register normaliser, and the Tier-A charity↔CRO join are all wired into `pipeline.py` and producing gold outputs. See [Project status](#project-status) below.

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
| Registered SQL view layer (`sql_views/`) | Working — 65+ views |
| Streamlit dashboard | Working — 11 pages (Member Overview is the canonical home, TheyWorkForYou-style) |
| Logic firewall (UI reads only via registered views) | Enforced |
| Provenance / source-PDF expanders | On every primary page |
| Oireachtas API ingestion (members, votes, debates, questions) | Working — paginated questions ingest (264k rows, 1000-row API cap lifted) |
| Attendance PDF extraction | Working |
| Payments PDF extraction | Working — full TAA + PRA reconciliation (`payments_full_psa_etl.py`) |
| Register of Members' Interests PDF extraction | Working |
| Lobbying.ie ingestion | Working — YTD CSV poller (`lobbying_poller.py`) wired into `pipeline.py` |
| Iris Oifigiúil parser | Production — incremental shard cache (per-PDF skip), wired into `pipeline.py` |
| CRO companies normaliser | Production — wired into `pipeline.py` |
| Charities register normaliser + Tier-A join + gold enrichment | Production — wired into `pipeline.py` |
| Iris SI ↔ bill enrichment | Production — wired into `pipeline.py` |
| SI entity / signatory enrichment (Wikidata ministerial tenure) | Production — wired into `pipeline.py` |
| Wikidata socials & Wikipedia member links | Production — wired into `pipeline.py` |
| Cross-source unified Member Overview profile | Working — `unique_member_code` populated on attendance, votes, payments, debates, legislation (sponsor_join_key), interests, appointments, and lobbying views |
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
| Members | Oireachtas API | Current Dáil / Seanad | `services/members.py`, `flatten_members_json_to_csv.py` | Production |
| Legislation (bills) | Oireachtas API | Per member / bill metadata | `legislation.py` | Production |
| Bill amendments | Oireachtas API | Amendment text and sponsors | `bill_amendments_flatten.py` | Production |
| Parliamentary questions | Oireachtas API | Per member, paginated (264k rows; 1000-row API cap lifted) | `services/member_paginated.py`, `questions.py` | Production |
| Debate listings (Dáil + Seanad) | Oireachtas API | Section-level debate index for member linkage | `services/dbsect_harvest.py`, `dbsect_listings_flatten.py` | Production |
| Votes / divisions | Oireachtas API | Division and vote records | `services/votes.py`, `transform_votes.py` | Production |
| Attendance | Oireachtas PDFs | Published attendance records | `attendance.py` | Production |
| Parliamentary Standard Allowance | Oireachtas PDFs | Full TAA + PRA payments per TD | `payments_full_psa_etl.py` | Production |
| Register of Members' Interests | Oireachtas PDFs | Published declarations | `member_interests.py`, `iris_memeber_interests.py` | Production |
| Lobbying returns | lobbying.ie YTD CSV | YTD register returns auto-polled per run | `lobbying_poller.py`, `lobby_processing.py`, `lobbying_pdf_extract.py` | Production |
| Iris Oifigiúil | irisoifigiuil.ie | Twice-weekly state gazette (SIs, appointments, notices) | `iris_oifigiuil_poller.py`, `iris_oifigiuil_etl_polars.py`, `iris_incremental_shards.py` | Production (incremental) |
| Iris SI ↔ bill enrichment | Iris + Oireachtas | Links statutory instruments back to parent enabling Acts | `iris_si_bill_enrichment.py` | Production |
| SI signatory enrichment | Iris + Wikidata | Minister-of-the-day attribution for each signed SI | `ministerial_tenure_build.py`, `si_entity_enrichment.py` | Production |
| CRO companies | CRO bulk export | Director / company / address normalised | `cro_normalise.py` | Production |
| Charities register | Charities Regulator XLSX | Register, trustees, annual reports + Tier-A join + NACE-sector gold | `charity_normalise.py`, `charity_resolved.py`, `charity_enriched.py` | Production |
| Member external links (socials / Wikipedia) | Wikidata SPARQL | Twitter/X, Wikipedia URLs per TD/Senator | `wikidata_socials_etl.py` | Production |

A small number of one-off enrichers (e.g. committees long-format ETL, public-appointments enrichment) still live in [`pipeline_sandbox/`](pipeline_sandbox/) until promoted; the rest of the historical sandbox has been integrated into `pipeline.py`.

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

### Pipeline shape

The pipeline is organised as **eight domain refresh chains**. Each chain is a
self-contained `<domain>_refresh.py` script that orchestrates its own step
sequence (poll → extract → enrich) for one logical area of the dataset.

`pipeline.py` is a thin dispatcher around those chains: by default it runs them
all in the order below; flags let you select or skip subsets.

| # | Chain | Script | What it does |
|---|---|---|---|
| 1 | `bootstrap` | `bootstrap_refresh.py` | Poll new PDFs + Members API + flatten members & debates. Produces `flattened_members.parquet` (the cross-source join-key carrier) and the questions/votes/debates JSON every downstream chain reads. |
| 2 | `members` | `members_refresh.py` | Wikidata SPARQL pulls (socials/Wikipedia, ministerial-of-the-day tenure) + committees long-format unpivot. |
| 3 | `payments` | `payments_refresh.py` | PSA (TAA + PRA) parser + member enrichment (`unique_member_code` / party / constituency). |
| 4 | `attendance` | `attendance_refresh.py` | Record-of-Attendance PDF parser. |
| 5 | `interests` | `interests_refresh.py` | Register of Members' Interests PDF parser. |
| 6 | `lobbying` | `lobbying_refresh.py` | lobbying.ie YTD CSV + CRO + charities Tier-A join + gold enrichment. |
| 7 | `iris` | `iris_refresh.py` | Iris Oifigiúil poller + silver rebuild + SI entity + SI-bill + appointments + corporate notices. |
| 8 | `legislation` | `legislation_refresh.py` | Bills + questions + amendments + votes + cross-dataset `enrich.py`. |

**Cross-chain dependencies** (run upstream first if invoking a chain standalone):

- Every chain assumes `bootstrap` has refreshed `flattened_members.parquet`.
- `iris` step 3 (SI signatory enrichment) reads `ministerial_tenure` from `members`.
- `legislation` reads bronze questions/votes JSON written by `bootstrap.members_api`.

### Run the pipeline

```bash
# Full refresh — runs all 8 chains in default order
python pipeline.py

# Show chains and exit
python pipeline.py --list

# Run a single chain (everything else skipped)
python pipeline.py --select iris

# Run multiple chains (comma-separated)
python pipeline.py --select bootstrap,members,iris

# Run everything EXCEPT a chain
python pipeline.py --exclude lobbying
```

Each chain runs as a subprocess of `pipeline.py`. Per-chain logs land at
`logs/runs/<run_id>/steps/NN_<slug>.log`; the run manifest records every chain
with exit code and final-line summary. A chain failure is isolated — the next
chain still runs.

### Run a single chain directly

Every chain is also a normal Python script that you can invoke on its own
(no need to go through `pipeline.py`):

```bash
python iris_refresh.py             # iris poller → silver → 4 gold enrichments
python iris_refresh.py --help      # see chain-specific flags (e.g. --skip-poll)

python payments_refresh.py         # PSA ETL + member enrichment
python lobbying_refresh.py         # full lobbying + CRO + charity chain
```

Use `<chain>_refresh.py --help` to see whether a chain accepts skip-flags
(`iris` does; the others currently don't — adding them is a one-line change).

### Bronze pollers (called by chains)

Two pollers handle "have new PDFs been published since last run?" and are
invoked by the chains above. You normally don't run them directly:

- `oireachtas_pdf_poller.py` — payments + attendance + interests, called by
  `bootstrap_refresh.step_poll_oireachtas`.
- `iris_oifigiuil_poller.py` — Iris Oifigiúil Tue/Fri issues, called by
  `iris_refresh.step_poll`.

For ad-hoc backfill the Iris poller has explicit flags worth knowing:

```bash
# Backfill a single calendar year (Tue/Fri only; current year truncated at today)
python iris_oifigiuil_poller.py --year 2020

# Backfill multiple years in one run
python iris_oifigiuil_poller.py --year 2018 2019 2020

# Fixed-window backfill: last N days ending today
python iris_oifigiuil_poller.py --lookback-days 365

# Preview without downloading
python iris_oifigiuil_poller.py --year 2020 --dry-run
```

`--year` and `--lookback-days` are mutually exclusive. Anything already on disk is skipped (no network request), so wider windows are cheap to point at.

### Manual / operator utilities (outside the chains)

A handful of scripts live at repo root or in `pipeline_sandbox/` but are
**deliberately not** wired into any chain. They're tools you reach for by hand
when something needs investigation, backfill, or one-off repair. Listing them
here so you remember they exist:

| Script | When you'd reach for it |
|---|---|
| `lobbying_fetch.py --start YYYY-MM-DD --end YYYY-MM-DD [--force] [--dry-run]` | Backfill historical lobbying.ie CSVs for a date window. The chain's `lobbying_poller` only covers YTD. |
| `iris_oifigiuil_poller.py --year YYYY [--dry-run]` | Iris historical backfill (see Iris-backfill section above). The chain runs it incremental with no flags. |
| `iris_oifigiuil_etl_polars.py --rebuild` | Force a full historical Iris re-extract (ignore the per-PDF shard cache). |
| `iris_incremental_shards.py "glob"` | Populate/inspect the per-PDF parquet shard cache without running silver/gold. |
| `pdf_endpoint_check.py` | Smoke-test the Oireachtas PDF endpoint when you suspect upstream is down. Commented-out as STEPS[0] in the old pipeline; useful for triage. |
| `pdf_backfill_scraper.py` | Hunt for historical PDFs outside the poller's window. Legacy helper, no docstring; treat as archaeology. |
| `repair_future_iris_placeholders.py` | One-shot: cleans up 146-byte HTML stubs left by an older Iris downloader. Run once after switching pollers; already done in this repo. |
| `pipeline_sandbox/cbi_registers_extract.py [--download \| --extract \| --xref]` | Central Bank registers ↔ member interests + lobbying cross-ref experiment. |
| `pipeline_sandbox/public_appointments_enrichment.py --write` | Run standalone (also wired into `iris` chain). |
| `corporate_notices_enrichment.py --write` | Same — standalone variant. |
| `payments_member_enrichment.py` | Same — also wired into `payments` chain. |
| `pipeline_sandbox/cso_pxstat_extract.py --write` | CSO PxStat per-LA × year housing tables. Not yet UI-consumed. |
| `pipeline_sandbox/eurostat_extract.py --write` | Eurostat housing/tenure/citizenship indicators for Ireland. Not yet UI-consumed. |
| `pipeline_sandbox/open_csv_extract.py --write` | DHLGH open-data housing/homelessness CSVs in one pass. Not yet UI-consumed. |
| `pipeline_sandbox/hap_funding_xlsx_extract.py --write` | HAP per-LA expenditure & tenancies. Not yet UI-consumed. |
| `pipeline_sandbox/ssha_appendix_a19_extract.py --write` | SSHA Appendix A1.9 per-LA citizenship of waiting-list applicants. Not yet UI-consumed. |

**Sandbox `--write` convention:** sandbox scripts are dry-run by default and
require `--write` to emit the parquet. Root-level scripts always write. If a
sandbox extract earns a UI page later, fold it into the relevant chain (most
likely a new `housing_refresh.py`) and drop the `--write` flag at the same
time it graduates.

Exit codes: `0` = clean run · `1` = at least one download failed · `2` = a past Tue/Fri returned 4xx on every URL variant (slug pattern may have changed; investigate before trusting future runs).

### Iris ETL: incremental shard cache

`iris_oifigiuil_etl_polars.py` no longer re-parses every PDF on every run. Each bronze PDF is extracted once into a per-PDF parquet shard under `data/silver/iris_oifigiuil_shards/`, fingerprinted by `(mtime_ns, size, EXTRACTOR_VERSION)`. Subsequent runs skip any PDF whose fingerprint is unchanged.

Silver and gold still rebuild over the **full corpus** every run (the silver/gold step is cheap and per-`source_file`), so a back-dated correction to a single PDF still propagates through `iris_notice_events_*.csv`, `iris_si_taxonomy.csv`, and the audit table. Determinism comes from sorting on `(issue_date, source_file, page_number, line_order)` before write — adding new shards never reorders existing rows.

```text
data/silver/iris_oifigiuil_shards/
├── _manifest.json                  fingerprint per PDF
├── bronze/IR010119.parquet         raw extracted lines (one per PDF)
├── audit/IR010119.json             per-PDF audit meta
└── member_extracts/IR010119.json   member-interest page ranges
```

The default `pipeline.py` invocation is incremental by definition — no flag needed. Three ways to force a full historical re-extract:

```bash
# 1. Ad-hoc: ignore the manifest, re-extract every PDF
python iris_oifigiuil_etl_polars.py --rebuild

# 2. Bump EXTRACTOR_VERSION in iris_incremental_shards.py.
#    Required whenever extract_lines_raw or find_member_interest_page_ranges
#    changes shape, values, or parsing rules — the next run auto-rebuilds.

# 3. Nuke and restart: delete data/silver/iris_oifigiuil_shards/
rm -rf data/silver/iris_oifigiuil_shards
```

You can also populate / inspect the cache without running silver/gold:

```bash
python iris_incremental_shards.py "data/bronze/iris_oifigiuil/*.pdf"
python iris_incremental_shards.py "data/bronze/iris_oifigiuil/*.pdf" --rebuild
```

**Cron-readiness:** all writes go through `.part` → `replace`, so a SIGKILL mid-run leaves only complete files; the next run resumes. Each step in `pipeline.py` is wrapped in `try/except`, so an Iris extract failure does not poison the rest of the run.

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

Downstream, the project uses a stable `unique_member_code` (Oireachtas member code) as the cross-page join key wherever the source carries it. For sources that arrive without one — lobbying.ie, the revolving-door gold table — the view layer joins against `flattened_members.parquet` on a normalised name (`LOWER(strip_accents(TRIM()))`) to attach the code. Only matches against the current Dáil/Seanad registry survive; non-Oireachtas DPOs surface with NULL and the UI degrades to a non-clickable name.

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

Production pages (native top-nav; filter bars live in the main panel, sidebar is hidden):

- **Member Overview** — canonical per-politician profile (TheyWorkForYou pattern); the app's default route at `/` and `/member-overview`. Unifies attendance, votes, payments, interests, debates, questions, committee seats, lobbying contacts, and external socials/Wikipedia per TD.
- **Attendance** — plenary attendance records, year-by-year hall of fame / shame, per-TD timeline
- **Votes** — division records, party breakdown, per-TD voting patterns, member-moved notice copy
- **Interests** — Register of Members' Interests, browseable by year and category, with typeahead member lookup
- **Payments** — Parliamentary Standard Allowance per TD, annual evolution, alltime ranking
- **Lobbying** — three-path investigative lookup (politician / organisation / policy area) with DPO-firm/client breakdowns
- **Legislation** — bills, sponsors, debates, stages, related documents, pre-2014 Acts
- **Statutory Instruments** — Iris-derived SIs with title-quality fixes, signatories, and parent-Act linkage
- **Appointments** — Iris-derived public appointments index (boards, tribunals, statutory officers)
- **Committees** — committee assignments, party composition, member rosters
- **Glossary** — civic-vocabulary index (TAA, PRA, DPO, SI, Iris, etc.)

The dashboard is designed for exploration and interpretation of prepared datasets, not for core data transformation. Each page's data-access module lives in [`utility/data_access/`](utility/data_access/) and is restricted to `SELECT / WHERE / ORDER BY / LIMIT` against registered DuckDB views.

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
│   ├── committees_long_format_etl.py
│   ├── public_appointments_enrichment.py
│   └── …
│
├── experimental/                            Scratch work not required for normal pipeline use
├── CRO/                                     CRO integration plan
├── test/                                    pytest tests and test notes
│
├── attendance.py                            Attendance PDF extraction
├── payments_full_psa_etl.py                 Full TAA + PRA payments parser (replaces old payments.py)
├── member_interests.py                      Register of Members' Interests extraction
├── iris_memeber_interests.py                Member-interest extraction from Iris notices
├── iris_oifigiuil_etl_polars.py             Iris Oifigiúil PDF parser (incremental by default)
├── iris_incremental_shards.py               Per-PDF shard cache used by the Iris ETL
├── iris_oifigiuil_poller.py                 Iris Oifigiúil twice-weekly poller
├── iris_si_bill_enrichment.py               Links Iris SIs back to parent enabling Acts
├── si_entity_enrichment.py                  Minister-of-the-day attribution for SI signatories
├── ministerial_tenure_build.py              Wikidata-sourced ministerial tenure table
├── wikidata_socials_etl.py                  Wikidata socials + Wikipedia links per member
├── oireachtas_pdf_poller.py                 Multi-source poller (payments / attendance / interests)
├── lobbying_poller.py                       Auto-polls lobbying.ie YTD CSV endpoint
├── bootstrap_refresh.py                     Chain: PDF pollers + Members API + flatten members & debates
├── members_refresh.py                       Chain: Wikidata socials + ministerial tenure
├── payments_refresh.py                      Chain: PSA ETL + member enrichment
├── attendance_refresh.py                    Chain: Record-of-Attendance PDF parser
├── interests_refresh.py                     Chain: Register of Members' Interests PDF parser
├── lobbying_refresh.py                      Chain: lobbying.ie + CRO + charities
├── iris_refresh.py                          Chain: Iris poller + silver + 4 gold enrichments
├── legislation_refresh.py                   Chain: bills + questions + amendments + votes + enrich
├── lobby_processing.py                      Lobbying CSV processing
├── lobbying_pdf_extract.py                  Extracts embedded PDFs from lobbying returns
├── legislation.py                           Legislation processing
├── bill_amendments_flatten.py               Bill amendment flattening
├── transform_votes.py                       Vote transformation
├── flatten_members_json_to_csv.py           Member JSON flattening
├── dbsect_listings_flatten.py               Debate-section listings flattening
├── cro_normalise.py                         CRO companies normaliser
├── charity_normalise.py                     Charities register normaliser
├── charity_resolved.py                      Tier-A CRO ↔ charity join
├── charity_enriched.py                      Gold charities table with NACE sector + compliance flags
├── enrich.py                                Cross-dataset enrichment
├── normalise_join_key.py                    Name normalisation and fuzzy join key logic
├── pdf_endpoint_check.py                    Source endpoint checks
├── pdf_downloader.py                        PDF downloader
├── pdf_backfill_scraper.py                  Historical PDF backfill
├── repair_future_iris_placeholders.py       Iris placeholder-PDF repair helper
├── quarantine.py                            Quarantine helpers
├── questions.py                             Parliamentary questions silver flattener
├── analytics_loading.py                     Analytics helpers
├── manifest.py                              Pipeline run manifest
├── config.py                                Paths, directories, logging, constants
├── pipeline.py                              Pipeline orchestrator — dispatches refresh chains
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
