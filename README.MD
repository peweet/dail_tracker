# Dáil Tracker

[![Python](https://img.shields.io/badge/python-%3E%3D3.12-blue.svg)](https://www.python.org/)
[![License: AGPL v3](https://img.shields.io/badge/license-AGPL--3.0-green.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-alpha-orange.svg)](https://github.com/peweet/dail_tracker)

A civic-data pipeline for Irish parliamentary transparency.

Dáil Tracker ingests, joins, and analyses public records about Irish elected
representatives — attendance, legislation, questions, votes, payments, declared
interests, and lobbying contacts — and presents them in one searchable dashboard.

The point of difference is **cross-source linkage**: declared interests joined
against the Companies Registration Office, the charities register, and Iris
Oifigiúil notices, so a TD's public footprint can be checked across sources
programmatically rather than by hand. No other Irish civic-tech tool does this.

Scope is the current Dáil and Seanad only. See
[`doc/DATA_LIMITATIONS.md`](doc/DATA_LIMITATIONS.md) for the rationale, and
[`doc/COMPETITIVE_LANDSCAPE.md`](doc/COMPETITIVE_LANDSCAPE.md) for how it
compares to existing tools.

Inspired by [TheyWorkForYou](https://github.com/mysociety/theyworkforyou),
[How They Vote EU](https://howtheyvote.eu/), and
[OpenKamer](https://github.com/openkamer/openkamer).

---

## Data sources

| Source | Used for | Format |
|---|---|---|
| [Oireachtas Open Data API](https://api.oireachtas.ie/) | Members, legislation, questions, votes | API / JSON |
| [data.oireachtas.ie](https://data.oireachtas.ie/) | Attendance, allowances, Register of Members' Interests | PDF |
| [lobbying.ie](https://www.lobbying.ie/app/home/search) | Lobbying returns | CSV |
| [Companies Registration Office](https://cro.ie/) | Director / shareholding cross-reference | Bulk export |
| [Charities Regulator](https://www.charitiesregulator.ie/) | Trustee cross-reference | XLSX |
| [Iris Oifigiúil](https://www.irisoifigiuil.ie/) | Statutory instruments, appointments, official notices | PDF |
| [eTenders / OGP](https://www.etenders.gov.ie/) | Public procurement awards (supplier → CRO match) | CSV |
| [TED](https://ted.europa.eu/) | EU-journal Irish award notices (silver only) | API / JSON |
| [Central Bank of Ireland registers](https://registers.centralbank.ie/) | Regulated-entity cross-reference for corporate notices | PDF |
| [Local Authority Annual Financial Statements](https://www.gov.ie/) | Council spend by service division | PDF |

Upstream data stays under the publishing bodies' own licences — see
[Licensing](#licensing) and [`NOTICE.md`](NOTICE.md). Candidate future datasets
are catalogued in [`doc/ENRICHMENTS.md`](doc/ENRICHMENTS.md).

**Editorial rule:** no inference in the app UI. Every claim shown to a user must
trace to a query; context and real-world claims appear only as cited footnotes.

---

## Pipeline

The pipeline follows a medallion layout — `data/bronze/` (raw inputs),
`data/silver/` (cleaned), `data/gold/` (joined analytical outputs) — and is
organised as a sequence of domain refresh chains. `pipeline.py` is a thin
dispatcher that runs them in order; flags select or skip subsets. Per-chain logs
land at `logs/runs/<run_id>/steps/`, a manifest records each chain, and
chain-level try/except keeps one flaky source from poisoning the rest.

```bash
python pipeline.py                       # full refresh (all chains)
python pipeline.py --list                # show chains and exit
python pipeline.py --select iris         # run one chain
python pipeline.py --select members,iris # a comma-separated subset
python pipeline.py --exclude lobbying    # run everything except one
```

The core domain chains are also standalone scripts (`<domain>_refresh.py`) you
can run directly — e.g. `python iris_refresh.py`. Use `--help` for chain-specific
flags.

The chains, in run order:

- **Core domains:** `bootstrap` (PDF/API polling + member & debate flattening),
  `members` (Wikidata + ministerial tenure + committees), `payments`,
  `attendance`, `seanad` (parity, reuses the Dáil parsers), `interests`,
  `lobbying` (+ CRO + charities join), `iris` (SIs + appointments + corporate
  notices), and `legislation` (bills + questions + amendments + votes).
- **Cross-reference & enrichment:** `afs` (LA financial statements → spend by
  division), `cbi` (Central Bank register ↔ corporate notices), `cro` (CRO
  company register ↔ corporate notices), `procurement` (eTenders awards +
  supplier→CRO match), `procurement_lobbying` (supplier ↔ lobbying overlap), and
  `ted` (EU award notices, silver only).
- **Monitoring (read-only, never mutate pipeline data):** `freshness`
  (data-age signal per domain → `data/_meta/freshness.json`) and `source_health`
  (per-source staleness / listing reachability → `data/_meta/source_health.json`).

Every chain assumes `bootstrap` has run first.

### Joins

There's no shared person identifier across sources. Where the source carries the
Oireachtas member code it's used directly as `unique_member_code`; otherwise the
view layer joins on a normalised name (see
[`shared/normalise_join_key.py`](shared/normalise_join_key.py)). Collisions and
missed matches remain possible and are documented as data limitations.

---

## Installation

Requires Python 3.12+.

```bash
git clone https://github.com/peweet/dail_tracker.git
cd dail_tracker
uv sync                                       # app runtime only (what Streamlit Cloud installs)
uv sync --extra pipeline --group dev          # full local dev: + ETL deps + test/lint/typecheck tooling
```

## Dashboard

```bash
streamlit run utility/app.py
```

A Streamlit app (titled *Oireachtas Explorer*) with fourteen pages — a Home
landing view, Member Overview (the canonical TheyWorkForYou-style profile),
Attendance, Votes, Interests, Payments & Donations, Lobbying, Legislation,
Statutory Instruments, Appointments, Corporate Notices, Committees, Courts &
Judiciary, and a Glossary.

Streamlit is a thin presentation layer: pages read **only** through registered
DuckDB views and are restricted to `SELECT / WHERE / ORDER BY / LIMIT`. All
transformation lives in the pipeline, SQL views (`sql_views/`), or gold layer.
Each page has a YAML contract in
[`dail_tracker_bold_ui_contract_pack_v5/`](dail_tracker_bold_ui_contract_pack_v5/).

**Stack:** DuckDB for the analytical layer, Polars for ETL, pandas in the
Streamlit layer, SQL views for reviewable joins.

---

## Roadmap

Active and planned work — data already lands in the pipeline ahead of the UI, so
several datasets exist in gold/silver before they get a dedicated page.

- **Procurement page** — the `procurement` chain (eTenders/OGP awards +
  supplier→CRO match) and `ted` chain (EU award notices) already produce gold/
  silver data (59k awards · 1.9k authorities · 16k suppliers). Today it surfaces
  only via the Lobbying page's "also a state supplier" enrichment; a dedicated
  Procurement tile is planned, built around the *awards-are-not-payments* caveat
  (`value_safe_to_sum`) and the lobbying/CRO cross-source flags. Full IA in
  [`doc/PROCUREMENT_BUILD_PLAN.md`](doc/PROCUREMENT_BUILD_PLAN.md).
- **Courts & Judiciary** — the live tile today is the daily **Legal Diary**
  (anonymised sittings, bench, case listings). A fuller section is planned over
  the validated sandbox datasets: the **bench roster**, an **appointments &
  government** spine (who appointed which judge, from Iris), **court-performance**
  (clearance, waiting times, 94-courthouse map), and **aggregate conduct** stats.
  Deliberately excluded: revolving-door name-matching (tested, removed — false
  positives), the judgments corpus (copyright), and per-judge scoring. Sketch and
  build contracts in
  [`doc/judiciary_feature_sources_claude_plan.md`](doc/judiciary_feature_sources_claude_plan.md).
- **LA Annual Financial Statements** — the `afs` chain produces council
  spend-by-division silver data; a public-finance view over it is planned.

Candidate future datasets are catalogued in
[`doc/ENRICHMENTS.md`](doc/ENRICHMENTS.md).

---

## Testing

```bash
uv sync --extra pipeline --group dev
uv run pytest test/ -v
uv run ruff check .
```

Tests cover join-key normalisation, silver/gold output shape, SQL view
registration, and lightweight Streamlit smoke tests. Integration tests that need
generated `data/silver/` or `data/gold/` outputs skip when those are missing.

---

## Data limitations

Public sources change format and cadence, so expect drift. Notable points:

- PDF table layouts and API fields can change between publications.
- Name-based joins can collide or miss matches.
- Lobbying data before 2020 is inconsistent and filtered out at the gold layer.
- Some giant Iris PDFs bypass the split logic and are quarantined.
- Scope is the current Dáil and Seanad only.

Full detail in [`doc/DATA_LIMITATIONS.md`](doc/DATA_LIMITATIONS.md).

---

## Licensing

Two kinds of material under **different** licences — see [`NOTICE.md`](NOTICE.md)
for the full text and required attribution strings.

- **Code** — [AGPL-3.0](LICENSE). Hosted/modified deployments must publish their
  source under the same licence.
- **Data** — stays under each publishing body's licence; the AGPL grants no
  rights over it. Oireachtas, lobbying.ie, CRO, and Charities data permit reuse
  with attribution (mostly CC BY 4.0). **Iris Oifigiúil is the exception** —
  Government of Ireland copyright, not an open licence; this project extracts
  *facts* (SI numbers, dates, names) rather than redistributing verbatim text.

**Personal data:** re-publishing public figures rests on a public-interest
basis. Cross-referencing named private individuals (CRO directors, charity
trustees) is kept in the sandbox layer pending a documented lawful basis;
personal insolvency notices are excluded by policy. Nothing here is legal advice.

---

## Contributing

Open an issue or pull request. For data issues, include the source link, the
expected and observed values, and the affected dataset or script.
