# Dáil Tracker

[![Python](https://img.shields.io/badge/python-%3E%3D3.12-blue.svg)](https://www.python.org/)
[![License: AGPL v3](https://img.shields.io/badge/license-AGPL--3.0-green.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-alpha-orange.svg)](https://github.com/peweet/dail_tracker)

A civic-data pipeline for Irish parliamentary transparency.

Dáil Tracker ingests, joins, and analyses public records about Irish elected
representatives and the state around them — attendance, legislation, questions,
votes, payments, declared interests, lobbying contacts, procurement awards,
political finance, and judicial appointments — and presents them in one
searchable dashboard and a read-only JSON API.

The point of difference is **cross-source linkage**: declared interests joined
against the Companies Registration Office, the charities register, and Iris
Oifigiúil notices, so a TD's public footprint can be checked across sources
programmatically rather than by hand. No other Irish civic-tech tool does this.

The member-level pipeline centres on the current Dáil and Seanad (with historic
interests backfilled for the "What They Own" record), alongside state-level
accountability datasets — procurement, public payments, courts, corporate
notices, and local-authority finance — that are not member-scoped. See
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
| [TED](https://ted.europa.eu/) | EU-journal Irish award + tender notices (silver only) | API / JSON / XML |
| [SIPO](https://www.sipo.ie/) | Party donations + GE2024 election spending | PDF (OCR) |
| Public-body PO / payment disclosures | Payments over €20k — departments, agencies, HSE, Tusla, 31 local authorities | PDF / CSV / XLSX |
| [Courts Service](https://www.courts.ie/) | Bench roster, Legal Diary, case clearance & waiting times | PDF / HTML |
| [CSO PxStat](https://data.cso.ie/) | Government-finance & housing denominators | API / JSON |
| [Central Bank of Ireland registers](https://registers.centralbank.ie/) | Regulated-entity cross-reference for corporate notices | PDF |
| [Local Authority Annual Financial Statements](https://www.gov.ie/) | Council spend by service division | PDF |
| [NOAC](https://noac.ie/) / [DHLGH](https://www.gov.ie/) | Local-authority accountability — revenue-collection rates, derelict-sites levy | PDF / XLSX |
| [An Coimisiún Pleanála](https://www.pleanala.ie/) | Council vs national planning-appeal overturn rates | ArcGIS / API |
| [State Boards (DPER)](https://www.stateboards.ie/) | State-board roster / public-appointment cross-reference | CSV / web |

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

#### Running a refresh yourself (Windows)

From the repo root, use the project venv's interpreter directly — a full run
hits ~30 network sources and takes a while, so let it run to completion:

```powershell
.venv\Scripts\python.exe pipeline.py            # full refresh
.venv\Scripts\python.exe pipeline.py --list     # see chains without running
.venv\Scripts\python.exe pipeline.py --select procurement,ted   # just a subset
```

Watch progress (or diagnose a failure) in the per-run logs — each run gets its
own timestamped directory under `logs/runs/`, with one log per chain:

```powershell
# newest run dir:
Get-ChildItem logs\runs | Sort-Object LastWriteTime -Desc | Select-Object -First 1
# then tail a chain, e.g.:
Get-Content logs\runs\<run-id>\steps\01_bootstrap.log -Tail 30 -Wait
```

The end-of-run summary lists which chains succeeded and which failed; a single
failed chain doesn't abort the rest. `manifest.json` in the run dir records the
exit code, summary line, and error for every chain.

The chains, in run order:

- **Core domains:** `bootstrap` (PDF/API polling + member & debate flattening),
  `members` (Wikidata + ministerial tenure + committees), `payments`,
  `attendance`, `seanad` (parity, reuses the Dáil parsers), `interests`,
  `lobbying` (+ CRO + charities join), `iris` (SIs + appointments + corporate
  notices), and `legislation` (bills + questions + amendments + votes).
- **Courts & judiciary:** `judiciary_bench` (promote validated bench / appointments
  / clearance / waiting roster to gold), `legal_diary_poller` (archive the daily
  Legal Diary `.docx`), `legal_diary_extract` (privacy-tiered schedule / counts /
  cases), and `judiciary_diary_link` (map surname-only diary judges to the bench).
- **Cross-reference & enrichment:** `afs` (LA financial statements → spend by
  division), `cbi` (Central Bank register ↔ corporate notices), `cro` (CRO
  company register ↔ corporate notices), `procurement` (eTenders awards +
  supplier→CRO match), `procurement_lobbying` (supplier ↔ lobbying overlap),
  `ted` / `ted_tenders` (EU award and tender notices, silver only), `cso`
  (PxStat government-finance & housing denominators), and `stateboards` (DPER
  state-boards roster + body universe).
- **Public payments (privacy-gated):** `public_body_payments` (departmental and
  agency PO/payment disclosures over €20k), `hse_tusla_payments`, and
  `la_payments` (31 local authorities) feed three schema-aligned fact tables,
  which `procurement_consolidate` folds into one listing-rot-guarded gold
  `procurement_payments_fact`.
- **Local-authority accountability:** `noac_collection` (NOAC per-LA
  revenue-collection rates), `derelict_sites` (DHLGH derelict-sites levy levied /
  collected / outstanding), and `planning_appeal_outcomes` (council vs An
  Coimisiún Pleanála overturn rate).
- **Monitoring (read-only, never mutate pipeline data):** `freshness`
  (data-age signal per domain → `data/_meta/freshness.json`), `source_health`
  (per-source staleness / listing reachability →
  `data/_meta/source_health.json`), and `output_regressions` (gold row/column
  drop vs baseline → `data/_meta/output_regressions.json`).

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

A Streamlit app (titled *Oireachtas Explorer*) with twenty-three pages across
seven top-level sections:

- **What They Own** — the plain-language front door to the Register of Members'
  Interests (property, shares, companies) across the whole record, sitting and
  former members.
- **Your Area** — Constituencies, Who Runs Your County (local-authority chief
  executives + accountability indicators), and Housing.
- **Members & Parliament** — a Home landing view, Member Overview (the canonical
  TheyWorkForYou-style profile), Attendance, Votes, and Committees.
- **The Money** — Payments, Election 2024 (SIPO donations + campaign spending),
  Procurement, Council Spending, Accommodation Spend, Public Payments, and
  Companies (the entity-first firm dossier).
- **Law & Records** — Legislation, Statutory Instruments, Corporate Notices, and
  Courts & Judiciary (bench roster, appointments, court performance, Legal Diary).
- **Influence** — Lobbying, Who Ministers Meet (ministerial diaries × lobbying),
  and Appointments.
- **Glossary**.

Streamlit is a thin presentation layer: pages read **only** through registered
DuckDB views and are restricted to `SELECT / WHERE / ORDER BY / LIMIT`. All
transformation lives in the pipeline, SQL views (`sql_views/`), or gold layer.
Query and composition logic lives in `dail_tracker_core`, a Streamlit-free core
that both the dashboard and the JSON API read through. Each page has a YAML
contract in
[`dail_tracker_bold_ui_contract_pack_v5/`](dail_tracker_bold_ui_contract_pack_v5/).

**Stack:** DuckDB for the analytical layer, Polars for ETL, pandas in the
Streamlit layer, SQL views for reviewable joins.

## JSON API

```bash
uv sync --extra api
uvicorn api.main:app
```

A read-only FastAPI app over the same data — open, no API key, versioned under
`/v1`, interactive docs at `/docs`. Routers cover members, legislation, votes,
payments, public payments, lobbying, procurement, committees, ministerial
tenure, political finance, judiciary, charities, appointments, and bulk exports;
`/v1/catalog` lists every served dataset. Routers are thin (parse → core →
serialize) — all retrieval lives in `dail_tracker_core`. Data is served under
CC-BY 4.0, mirroring the Oireachtas PSI licence. Design rationale in
[`doc/API_LAYER_PLAN.md`](doc/API_LAYER_PLAN.md).

---

## Roadmap

Active and planned work — data already lands in the pipeline ahead of the UI, so
several datasets exist in gold/silver before they get a dedicated page.

- **Government-finance denominators** — the `cso` chain lands government-finance
  aggregates (`v_gov_finance_annual`); the council spend-by-division (`afs`),
  collection-rate, and derelict-sites data now surface on the Council Spending
  and Who Runs Your County pages, with per-capita denominator usage still to come.
  Plan in [`doc/PER_LA_AFS_BUILD_PLAN.md`](doc/PER_LA_AFS_BUILD_PLAN.md).
- **GE2024 candidate expenses** — per-candidate expense statements already show
  on the Election 2024 page, with 473 of the 614 sourced candidates extracted so
  far; OCR of the remainder is in progress and the coverage gap is shown
  in-app, never implied complete.
- **Bulk data exports** — parquet downloads of the gold layer via the API
  (Phase 2 of [`doc/API_LAYER_PLAN.md`](doc/API_LAYER_PLAN.md)), demand-gated.
- **Scheduled data refresh** — read-only canaries already run on GitHub Actions
  (source health, freshness, a full pipeline probe); a fully automated cloud
  refresh of the data itself is the remaining step.

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
registration, API endpoints (FastAPI TestClient), and lightweight Streamlit
smoke tests. Integration tests that need generated `data/silver/` or
`data/gold/` outputs skip when those are missing.

CI (GitHub Actions) runs lint, the logic-firewall check, typecheck, tests, and
SQL-view contracts on every push; scheduled workflows act as canaries — source
health, data freshness, lobbying-register freshness, and a full pipeline probe.

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
