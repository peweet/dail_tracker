# Dáil Tracker

[![Python](https://img.shields.io/badge/python-%3E%3D3.12-blue.svg)](https://www.python.org/)
[![License: AGPL v3](https://img.shields.io/badge/license-AGPL--3.0-green.svg)](LICENSE)
[![Commercial licence available](https://img.shields.io/badge/commercial-licence%20available-blue.svg)](COMMERCIAL-LICENSE.md)
[![Status](https://img.shields.io/badge/status-alpha-orange.svg)](https://github.com/peweet/dail_tracker)

**An Irish civic-data and public-record linkage engine.** Dáil Tracker ingests,
normalises, links, and serves public records about elected representatives,
public bodies, suppliers, companies, charities, courts, lobbying organisations,
political finance, local authorities, procurement, public payments, statutory
instruments, state appointments, and related accountability datasets.

The project's value is **cross-source linkage**, not ownership of private data.
Almost all of the underlying records are public, but they are fragmented across
parliamentary APIs, public PDFs, official registers, local-authority sites,
procurement portals, SIPO filings, Iris Oifigiúil, courts material, and
departmental publications. Dáil Tracker makes them *usable* — extracting from
PDFs, normalising names, linking entities, applying money-grain safety rules,
preserving provenance — and surfaces them as searchable dossiers, SQL views,
dashboard pages, and reviewed read-only API resources.

> **Dáil Tracker is not an official record and does not infer wrongdoing,
> influence, or causation.** It is a discovery and accountability layer: every
> claim shown to a user should trace back to a source query or public document,
> and high-stakes claims should be checked against the original source.

It is best understood as **civic / public-sector intelligence infrastructure**,
not just a visualisation app.

Inspired by [TheyWorkForYou](https://github.com/mysociety/theyworkforyou),
[How They Vote EU](https://howtheyvote.eu/), and
[OpenKamer](https://github.com/openkamer/openkamer).

---

## What it's useful for

| Use case | What you can do |
|---|---|
| **Citizen accountability** | Look up a TD/Senator and see votes, questions, speeches, attendance, payments, declared interests, committees, legislation, ministerial history, and related context in one dossier. |
| **Journalism & investigation** | Find leads across lobbying, procurement, public payments, company notices, charities, ministerial diaries, political finance, public appointments, and corporate distress. |
| **Procurement intelligence** | Explore which suppliers win awards, which bodies buy from whom, CPV/category market maps, supplier payment histories, award-vs-payment differences, TED/eTenders visibility, single-bid/competition signals, supplier↔lobbying overlap, and CRO company-status enrichment. |
| **Political finance** | Inspect SIPO party donations, GE2024 candidate expenses, party/national-agent spend, and GE2020 party spend — kept as **separate money grains** that must not be summed. |
| **Local-government accountability** | LA Annual Financial Statements, NOAC indicators and scorecards, derelict-sites levy, planning-appeal outcomes, and local-authority payment disclosures. |
| **Legal / regulatory / policy context** | Statutory instruments, bills, judicial appointments, corporate notices, Legal Diary / courts data, public appointments, and source-linked public records. |
| **Entity discovery** | Connect suppliers, companies, charities, public bodies, lobbying organisations, ministerial-diary organisations, corporate notices, and state appointments. |

Scale (gold-layer row counts, from
[`output_baseline.json`](data/_meta/output_baseline.json)): ~424k consolidated
public-payment rows, ~63k procurement-award rows, ~51k corporate notices, ~321k
parliamentary speeches, ~230k Dáil division records, ~43k ministerial-diary
engagements, ~14k enriched charities, ~6k statutory instruments.

---

## Major data domains

Ten linked domains, each with its own extractor chains, gold tables, SQL views,
and — where reviewed — an API resource:

- **Parliament & members** — TDs/Senators, votes, questions, speeches, attendance, committees, ministerial tenure, T&A allowances.
- **Interests & lobbying** — Register of Members' Interests, lobbying.ie returns, revolving-door former office-holders.
- **Companies & charities** — CRO company register and Charities Regulator finance, used to resolve and cross-reference entities.
- **Legislation & statutory instruments** — bills, amendments, SIs (2016+), and the Act→SI chain.
- **Iris Oifigiúil & corporate notices** — receiverships, wind-ups, examinerships, SCARP, public appointments, state boards.
- **Procurement** — eTenders/OGP awards, TED award + tender notices, supplier↔CRO matching, competition/single-bid signals.
- **Public payments** — consolidated >€20k PO/payment disclosures across departments, agencies, HSE/Tusla, and local authorities.
- **Political finance** — SIPO donations, GE2024 candidate + party spend, GE2020 party spend (distinct grains — and *not* election results).
- **Judiciary & courts** — bench roster, appointments/elevations, courts-health throughput, the daily Legal Diary.
- **Local government** — LA Annual Financial Statements, NOAC indicators/scorecards, derelict-sites levy, planning-appeal outcomes.

Plus specific geospatial/planning endpoints (ArcGIS/CKAN), CSO denominators, and
the monitoring layer (freshness, source-health, output-regression).

---

## Architecture

- **Python pipeline** of domain-specific refresh chains. `pipeline.py` is a thin dispatcher; each chain is a self-contained `<domain>_refresh.py` / extractor that polls → extracts → enriches and isolates its own failures.
- **Medallion data layers**: `data/bronze/` (raw inputs), `data/silver/` (cleaned), `data/gold/` (joined analytical outputs).
- **DuckDB + Parquet** as the analytical storage layer (atomic, zstd + statistics, row-floor guarded writes).
- **SQL views** (`sql_views/`) provide stable, reviewable join surfaces for the app and API.
- **Streamlit** dashboard + **FastAPI** read-only JSON API, both reading through a Streamlit-free **core query layer** (`dail_tracker_core/`).
- **Logic firewall**: pages contain no business logic — they render registered contracts and are restricted to `SELECT / WHERE / ORDER BY / LIMIT`.
- **Monitoring**: `freshness`, `source_health`, and `output_regressions` chains track data age, source reachability, silent row-thinning, schema drops, and source breakage.

Chains differ in nature, and the docs are explicit about which is which:

- some are **self-fetching and networked** (eTenders, TED, CSO, Iris, members);
- some are **deterministic transforms** over cached or committed data (participation, judiciary promote, consolidation);
- some are **no-op-safe** if a manual input is absent (disclosed-PO BigQuery drop, ministerial diaries);
- some require **manual PDF/OCR work, off-box GPU OCR, or private/manual file drops** (lobbying CSV, SIPO OCR, ministerial-diary extract);
- some datasets are **sandbox-only** and deliberately **not** promoted to gold/API.

See [`doc/SOURCES.md`](doc/SOURCES.md) for the full chain-by-chain catalogue with
status labels, and [`doc/SANDBOX_MAP.md`](doc/SANDBOX_MAP.md) for what is LIVE vs
experimental.

---

## Data sources

A summary; the authoritative, status-labelled catalogue is in
[`doc/SOURCES.md`](doc/SOURCES.md).

| Source | Used for | Format |
|---|---|---|
| [Oireachtas Open Data API](https://api.oireachtas.ie/) | Members, legislation, questions, votes, debates | API / JSON |
| [data.oireachtas.ie](https://data.oireachtas.ie/) | Attendance, allowances, Register of Members' Interests | PDF |
| Google News RSS | Per-member news-mention feed | RSS |
| [lobbying.ie](https://www.lobbying.ie/) | Lobbying returns (manual CSV export) | CSV |
| [Companies Registration Office](https://cro.ie/) | Director / supplier / shareholding cross-reference | Bulk export |
| [Charities Regulator](https://www.charitiesregulator.ie/) | Charity register + finances, trustee cross-reference | XLSX |
| [Iris Oifigiúil](https://www.irisoifigiuil.ie/) | Statutory instruments, appointments, corporate notices | PDF |
| [eTenders / OGP](https://www.etenders.gov.ie/) | Public procurement awards (supplier → CRO match) | CSV |
| [TED](https://ted.europa.eu/) | EU-journal Irish award + tender notices (silver only) | API / JSON / XML |
| [SIPO](https://www.sipo.ie/) | Party donations + GE2024 & GE2020 election spending | PDF (OCR) |
| Public-body PO / payment disclosures (>€20k) | Departments, agencies, HSE, Tusla, 31 local authorities | PDF / CSV / XLSX |
| [Courts Service](https://www.courts.ie/) | Bench roster, Legal Diary, case clearance & waiting times | PDF / HTML |
| [CSO PxStat](https://data.cso.ie/) | Government-finance & housing denominators | API / JSON |
| [Housing Agency](https://www.housingagency.ie/) | Social-housing waiting list (SSHA) | PDF |
| [Central Bank registers](https://registers.centralbank.ie/) | Regulated-entity cross-reference (sandbox) | PDF |
| [LA Annual Financial Statements](https://www.gov.ie/) | Council spend by service division | PDF |
| [NOAC](https://noac.ie/) / [DHLGH](https://www.gov.ie/) | LA accountability — collection rates, scorecard, derelict-sites levy | PDF / XLSX |
| [An Coimisiún Pleanála](https://www.pleanala.ie/) | Council vs national planning-appeal overturn | ArcGIS / API |
| [State Boards (DPER)](https://www.stateboards.ie/) | State-board roster / public-appointment cross-reference | web + curated CSV |

Upstream data stays under the publishing bodies' own licences — see
[Licensing](#licensing) and [`NOTICE.md`](NOTICE.md). Candidate future datasets
are catalogued in [`doc/ROADMAP_SOURCES.md`](doc/ROADMAP_SOURCES.md) and
[`doc/ENRICHMENTS.md`](doc/ENRICHMENTS.md).

**Editorial rule:** no inference in the app UI. Every claim shown to a user must
trace to a query; context and real-world claims appear only as cited footnotes.

### Source-status categories

The catalogue labels every source so the boundary between *built* and *aspired*
is unambiguous: `implemented`, `implemented-api-exposed`,
`implemented-not-api-exposed`, `partial`, `sandbox`, `manual-input`, `candidate`,
`not-found`, `deprecated/broken`. Notably:

- **Implemented:** members/legislation/votes, payments, interests, lobbying, Iris (SIs/corporate notices/appointments), procurement awards, the consolidated >€20k payments fact, SIPO finance, judiciary, NOAC/AFS local-government accountability, CSO denominators.
- **Partial / sandbox / manual:** TED (silver), per-LA AFS (~22/31), LA payments (20/31 parse), disclosed-PO BigQuery drop, CBI registers (sandbox), council minutes (sandbox), judiciary (promoted from a static sandbox), lobbying acquisition (manual CSV), SIPO/diary OCR.
- **Not built:** **actual election results** (counts/seats/turnout — *distinct from* the implemented SIPO finance), **C&AG audit reports**, **OGP central-framework catalogue** (*distinct from* implemented OGP awards), a **general data.gov.ie crawler**, FOI/OIC decisions, most regulatory-enforcement feeds, RBO, Companies House UK.

---

## Money grains — read before summing anything

**Money fields are not interchangeable.** Award values, framework/DPS ceilings,
call-offs, PO commitments, actual payments, audited expenditure, election
expenses, donations, and parliamentary allowances are different *grains*. Sum
values **only** where a view explicitly marks them safe (`value_safe_to_sum`).

- Award value ≠ money paid; framework ceiling ≠ drawdown; "ordered" ≠ "paid".
- Do **not** union or sum eTenders and TED (siblings, ~66% overlap by name).
- Do **not** add public-body payments to procurement awards.
- Do **not** add SIPO donations to election expenses, or sum GE2020 with GE2024.
- Naive sums are wildly wrong (eTenders ~€649bn naive vs ~€15.6bn sum-safe; TED ~€624bn naive is meaningless — trust count + median).

Full definitions, magnitudes, and the disclosure-regime table are in
[`doc/DATA_GRAINS.md`](doc/DATA_GRAINS.md).

---

## Pipeline

```bash
python pipeline.py                       # full refresh (all chains)
python pipeline.py --list                # show chains and exit
python pipeline.py --select iris         # run one chain
python pipeline.py --select members,iris # a comma-separated subset
python pipeline.py --exclude lobbying    # run everything except one
```

The core domain chains are also standalone scripts (`<domain>_refresh.py`) you
can run directly. Per-chain logs land at `logs/runs/<run_id>/steps/`; a manifest
records each chain; chain-level try/except keeps one flaky source from poisoning
the rest, and the end-of-run summary lists which chains failed.

#### Running a refresh yourself (Windows)

```powershell
.venv\Scripts\python.exe pipeline.py            # full refresh (~30 sources, runs a while)
.venv\Scripts\python.exe pipeline.py --list     # see chains without running
.venv\Scripts\python.exe pipeline.py --select procurement,ted   # just a subset
```

Watch progress (or diagnose a failure) in the per-run logs:

```powershell
Get-ChildItem logs\runs | Sort-Object LastWriteTime -Desc | Select-Object -First 1
Get-Content logs\runs\<run-id>\steps\01_bootstrap.log -Tail 30 -Wait
```

The chain groups, in run order — **bootstrap runs first; every chain assumes it
has** (full descriptions in [`doc/SOURCES.md`](doc/SOURCES.md)):

- **Core domains:** `bootstrap`, `members`, `news_mentions`, `payments`, `attendance`, `seanad`, `member_contact`, `interests`, `lobbying`, `iris`, `legislation`.
- **Participation & committees:** `participation`, `committee_evidence` / `committee_evidence_promote`.
- **Courts & judiciary:** `judiciary_bench`, `legal_diary_poller`, `legal_diary_extract`, `judiciary_diary_link`.
- **Cross-reference & enrichment:** `afs`, `cbi`, `cro`, `corporate_receiver`, `procurement`, `procurement_lobbying`, `ted` / `ted_tenders`, `cso`, `stateboards`.
- **Public payments (privacy-gated):** `public_body_payments`, `hse_tusla_payments`, `disclosed_bq_po` (+ `_newbodies`), `la_payments` → `procurement_consolidate` → `ministerial_diaries`.
- **Local-authority accountability:** `noac_collection`, `noac_scorecard` / `_history` / `noac_indicators`, `derelict_sites`, `planning_appeal_outcomes`.
- **Monitoring (read-only):** `freshness`, `source_health`, `output_regressions`.

### Joins

There's no shared person identifier across sources. Where the source carries the
Oireachtas member code it's used directly as `unique_member_code`; otherwise the
view layer joins on a normalised name (see
[`shared/normalise_join_key.py`](shared/normalise_join_key.py)). Collisions and
missed matches remain possible and are documented as data limitations — the join
key is an engineering compromise, not a legal identity key.

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

A Streamlit app with **27 pages** across **8 top-nav sections** — *What They Own,
Your Area, Members & Parliament, The Money, Law & Records, Influence, Planning,
Glossary*. Pages read **only** through registered DuckDB views; all
transformation lives in the pipeline, SQL views, or gold layer. Query and
composition logic lives in `dail_tracker_core`, which both the dashboard and the
JSON API read. The full page list is in
[`doc/SOURCES.md`](doc/SOURCES.md#data-products--app-surfaces).

**Stack:** DuckDB (analytical layer), Polars (ETL), pandas (UI layer only), SQL
views (reviewable joins).

## JSON API

```bash
uv sync --extra api
uvicorn api.main:app
```

A read-only FastAPI app over the same data — open, no API key, versioned under
`/v1`, interactive docs at `/docs`, served under **CC-BY-4.0**. `/v1/catalog`
lists the published resources and `/v1/coverage` states scope, year ranges, and
money-grain rules. Routers are thin (parse → core → serialize); all retrieval
lives in `dail_tracker_core`.

> **The API catalogue is intentionally curated, not an auto-dump of every
> view/parquet.** Some underlying tables carry PII (SIPO donor addresses,
> personal insolvency) or are sandbox-only / experimental and must not be exposed
> without review. Only resources with a dedicated, reviewed endpoint appear. The
> 16 published resources are listed in
> [`doc/SOURCES.md`](doc/SOURCES.md#api-resources).

---

## Data quality, caveats, and trust boundaries

This is a discovery and accountability layer, not an authority on any single
figure. Notable boundaries (full detail in
[`doc/DATA_LIMITATIONS.md`](doc/DATA_LIMITATIONS.md) §1–26 and
[`doc/DATA_GRAINS.md`](doc/DATA_GRAINS.md)):

- The project is **not an official record**; the upstream publisher is authoritative.
- Much data is **extraction-derived from PDFs**, so aggregates are a **floor** ("at least €Y, from the documents we could read"), and PDF/OCR extraction can fail silently when layouts change.
- **Name-based joins** can collide or miss (CRO supplier match ~46–61%).
- **CRO / lobbying / charity / supplier joins are leads, not proof.** Lobbying overlap is **co-occurrence, not causation**. A ministerial-diary meeting is **access, not proof of influence**. A single-bid signal is a **market signal, not a verdict**.
- **OCR figures are not authoritative** without source verification (SIPO and diary rows carry explicit "verify against the PDF (page N)" caveats).
- Some sources are **incomplete, sandboxed, manual, or no-op-safe**; some rows are **privacy-quarantined**; some personal data must **never** be exposed through the API/catalogue.
- Cross-reference panels naming **private individuals** (CRO directors, charity trustees, CBI-register firms) sit in the **sandbox layer, not gold**; personal-insolvency notices are excluded by policy.

---

## Roadmap

Data lands in the pipeline ahead of the UI, so several datasets exist in
gold/silver before they get a dedicated page.

- **Government-finance denominators** — the `cso` chain lands `v_gov_finance_annual`; a dedicated national government-finance page is still to come ([`doc/PER_LA_AFS_BUILD_PLAN.md`](doc/PER_LA_AFS_BUILD_PLAN.md)).
- **GE2024 candidate expenses** — 473 of ~614 sourced candidates extracted; OCR of the remainder in progress, coverage shown in-app.
- **Bulk data exports** — parquet downloads of the gold layer via the API (demand-gated).
- **Scheduled data refresh** — read-only canaries already run on GitHub Actions; a fully automated cloud refresh of the data itself is the remaining step.

Candidate future datasets, the monetization analysis, and the workflow-led
product direction are in [`doc/ROADMAP_SOURCES.md`](doc/ROADMAP_SOURCES.md).

---

## Commercial use

The free/public side is strongest as a **civic transparency** tool. Its most
commercial surface is **procurement and public-sector intelligence** — the joins
between suppliers, buyers, CPV/category markets, awards, public-body payments,
and lobbying/company/charity overlaps that no incumbent assembles in one place.

A focused business-intelligence offering could support: supplier and
buyer/public-body dossiers; CPV/category market maps; inflation-adjusted
historical benchmarks; tender and renewal alerts; competitor monitoring;
bid-intelligence summaries; and exports / an authenticated API tier.

> **Honest limits.** It does **not** calculate a profitable or winning bid price
> and **cannot replace quantity-surveyor / estimator work.** It supports
> *bid/no-bid* decisions and historical market research, not pricing. The
> dual-licence (below) exists for closed-source / SaaS use; the full
> commercialisation analysis is in
> [`doc/ROADMAP_SOURCES.md`](doc/ROADMAP_SOURCES.md#monetization--commercial-usefulness)
> and [`doc/COMMERCIALISATION_PLAN.md`](doc/COMMERCIALISATION_PLAN.md).

---

## Testing

```bash
uv sync --extra pipeline --group dev
uv run pytest test/ -v
uv run ruff check .
```

Tests cover join-key normalisation, silver/gold output shape, SQL view
registration, API endpoints (FastAPI TestClient), and lightweight Streamlit smoke
tests. Integration tests that need generated `data/silver/` or `data/gold/`
outputs skip when those are missing. Testing is partial coverage by design — it
catches obvious regressions, it does not prove every extracted row is correct.

CI (GitHub Actions) runs lint, the logic-firewall check, typecheck, tests, and
SQL-view contracts on every push; scheduled workflows act as canaries — source
health, data freshness, lobbying-register freshness, and a full pipeline probe.

---

## Licensing

Two kinds of material under **different** licences — see [`NOTICE.md`](NOTICE.md)
for the full text and required attribution strings.

- **Code** — **dual-licensed**: [AGPL-3.0](LICENSE) (free) *or* a [commercial licence](COMMERCIAL-LICENSE.md). Under the AGPL, hosted/modified deployments must publish their source under the same licence; a commercial licence waives that for closed-source / SaaS use. Contributions are accepted under the [`CLA.md`](CLA.md), which keeps this dual-licensing possible.
- **Data** — stays under each publishing body's licence; the AGPL grants no rights over it. Oireachtas, lobbying.ie, CRO, and Charities data permit reuse with attribution (mostly CC BY 4.0). **Iris Oifigiúil is the exception** — Government of Ireland copyright, not an open licence; this project extracts *facts* (SI numbers, dates, names) rather than redistributing verbatim text.

**Personal data:** re-publishing public figures rests on a public-interest basis.
Cross-referencing named private individuals (CRO directors, charity trustees) is
kept in the sandbox layer pending a documented lawful basis; personal-insolvency
notices are excluded by policy. Nothing here is legal advice.

---

## Contributing

Open an issue or pull request — see [`CONTRIBUTING.md`](CONTRIBUTING.md). For data
issues, include the source link, the expected and observed values, and the
affected dataset or script. Because the project is dual-licensed, code
contributions are accepted under the [Contributor Licence Agreement](CLA.md).

For commercial-licensing enquiries, see
[`COMMERCIAL-LICENSE.md`](COMMERCIAL-LICENSE.md).
