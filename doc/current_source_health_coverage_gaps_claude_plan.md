# Claude Plan: Current Source Health Coverage Gaps and Index-Poller Backlog

## Purpose

Create a source-health plan for the current `dail_tracker` branch.

This file focuses only on:

- sources checked from the current branch;
- which sources already have link/API/poller health coverage;
- which sources are not yet covered by a generalized source-health system;
- which sources need index pollers, file fingerprinting, parser canaries, or manual-source freshness flags;
- how to factor existing `manifest.py` run-manifest code into the health model.

This is **not** a feature roadmap and not a new-source discovery list.

---

## Current Coverage Baseline

### Existing fixed-link endpoint checker

Checked file:

```text
pdf_endpoint_check.py
```

Current coverage:

- Dáil attendance PDFs;
- Dáil PSA/payment PDFs;
- Dáil member interests PDFs;
- Seanad member interests PDFs;
- selected manual endpoint pages:
  - https://www.oireachtas.ie/en/foi/frequently-requested-information/
  - https://www.oireachtas.ie/en/publications/?q=&topic%5B%5D=record-of-attendance
  - https://www.oireachtas.ie/en/publications/?q=&topic%5B%5D=parliamentary-allowances

What it does:

- HEAD checks hardcoded URLs;
- follows redirects;
- records final URL;
- records status;
- records content type;
- records content length;
- records Last-Modified;
- exits non-zero on broken URLs.

Gap:

- does not generalize to non-Oireachtas sources;
- does not hash file contents;
- does not run parser canaries;
- does not detect extraction drift;
- hardcoded known URLs only.

---

### Existing Oireachtas publications index poller

Checked file:

```text
oireachtas_pdf_poller.py
```

Current coverage:

- Dáil payments;
- Dáil attendance;
- Dáil interests;
- Seanad payments;
- Seanad attendance;
- likely chamber-specific filename hints through `SOURCES`.

Source pattern:

```text
https://www.oireachtas.ie/en/publications/?topic%5B%5D={slug}&resultsPerPage=50
```

Current slugs:

```text
parliamentary-allowances
record-of-attendance
register-of-members-interests
```

What it does:

- polls Oireachtas publication index pages;
- parses publication cards;
- filters by filename hints;
- downloads new PDFs;
- checks minimum file size;
- checks `%PDF-` signature;
- exits with distinct code for HTML drift when no cards are found.

Gap:

- Oireachtas-specific only;
- not a generic index poller yet;
- does not produce a unified source inventory JSON shared by all other sources.

---

### Existing run manifest

Checked file:

```text
manifest.py
```

Current capabilities:

- creates per-run manifest:
  - `logs/runs/<run_id>/manifest.json`
- creates rollup index:
  - `data/manifests/manifest.json`
- records steps, durations, exit codes, log paths, and summary lines;
- supports optional endpoint validation with:

```text
DAIL_CHECK_ENDPOINTS=1
```

Current endpoint integration:

```text
manifest._check_endpoints()
    -> imports pdf_endpoint_check
    -> runs endpoint_checker()
```

Gap:

- endpoint check is optional and PDF-specific;
- source-health artifacts are not yet first-class manifest step outputs;
- no unified per-source health state is written into manifest;
- no source registry is read by manifest.

Recommended change:

Do **not** replace `manifest.py`.

Instead:

1. Add a new source-health step that emits structured JSON.
2. Have `pipeline.py` record that step in the existing manifest.
3. Have `manifest.py` include paths to generated source-health artifacts.

Suggested artifacts:

```text
data/_meta/source_registry.yml
data/_meta/source_health.json
data/_meta/source_inventory_latest.json
data/_meta/source_snapshots/YYYY-MM-DD_source_inventory.json
```

---

## Checked Source Inventory

The sources below were checked against current branch code or metadata.

Each row states whether the source is already covered by existing monitoring, partially covered, or uncovered.

---

## Oireachtas API Sources

### Source group

```text
Oireachtas API
```

Base URL:

```text
https://api.oireachtas.ie/v1
```

Checked files:

```text
services/dail_config.py
services/urls.py
services/oireachtas_api_main.py
services/member_paginated.py
services/votes.py
services/legislation_unscoped.py
bootstrap_refresh.py
seanad_refresh.py
```

Covered endpoints / scenarios:

```text
/v1/legislation
/v1/questions
/v1/votes
/v1/debates
members/member URIs from data.oireachtas.ie
AKN debate-section URIs from data.oireachtas.ie
```

Current coverage status:

```text
PARTIALLY COVERED
```

Existing protection:

- pipeline fetches these via `services.oireachtas_api_main`;
- paginated fetching exists for questions and legislation;
- votes are fetched through `services.votes`;
- debate listings use deduplicated date/chamber worklist;
- bootstrap chain runs this before downstream chains.

Missing health checks:

- API response schema canary;
- response row-count canary by endpoint;
- latest date per endpoint;
- per-chamber vote freshness;
- per-member pagination completeness summary;
- failure count exposed in `source_health.json`.

Recommended action:

Create Oireachtas API source-health checks separate from the PDF endpoint checker.

Suggested registry entries:

```yaml
oireachtas_api_members:
  domain: members
  type: api
  base_url: https://api.oireachtas.ie/v1
  endpoints:
    - members

oireachtas_api_questions:
  domain: questions
  type: api
  base_url: https://api.oireachtas.ie/v1
  endpoints:
    - questions
  requires_pagination: true

oireachtas_api_legislation:
  domain: legislation
  type: api
  base_url: https://api.oireachtas.ie/v1
  endpoints:
    - legislation
  requires_pagination: true

oireachtas_api_votes:
  domain: votes
  type: api
  base_url: https://api.oireachtas.ie/v1
  endpoints:
    - votes
  chamber_aware: true

oireachtas_api_debates:
  domain: debates
  type: api
  base_url: https://api.oireachtas.ie/v1
  endpoints:
    - debates
  chamber_aware: true
```

Health checks:

- HTTP 200;
- required top-level keys;
- nonzero results where expected;
- latest date moves forward or remains within allowed lag;
- pagination completeness reaches `head.counts.resultCount`;
- Dáil and Seanad vote counts tracked separately.

Index poller needed:

```text
No. API canary + schema/row-count health is enough.
```

---

## Oireachtas PDF Publication Sources

### Source group

```text
Oireachtas publication PDFs
```

Checked files:

```text
pdf_endpoint_check.py
oireachtas_pdf_poller.py
bootstrap_refresh.py
seanad_refresh.py
```

Source index URLs:

```text
https://www.oireachtas.ie/en/publications/?topic%5B%5D=parliamentary-allowances&resultsPerPage=50
https://www.oireachtas.ie/en/publications/?topic%5B%5D=record-of-attendance&resultsPerPage=50
https://www.oireachtas.ie/en/publications/?topic%5B%5D=register-of-members-interests&resultsPerPage=50
```

Current coverage status:

```text
COVERED, but not in a generic registry
```

Existing protection:

- hardcoded URL endpoint checks;
- index polling for new PDFs;
- PDF size checks;
- `%PDF-` signature checks;
- HTML drift exit code.

Missing health checks:

- source inventory output;
- hash per downloaded PDF;
- parser canary after download;
- row-count expectations after parse;
- latest period by chamber;
- explicit source-health status in `source_health.json`.

Recommended action:

Keep existing Oireachtas poller.

Add output artifacts:

```text
data/_meta/source_inventory_oireachtas_pdfs.json
data/_meta/source_health_oireachtas_pdfs.json
```

Index poller needed:

```text
Already exists.
```

---

## Iris Oifigiúil

### Source group

```text
Iris Oifigiúil PDFs
```

Checked files:

```text
iris_oifigiuil_poller.py
iris_refresh.py
iris_silver_rebuild.py
si_entity_enrichment.py
public_appointments_enrichment.py
corporate_notices_enrichment.py
```

Source URL patterns:

```text
https://www.irisoifigiuil.ie/currentissues/IR{DDMMYY}.pdf
https://irisoifigiuil.ie/archive/{year}/{month_name}/IR{DDMMYY}.pdf
https://irisoifigiuil.ie/archive/{year}/{month_name}/Ir{DDMMYY}.pdf
```

Current coverage status:

```text
PARTIALLY COVERED BY DOMAIN-SPECIFIC POLLER
```

Existing protection:

- Tue/Fri expected-date enumeration;
- gap filling from latest on-disk issue;
- HEAD checks current and archive URL variants;
- PDF download size check;
- `%PDF-` signature check;
- slug-miss exit code;
- used inside `iris_refresh.py`;
- downstream steps build:
  - statutory instruments;
  - bill/SI links;
  - public appointments;
  - corporate notices;
  - SI legal state.

Missing health checks:

- issue-level source inventory;
- SHA256 per Iris PDF;
- parsed notice count per issue;
- source text parser canary;
- missed-issue health summary;
- row-count diff by notice type:
  - SI;
  - corporate;
  - appointments;
  - judicial appointments;
  - bill/SI mentions.

Recommended action:

Keep Iris poller.

Add:

```text
data/_meta/iris_source_inventory.json
data/_meta/iris_source_health.json
```

Index poller needed:

```text
No separate index poller needed because date-pattern poller exists.
```

File fingerprinting needed:

```text
Yes.
```

Parser canary needed:

```text
Yes.
```

---

## eISB Legislation Directory / SI Legal State

### Source group

```text
eISB Legislation Directory
```

Checked file:

```text
pipeline_sandbox/si_legislation_directory_extract.py
```

Base URL:

```text
https://www.irishstatutebook.ie/isbc
```

Index URL pattern:

```text
https://www.irishstatutebook.ie/isbc/si{year}.html
```

Range-page pattern:

```text
si{year}_{start}-{end}.html
```

Current coverage status:

```text
COVERED BY DOMAIN-SPECIFIC FRESHNESS GATE
```

Existing protection:

- each year index is fetched;
- `Updated to` date is read;
- year range pages are re-crawled only when the index date moves;
- HTML cached to:
  - `data/bronze/eisb_directory/`
- output:
  - `data/gold/parquet/si_current_state.parquet`
  - `data/_meta/si_current_state_coverage.json`
- tests protect parsing contract.

Missing health checks:

- source-health summary in unified `source_health.json`;
- per-year index status;
- per-year changed/not-changed state;
- parse-row count per year;
- coverage regression threshold in generic health framework.

Recommended action:

Keep current eISB freshness gate.

Add its results to unified source-health artifacts.

Index poller needed:

```text
Already exists in domain-specific form.
```

---

## Lobbying.ie API

### Source group

```text
Lobbying.ie returns API
```

Checked file:

```text
lobbying_poller.py
```

Endpoint:

```text
https://api.lobbying.ie/api/ExportReturns/Csv
```

Current coverage status:

```text
PARTIALLY COVERED
```

Existing protection:

- API request uses required filter-param schema;
- year-to-date CSV fetch;
- response size floor;
- schema/header required-column check;
- hash comparison before overwrite;
- quarantine of schema-drift or suspiciously small wave response;
- distinct exit code for human-needed state.

Known issue:

```text
NEXT_WAVE_DATE is operator-edited.
```

Missing health checks:

- derive next wave date automatically;
- current regulatory period coverage;
- latest return date;
- row count by period;
- warning if period is unexpectedly absent;
- unified source-health JSON.

Recommended action:

Add source-health output from `lobbying_poller.py`.

Index poller needed:

```text
No. API schema/period canary is needed instead.
```

---

## eTenders / OGP Open Procurement CSV

### Source group

```text
eTenders / OGP procurement open data
```

Checked file:

```text
pipeline_sandbox/procurement_etenders_extract.py
```

Landing page:

```text
https://data.gov.ie/dataset/contract-notices-published-on-etenders
```

Download URL in code:

```text
https://assets.gov.ie/static/documents/7ba65f1b/Public_Procurement_Opendata_Dataset.csv
```

Current coverage status:

```text
PARTIALLY COVERED BY EXTRACTOR, NOT GENERIC SOURCE HEALTH
```

Existing protection:

- downloads CSV if cache missing;
- schema column discovery;
- outputs:
  - `data/gold/parquet/procurement_awards.parquet`
  - `data/gold/parquet/procurement_supplier_cro_match.parquet`
  - `data/_meta/procurement_coverage.json`
- value semantics:
  - award value;
  - framework/DPS caveats;
  - `value_safe_to_sum`;
  - privacy supplier classes;
  - CRO matching.

Known issue:

```text
CACHE = Path("c:/tmp/etenders_opendata.csv")
```

Missing health checks:

- landing-page resource URL drift;
- direct download URL hash;
- CSV schema fingerprint;
- latest award/publication date;
- row-count collapse guard;
- safe-to-sum invariant;
- privacy quarantine invariant;
- source-health JSON.

Recommended action:

1. Replace Windows temp cache with repo-local or `tempfile`.
2. Add data.gov.ie landing-page poller or CKAN package probe.
3. Track resource URL and file hash.
4. Block publish if schema changes or row count collapses.

Index poller needed:

```text
Yes, preferably CKAN/data.gov.ie package/resource poller.
```

---

## TED API

### Source group

```text
TED / EU Tenders Electronic Daily
```

Checked file:

```text
pipeline_sandbox/ted_ireland_extract.py
```

API endpoint:

```text
https://api.ted.europa.eu/v3/notices/search
```

Landing page:

```text
https://ted.europa.eu/
```

Query in code:

```text
buyer-country=IRL AND notice-type=can-standard AND publication-date>=20240101
```

Current coverage status:

```text
PARTIALLY COVERED BY SILVER EXTRACTOR, NOT GENERIC SOURCE HEALTH
```

Existing protection:

- POST API request;
- raw JSON cached to:
  - `data/bronze/ted/ted_ie_awards_raw.json`
- outputs:
  - `data/silver/parquet/ted_ie_awards.parquet`
  - `data/_meta/ted_ie_awards_coverage.json`
- value-safety handling for frameworks/pan-EU outliers;
- CRO matching;
- privacy review rows.

Missing health checks:

- API status canary;
- query result count;
- schema/field availability;
- date span;
- raw JSON hash;
- value-safety invariant;
- no promotion to frontend until SQL/gold contract exists.

Recommended action:

Add TED API canary to source-health framework.

Index poller needed:

```text
No. API canary is enough.
```

---

## Public-Body / Semi-State Payments and Purchase Orders

### Source group

```text
Generic public-body PO/payment publishers
```

Checked file:

```text
pipeline_sandbox/procurement_public_body_extract.py
```

Current coverage status:

```text
PARTIALLY COVERED BY PRE-ETL EXTRACTOR, NOT GENERIC SOURCE HEALTH
```

Extractor status:

- config-driven;
- not wired into `pipeline.py`;
- writes gold-candidate/sandbox output;
- HSE/Tusla explicitly excluded to bespoke parser;
- local authority POs/payments explicitly separate;
- budget tier separate;
- emits same schema for future union.

Checked publishers in current code:

| Publisher ID | Publisher | Listing URL | Value semantics | Needs index poller? |
|---|---|---|---|---|
| `ie_opw` | Office of Public Works | https://www.gov.ie/en/office-of-public-works/collections/payments-greater-than-20000/ | `payment_actual` | Yes |
| `dept_climate` | Dept of Climate, Energy and the Environment | https://www.gov.ie/en/department-of-climate-energy-and-the-environment/collections/payments-over-20000/ | `payment_actual` | Yes |
| `dept_defence` | Department of Defence | https://www.gov.ie/en/department-of-defence/collections/purchase-orders-over-20000/ | `po_committed` | Yes |
| `dept_culture` | Department of Culture, Communications and Sport | https://www.gov.ie/en/department-of-culture-communications-and-sport/collections/purchase-orders/ | `po_committed` | Yes |
| `ie_teagasc` | Teagasc | https://www.teagasc.ie/about/corporate-responsibility/information-for-suppliers/ | `po_committed` | Yes |
| `ie_bordbia` | Bord Bia | https://www.bordbia.ie/about/governance/corporate-governance/purchase-orders/ | `po_committed` | Yes |
| `ie_bim` | Bord Iascaigh Mhara | https://bim.ie/about/corporate-governance/purchase-orders-over-20k/ | `po_committed` | Yes |
| `ie_cib` | Citizens Information Board | https://www.citizensinformationboard.ie/en/freedom_of_information/financial_information/payments_or_purchase_orders_for_goods_and_services.html | `payment_actual` | Yes |
| `ie_hea` | Higher Education Authority | https://hea.ie/about-us/public-sector-information/ | `payment_actual` | Yes |
| `ie_tii` | Transport Infrastructure Ireland | https://www.tii.ie/en/compliance/payments/ | `payment_actual` | Yes |
| `ie_revenue` | Revenue Commissioners | https://www.revenue.ie/en/corporate/statutory-obligations/freedom-of-information/section8/procurement.aspx | `payment_actual` | Yes |
| `ie_atu` | Atlantic Technological University | https://www.atu.ie/freedom-of-information/freedom-of-information-financial-information | `payment_actual` | Yes |
| `ie_nta` | National Transport Authority | https://www.nationaltransport.ie/publications/2026-purchase-orders-e20000-and-over/ | `po_committed` | Yes |
| `ie_marine` | Marine Institute | https://www.marine.ie/site-area/about-us/purchase-orders | `po_committed` | Yes |
| `ie_esbnetworks` | ESB Networks DAC | https://www.esbnetworks.ie/about-us/company/publication-scheme/financial-information | `payment_actual` | Yes |
| `ie_tailte` | Tailte Éireann | https://tailte.ie/category/publications/ | `po_committed` | Yes |
| `dept_housing` | Department of Housing, Local Government and Heritage | https://www.gov.ie/en/department-of-housing-local-government-and-heritage/collections/purchase-orders-and-payments-over-20000/ | `payment_actual` | Yes |
| `ie_cdetb` | City of Dublin ETB | https://www.cityofdublinetb.ie/about-us/finance-and-procurement/procurement/ | `po_committed` | Yes |
| `ie_enterprise_ireland` | Enterprise Ireland | https://www.enterprise-ireland.com/en/about-us/our-policies/purchase-orders-over-20000 | `po_committed` | Yes, but current caveat says clean PO file not found |

Existing protection:

- harvests landing page plus one-hop crawl;
- filters candidate files;
- parses PDF/XLSX/CSV;
- tracks per-publisher rows, files seen, files parsed, files skipped;
- records value semantics and privacy risk.

Missing health checks:

- source inventory persisted separately from extraction;
- link diff by publisher;
- file hash and Last-Modified per file;
- parser canary before full extraction;
- per-publisher stale thresholds;
- public-display privacy quarantine enforcement before promotion;
- manifest/source-health integration.

Recommended action:

Create a generic `source_index_poller.py` driven by the existing `PUBLISHERS` config.

It should emit:

```text
data/_meta/source_inventory_public_body_payments.json
data/_meta/source_health_public_body_payments.json
```

Index poller needed:

```text
Yes.
```

Priority:

```text
High.
```

---

## HSE and Tusla Bespoke Payment Parsers

### Source group

```text
HSE / Tusla public payments
```

Checked file:

```text
pipeline_sandbox/procurement_hse_tusla_parser.py
```

Current coverage status:

```text
PRE-ETL / BESPOKE PARSER, NOT SOURCE-HEALTH COVERED
```

Existing protection:

- bespoke PDF parsing by word-geometry columns;
- data-quality report;
- supplier-name quality checks;
- period coverage;
- top supplier checks;
- outlier warnings;
- duplicate content flags.

Known status:

- not wired to pipeline;
- writes to temp/probe output;
- source URL is read from a probe JSON, not first-class source registry.

Missing health checks:

- HSE payment index poller;
- Tusla payment index poller;
- source file inventory;
- direct source URLs stored in durable metadata;
- file hash;
- parser row-count threshold;
- privacy quarantine;
- promotion path to gold/silver.

Recommended action:

Create explicit HSE/Tusla source registry entries rather than relying on probe JSON.

Index poller needed:

```text
Yes.
```

Priority:

```text
Very high.
```

---

## Local-Authority Purchase Orders / Payments

### Source group

```text
31 local-authority PO/payment sources
```

Checked file:

```text
pipeline_sandbox/procurement_la_payments_extract.py
```

Current coverage status:

```text
PARTIALLY COVERED BY SELF-FETCHING EXTRACTOR, NOT GENERIC SOURCE HEALTH
```

Known source set:

The extractor is configured around all 31 Irish local authorities.

Councils to track:

```text
Carlow County Council
Cavan County Council
Clare County Council
Cork City Council
Cork County Council
Donegal County Council
Dublin City Council
Dún Laoghaire-Rathdown County Council
Fingal County Council
South Dublin County Council
Galway City Council
Galway County Council
Kerry County Council
Kildare County Council
Kilkenny County Council
Laois County Council
Leitrim County Council
Limerick City and County Council
Longford County Council
Louth County Council
Mayo County Council
Meath County Council
Monaghan County Council
Offaly County Council
Roscommon County Council
Sligo County Council
Tipperary County Council
Waterford City and County Council
Westmeath County Council
Wexford County Council
Wicklow County Council
```

Existing protection:

- one config per council in `SCHEMA_MAP`;
- per-council listing/direct URLs;
- per-council value kind:
  - `po_committed`;
  - `payment_actual`;
- raw files self-fetched to bronze;
- output:
  - `data/silver/parquet/la_payments_fact.parquet`;
- privacy quarantine for sole traders/individual/bare-ID payees;
- source hashing in row schema;
- value-kind and realisation-tier semantics.

Missing health checks:

- source inventory per council;
- latest available quarter/year per council;
- stale threshold per council;
- parser canary per council;
- row-count collapse per council;
- missing-source flag;
- public-display enforcement before UI/gold promotion.

Recommended action:

Create local-authority source-health output from existing SCHEMA_MAP.

Do not manually duplicate all council URLs in a separate YAML if they already exist in code.

Instead, add a helper:

```text
pipeline_sandbox/procurement_la_payments_extract.py --emit-source-registry
```

or a small parser that reads the config and emits:

```text
data/_meta/source_registry_la_payments.generated.json
```

Index poller needed:

```text
Yes, generated from existing SCHEMA_MAP.
```

Priority:

```text
Very high.
```

---

## Local Authority AFS / Amalgamated Annual Financial Statements

### Source group

```text
Local Authority Annual Financial Statements, amalgamated all-31-LA PDFs
```

Checked file:

```text
pipeline_sandbox/afs_amalgamated_extract.py
```

Source collection:

```text
Dept of Housing "Local Authority Annual Financial Statements" collection on gov.ie
```

Hardcoded PDF URLs currently checked:

```text
2016 https://assets.gov.ie/static/documents/local-authority-annual-financial-statement-2016-4f047a6a-642a-4ad6-88b4-b8ec07f7128f.pdf
2017 https://assets.gov.ie/static/documents/local-authority-annual-financial-statement-2017-6d7db48d-0e59-4b40-8ce5-d528e0daa390.pdf
2018 https://assets.gov.ie/static/documents/local-authority-annual-financial-statement-2018-7864f1e9-b6a6-4bb9-93ca-4cb151c86a50.pdf
2019 https://assets.gov.ie/static/documents/local-authority-annual-financial-statement-2019.pdf
2020 https://assets.gov.ie/static/documents/local-authority-annual-financial-statement-2020.pdf
2021 https://assets.gov.ie/static/documents/local-authority-annual-financial-statement-2021.pdf
2022 https://assets.gov.ie/static/documents/local-authority-annual-financial-statement-2022-45632ad8-16cd-47ae-95de-942e4e8d5265.pdf
2023 https://assets.gov.ie/static/documents/AFS_2023.pdf
```

Current coverage status:

```text
PARTIALLY COVERED BY HARD-CODED SOURCE LIST, NOT INDEX-POLLED
```

Existing protection:

- PDF download;
- page/table parser;
- division-count check;
- gross-total reconciliation;
- output:
  - `data/silver/parquet/afs_amalgamated_divisions.parquet`.

Missing health checks:

- gov.ie collection index poller for new AFS years;
- fixed PDF link health;
- file hash;
- parser canary;
- reconciliation threshold in unified health report.

Recommended action:

Add gov.ie collection index poller to find latest annual AFS PDF.

Index poller needed:

```text
Yes.
```

Priority:

```text
Medium-high.
```

---

## CBI Registers

### Source group

```text
Central Bank of Ireland registers
```

Checked file:

```text
pipeline_sandbox/cbi_registers_extract.py
```

Source URL:

```text
https://registers.centralbank.ie/downloadspage.aspx
```

Current coverage status:

```text
PARTIALLY COVERED BY BESPOKE POSTBACK EXTRACTOR, NOT GENERIC SOURCE HEALTH
```

Existing protection:

- scrapes ASP.NET postback targets;
- downloads register PDFs;
- caches PDFs;
- extracts heuristic firm rows;
- promotes only corporate-notices xref to gold;
- exact normalized-name matching for promoted corporate xref;
- known caveat that some registers fail direct postback.

Outputs:

```text
data/sandbox/_cbi_raw/*.pdf
data/sandbox/parquet/cbi_authorised_firms.parquet
data/sandbox/parquet/cbi_xref_member_interests.parquet
data/sandbox/parquet/cbi_xref_lobbying_entities.parquet
data/gold/parquet/cbi_xref_corporate_notices.parquet
data/sandbox/_cbi_meta.json
```

Missing health checks:

- postback-target inventory diff;
- per-register success/failure state;
- failed-register list in source health;
- PDF hash per register;
- parser row count per register;
- xref match-count health.

Recommended action:

Add CBI postback-target inventory to source health.

Index poller needed:

```text
Yes, but not normal HTML link scraping. It needs ASP.NET postback target inventory.
```

Priority:

```text
Medium.
```

---

## CRO Companies Register

### Source group

```text
CRO bulk companies CSV
```

Checked file:

```text
cro_normalise.py
```

Input pattern:

```text
data/bronze/cro/companies_*.csv
```

Current coverage status:

```text
MANUAL SOURCE / NOT AUTOMATED
```

Existing protection:

- latest filename picked lexically;
- schema width check;
- normalization;
- deduplication;
- output:
  - `data/silver/cro/companies.parquet`.

Missing health checks:

- source download/poller;
- source-file age;
- latest file date;
- manual-refresh warning;
- stale threshold;
- row-count drift;
- schema drift surfaced in `source_health.json`.

Recommended action:

Do not pretend CRO is automated.

Add manual-source health:

```yaml
cro_companies:
  domain: entity_resolution
  refresh_mode: manual
  input_pattern: data/bronze/cro/companies_*.csv
  stale_after_days: 45
```

Index poller needed:

```text
No, unless a reliable official bulk download endpoint is added later.
```

Priority:

```text
Very high manual-source flag.
```

---

## Charities Public Register

### Source group

```text
Charities Public Register XLSX
```

Checked file:

```text
charity_normalise.py
```

Input pattern:

```text
data/bronze/charities/public_register_*.xlsx
```

Current coverage status:

```text
MANUAL OR SEMI-MANUAL SOURCE / NOT AUTOMATED
```

Existing protection:

- latest filename picked lexically;
- reads Public Register and Annual Reports sheets;
- required column checks;
- output:
  - `data/silver/charities/register.parquet`
  - `data/silver/charities/annual_reports.parquet`
  - `data/silver/charities/charity_latest.parquet`
  - `data/silver/charities/trustees_long.parquet`.

Missing health checks:

- source download/poller;
- latest file date;
- source-file age;
- row-count drift;
- schema drift;
- manual-refresh warning.

Recommended action:

Add manual/semi-manual source-health entry.

Index poller needed:

```text
No, unless official register download URL is added.
```

Priority:

```text
High manual-source flag.
```

---

## Wikidata SPARQL — Member External Links

### Source group

```text
Wikidata member social/external links
```

Checked file:

```text
wikidata_socials_etl.py
```

Endpoint:

```text
https://query.wikidata.org/sparql
```

Current coverage status:

```text
PARTIALLY COVERED BY FETCH RETRIES, NOT GENERIC SOURCE HEALTH
```

Existing protection:

- one SPARQL query;
- retries/backoff;
- raw CSV cached:
  - `data/bronze/wikidata/member_external_links_raw.csv`;
- coverage logging by platform.

Missing health checks:

- SPARQL availability canary;
- row-count coverage by chamber;
- platform coverage drift;
- source-health JSON.

Recommended action:

Add non-critical optional-source health.

Index poller needed:

```text
No. API/SPARQL canary is enough.
```

Priority:

```text
Low-medium.
```

---

## Wikidata SPARQL — Ministerial Tenure

### Source group

```text
Wikidata ministerial tenure
```

Checked file:

```text
ministerial_tenure_build.py
```

Endpoint:

```text
https://query.wikidata.org/sparql
```

Current coverage status:

```text
PARTIALLY COVERED BY FETCH RETRIES, NOT GENERIC SOURCE HEALTH
```

Existing protection:

- SPARQL query for Irish ministerial P39 positions;
- raw CSV cached:
  - `data/bronze/wikidata/ministerial_tenure_raw.csv`;
- used for SI signatory attribution.

Missing health checks:

- SPARQL availability canary;
- row-count drift;
- unresolved minister count;
- source-health JSON.

Recommended action:

Add optional-source health; do not block full pipeline on Wikidata outage unless SI attribution requires it.

Index poller needed:

```text
No.
```

Priority:

```text
Medium.
```

---

## Electoral Commission Constituency Review / RTE Mirror PDF

### Source group

```text
Electoral Commission Constituency Review 2023 population table
```

Checked file:

```text
ec_constituency_pop_extract.py
```

Source landing page:

```text
https://www.electoralcommission.ie/publications/constituency-review-reports/
```

Fetch URL currently used:

```text
https://www.rte.ie/documents/news/2023/08/constituency-review-report-2023.pdf
```

Current coverage status:

```text
FIXED PDF WITH STRONG PARSER INTEGRITY CHECKS, NOT SOURCE-HEALTH COVERED
```

Existing protection:

- downloads fixed PDF if local cache absent;
- parses Appendix 2 page;
- checks exactly 43 constituencies;
- checks national total 5,149,139;
- checks seat sum 174;
- refuses write if checks fail.

Missing health checks:

- link health;
- file hash;
- source provenance for mirror vs official landing page;
- manual reminder if Electoral Commission source changes after boundary reviews.

Recommended action:

Add fixed-file source-health entry.

Index poller needed:

```text
No for current 2023 static source.
Yes only after next constituency review cycle.
```

Priority:

```text
Low.
```

---

## CSO / PxStat Local Finance Budget Probe

### Source group

```text
CSO PxStat local government finance / budget probe
```

Checked file:

```text
pipeline_sandbox/probe_la_finance_budget.py
```

Current coverage status:

```text
PROBE ONLY / NOT SOURCE-HEALTH COVERED
```

Known role:

- budget/expenditure macro context;
- not wired into pipeline;
- writes to temp.

Recommended action:

Keep probe-only until promoted.

If promoted:

- add PxStat API canary;
- record table IDs;
- record schema/field list;
- track latest reference year;
- do not mix budget values with spend/payments.

Index poller needed:

```text
No. PxStat API/table canary needed.
```

Priority:

```text
Low until promoted.
```

---

## SIPO OCR / One-Shot Work

### Source group

```text
SIPO OCR / election expenses / donations probes
```

Checked directory:

```text
pipeline_sandbox/
```

Relevant visible files:

```text
probe_sipo_ocr_*.py
sipo_donations_paddle_etl.py
sipo_expense_items_paddle_etl.py
sipo_expenses_paddle_etl.py
test_sipo_data_quality.py
```

Current coverage status:

```text
ONE-SHOT / PROBE ONLY
```

Recommended action:

Do not add to normal source-health cron unless promoted.

If outputs are promoted:

- mark source as one-shot;
- record source file hashes;
- record OCR confidence;
- record manual-review state;
- keep OCR dependencies out of normal CI.

Index poller needed:

```text
No, unless SIPO source feed is later promoted.
```

Priority:

```text
Low unless promoted.
```

---

## Existing Manifest Integration Plan

Do not replace the manifest model.

Current `manifest.py` should be reused.

### New source-health step

Add a new pipeline chain or bootstrap step:

```text
source_health
```

or separate tool:

```text
tools/build_source_health.py
```

This should:

1. read `data/_meta/source_registry.yml`;
2. run source checks;
3. write structured artifacts;
4. return an exit code based on severity.

Suggested outputs:

```text
data/_meta/source_health.json
data/_meta/source_inventory_latest.json
data/_meta/source_snapshots/YYYY-MM-DD_source_inventory.json
```

### Manifest fields to add per step

For any source-health step, record:

```json
{
  "step": "source_health",
  "status": "ok|warning|failed",
  "artifact_paths": [
    "data/_meta/source_health.json",
    "data/_meta/source_inventory_latest.json"
  ],
  "summary": {
    "sources_checked": 0,
    "sources_ok": 0,
    "sources_warning": 0,
    "sources_failed": 0,
    "new_files_found": 0,
    "parser_canary_failures": 0,
    "manual_sources_stale": 0
  }
}
```

This fits the existing per-run manifest model.

---

## Recommended Source Registry Shape

Create:

```text
data/_meta/source_registry.yml
```

Initial groups:

```yaml
groups:
  oireachtas_pdfs:
    owned_by: oireachtas_pdf_poller
    status: already_has_index_poller

  iris_oifigiuil:
    owned_by: iris_oifigiuil_poller
    status: already_has_date_pattern_poller

  eisb_si_directory:
    owned_by: si_legislation_directory_extract
    status: already_has_freshness_gate

  lobbying_api:
    owned_by: lobbying_poller
    status: api_canary_needed

  etenders_ogp:
    owned_by: procurement_etenders_extract
    status: landing_resource_poller_needed

  ted_api:
    owned_by: ted_ireland_extract
    status: api_canary_needed

  public_body_payments:
    owned_by: procurement_public_body_extract
    status: generic_index_poller_needed

  hse_tusla_payments:
    owned_by: procurement_hse_tusla_parser
    status: explicit_index_poller_needed

  local_authority_payments:
    owned_by: procurement_la_payments_extract
    status: generated_index_poller_needed_from_schema_map

  afs_amalgamated:
    owned_by: afs_amalgamated_extract
    status: gov_ie_collection_poller_needed

  cbi_registers:
    owned_by: cbi_registers_extract
    status: aspnet_postback_inventory_needed

  cro_companies:
    owned_by: cro_normalise
    status: manual_source_freshness_needed

  charities_register:
    owned_by: charity_normalise
    status: manual_source_freshness_needed

  wikidata:
    owned_by:
      - wikidata_socials_etl
      - ministerial_tenure_build
    status: optional_api_canary_needed

  electoral_commission_constituency_review:
    owned_by: ec_constituency_pop_extract
    status: fixed_file_health_needed
```

---

## Priority Order For Claude

### Priority 1 — Do not duplicate existing coverage

Leave these mechanisms intact:

```text
pdf_endpoint_check.py
oireachtas_pdf_poller.py
iris_oifigiuil_poller.py
si_legislation_directory_extract.py freshness gate
lobbying_poller.py schema/size/hash canary
```

Extend them by emitting source-health JSON.

---

### Priority 2 — Add generic source inventory for public-money sources

Target first:

```text
procurement_public_body_extract.py
procurement_la_payments_extract.py
procurement_hse_tusla_parser.py
```

Deliverables:

```text
data/_meta/source_inventory_public_money.json
data/_meta/source_health_public_money.json
```

Required:

- listing URL status;
- candidate file links;
- new/removed file links;
- content type;
- content length;
- file hash;
- latest period inferred;
- parser canary status.

---

### Priority 3 — Manual-source freshness flags

Target:

```text
cro_normalise.py
charity_normalise.py
```

Deliverables:

```text
data/_meta/manual_source_health.json
```

Required:

- latest local bronze file;
- date inferred from filename;
- days old;
- stale-after threshold;
- manual action required flag.

---

### Priority 4 — API canaries

Target:

```text
Oireachtas API
Lobbying.ie API
TED API
Wikidata SPARQL
CSO/PxStat if promoted
```

Deliverables:

```text
data/_meta/api_source_health.json
```

Required:

- HTTP status;
- schema keys;
- row count;
- latest date;
- response hash or raw capture hash;
- stale/failed/ok status.

---

### Priority 5 — Fixed PDF/source-file canaries

Target:

```text
Electoral Commission/RTE constituency PDF
AFS fixed PDFs
CBI downloaded PDFs
```

Deliverables:

```text
data/_meta/fixed_file_source_health.json
```

Required:

- URL status;
- SHA256;
- content type;
- content length;
- page count;
- parser canary;
- row count;
- reconciliation result.

---

## Minimal First PR

### Files to add

```text
data/_meta/source_registry.yml
tools/build_source_inventory.py
tools/build_source_health.py
test/test_source_health_registry.py
test/test_source_inventory_public_money.py
```

### Files to modify

```text
manifest.py
pipeline.py
```

### First scope

Only cover:

```text
procurement_public_body_extract.py PUBLISHERS
cro_normalise.py manual source status
charity_normalise.py manual source status
```

### Acceptance criteria

- no network tests in normal CI;
- fixtures for one listing page;
- source registry validates;
- source health JSON writes;
- manifest records artifact path;
- no data is published;
- no parser output is changed.

---

## Source Categories That Need Index Pollers

### Already has index/date poller

```text
Oireachtas publication PDFs
Iris Oifigiúil PDFs
eISB SI Legislation Directory
```

### Needs generic index poller

```text
Public-body payment/PO listing pages
Local-authority payment/PO listing pages
HSE payment/PO pages
Tusla payment/PO pages
AFS gov.ie collection page
CBI ASP.NET postback target list
```

### Does not need index poller; needs API canary

```text
Oireachtas API
Lobbying.ie API
TED API
Wikidata SPARQL
CSO/PxStat if promoted
```

### Does not need index poller; needs manual-source age check

```text
CRO companies CSV
Charities Public Register XLSX
```

### Fixed reference file; needs file hash / parser canary

```text
Electoral Commission constituency review PDF
historical AFS PDFs already hardcoded
```

---

## Final Instruction To Claude

Before implementing any new monitor, first extract the current source configs from code:

```text
oireachtas_pdf_poller.SOURCES
procurement_public_body_extract.PUBLISHERS
procurement_la_payments_extract.SCHEMA_MAP
procurement_hse_tusla_parser.SPECS
afs_amalgamated_extract.URLS
```

Then generate or validate source registry entries from those configs.

Avoid maintaining duplicate URL lists manually.

The goal is:

> Source configs should live in one place, and the health system should read or generate from them.

Do not build a parallel registry that silently diverges from the extraction code.
