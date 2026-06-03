# Procurement Semi-State and Public-Body Expansion Plan

**Project:** Dáil Tracker  
**Audience:** Claude / coding assistant / future implementation agent  
**Status:** Planning document  
**Recommended location:** `doc/PROCUREMENT_SEMISTATE_EXPANSION_PLAN.md`

---

## 1. Purpose

This document is a Claude-ready implementation plan for expanding Dáil Tracker’s procurement and public-payments work beyond the current eTenders and local-authority exploration.

The aim is **not** to immediately build a finished Streamlit page.

The aim is to:

1. keep the work inside `pipeline_sandbox/` until it is proven;
2. identify and probe semi-state, state-body, public-agency, health, education, and other public-body procurement/payment sources;
3. validate whether each source has usable supplier-level data, category-level data, annual-report-only data, or only FOI/AIE clues;
4. build high-fidelity, source-linked, caveated gold parquet output only after probes prove the data is worth promoting;
5. eventually expose the resulting data on a dedicated Procurement page and in cross-enrichment panels.

The expected end state is:

```text
publisher/source registry
→ probe scripts
→ source/file inventory
→ sample extraction reports
→ schema/quality assessment
→ reviewed parser(s)
→ high-fidelity gold parquet
→ SQL views
→ tests
→ optional Streamlit Procurement page
→ cross-enrichment into Lobbying / Corporate / Member Overview
```

---

## 2. Current Project Context

The project already has procurement work on the `seanad-app-parity` branch.

Known current procurement files include:

```text
pipeline_sandbox/procurement_etenders_extract.py
pipeline_sandbox/procurement_lobbying_xref.py
pipeline_sandbox/procurement_la_seed.py
pipeline_sandbox/procurement_la_registry.py

data/gold/parquet/procurement_awards.parquet
data/gold/parquet/procurement_lobbying_overlap.parquet

data/_meta/procurement_coverage.json
data/_meta/procurement_lobbying_overlap_coverage.json

sql_views/procurement_awards.sql
sql_views/procurement_lobbying_overlap.sql
sql_views/lobbying_org_procurement.sql
```

The project already does several important things correctly:

- distinguishes awarded value from actual spend;
- avoids treating framework/DPS ceilings as safe totals;
- carries `value_safe_to_sum`;
- classifies suppliers;
- quarantines likely sole traders / individuals;
- matches company-class suppliers to CRO where possible;
- cross-links procurement suppliers to lobbying organisations/clients;
- explicitly caveats that procurement/lobbying co-occurrence is not evidence of influence.

Do **not** duplicate or replace this work without inspecting it first.

---

## 3. Important Architectural Constraints

Claude should follow these constraints.

### 3.1 Keep discovery work in `pipeline_sandbox/`

New probes and source registries should start in:

```text
pipeline_sandbox/
```

or as seed metadata under:

```text
data/_meta/procurement_publishers/
```

Do not wire new procurement-source classes into `pipeline.py` until:

- source coverage is known;
- extraction is repeatable;
- privacy risks are assessed;
- schema is stable;
- gold parquet output is validated;
- tests exist.

### 3.2 Probe before ETL

Do not create a large ETL script first.

Start with small probe scripts that answer:

```text
Does this source exist?
Can files be enumerated?
What formats are used?
Are files PDF, XLSX, CSV, HTML, or mixed?
Are PDFs digital or scanned?
Is OCR needed?
Are rows supplier-level, category-level, payment-level, purchase-order-level, or annual-summary only?
Is supplier name present?
Is amount present?
Is period/date present?
Are caveats present?
Are personal names / sole traders present?
Can the source be parsed safely?
Does the source overlap with CRO, lobbying, corporate notices, or eTenders?
Are there strange values, duplicate totals, negative amounts, zero amounts, VAT caveats, paid flags, or repeated framework-style values?
```

Only after these are answered should a real parser be built.

### 3.3 Prefer high-fidelity gold parquet over UI rush

The end result of this phase is **not necessarily a finished app page**.

The desired deliverable is a high-quality gold table such as:

```text
data/gold/parquet/public_payments_fact.parquet
```

or a more scoped first table such as:

```text
data/gold/parquet/procurement_semistate_payments.parquet
data/gold/parquet/procurement_public_body_payments.parquet
```

The Streamlit page can come later.

### 3.4 Preserve provenance

Every extracted row should retain enough information to verify the source:

```text
publisher_name
publisher_type
source_landing_url
source_file_url
source_file_name
source_file_hash
period
year
quarter
source_row_number
source_page_number, where applicable
parser_name
parser_version
downloaded_at
extraction_status
extraction_confidence
source_caveat
```

### 3.5 Avoid unsafe claims

The UI and metadata must not imply more than the source supports.

Avoid unsafe wording:

```text
government spend
paid because of lobbying
corruption
influence
confirmed contract relationship
distressed supplier
```

Prefer cautious wording:

```text
public purchase-order/payment record
appears in procurement/payment file
supplier name match
company-class supplier
co-occurrence only
awarded value, not spend
purchase order, not necessarily payment
source-linked record
verify with source
```

### 3.6 Do not publish personal names casually

If a supplier appears to be:

```text
individual
sole trader
unincorporated personal name
employee-like payment
```

then default to:

```text
privacy_status = quarantined
public_display = false
```

unless there is a clear public-interest reason and a documented policy.

---

## 4. External Source Classes to Investigate

Do not assume all sources are equally usable. Classify each source honestly.

### 4.1 Source status enum

Use this enum in seed registries:

```text
CONFIRMED_SUPPLIER_LEVEL
CONFIRMED_CATEGORY_LEVEL
CONFIRMED_PUBLIC_CONTRACTS_ONLY
ANNUAL_REPORT_ONLY
FOI_AIE_CONFIRMED_EXISTS
FOI_CLUE_ONLY
NEEDS_MANUAL_CHECK
NOT_FOUND
NOT_APPLICABLE
```

### 4.2 Format enum

Use this enum where possible:

```text
PDF_DIGITAL
PDF_SCANNED
XLSX
CSV
HTML_TABLE
LOOKER_OR_DASHBOARD_ONLY
ANNUAL_REPORT_PDF
MIXED
UNKNOWN
```

### 4.3 Grain enum

Use this enum where possible:

```text
payment
purchase_order
contract_award
framework_ceiling
category_total
annual_summary
mixed
unknown
```

---

## 5. Source Categories

### 5.1 Semi-State / Commercial State Bodies

Create a seed registry for semi-state bodies.

Suggested file:

```text
pipeline_sandbox/procurement_semistate_seed.py
```

or, if using CSV first:

```text
data/_meta/procurement_publishers/semistate_seed.csv
```

Suggested sectors:

```text
transport
energy_utilities
forestry_land
agri_food_marine
media_culture
enterprise_tourism
property_land
```

Bodies to investigate include:

```text
CIÉ
Iarnród Éireann
Bus Éireann
Dublin Bus
daa
Dublin Port
Shannon Foynes Port
Port of Cork

ESB
ESB Networks DAC
EirGrid
Gas Networks Ireland
Uisce Éireann
Bord na Móna
Coillte

Teagasc
Bord Bia
BIM

RTÉ
TG4

Enterprise Ireland
IDA Ireland
Fáilte Ireland
Tourism Ireland
```

Known starting links / clues from prior research:

```text
Teagasc:
https://teagasc.ie/about/corporate-responsibility/information-for-suppliers/

Bord Bia:
https://www.bordbia.ie/about/governance/corporate-governance/purchase-orders/

BIM:
https://bim.ie/about/corporate-governance/purchase-orders-over-20k/

RTÉ:
https://about.rte.ie/procurement-2/

ESB Networks DAC:
https://www.esbnetworks.ie/about-us/company/publication-scheme/financial-information

Iarnród Éireann FOI publication scheme PDF clue:
https://www.irishrail.ie/_kontent/d2e85689-fbf9-010a-a02e-587ed94b5032/480d2a0d-cd41-430d-956f-a64b2f5d6e8a/025b45de-29cc-4f2c-b5b4-184ba73ef865.pdf

Bord na Móna / Coillte AIE or OCEI clues:
https://ocei.ie/
```

Important distinction:

- Some bodies publish supplier-level rows.
- Some publish category-level totals only.
- Some only mention procurement in annual reports.
- Some appear to have data only through FOI/AIE/OCEI decisions.

Do not collapse these into the same data product.

---

### 5.2 OPW and Property / Land Bodies

OPW and Tailte Éireann are high-value state-body sources and should be included early.

Known links:

```text
OPW payments greater than €20,000:
https://www.gov.ie/en/office-of-public-works/collections/payments-greater-than-20000/

Tailte Éireann publications archive:
https://tailte.ie/category/publications/
```

Tailte may include:

```text
purchase orders over €20,000
prompt-payment reports
public contracts over €25,000
annual reports
FOI disclosure logs
```

OPW likely has a clean `gov.ie` collection with quarterly files.

Recommended seed:

```text
data/_meta/procurement_publishers/property_land_seed.csv
```

---

### 5.3 Central Government Departments

Departments should be their own seed registry.

Suggested file:

```text
data/_meta/procurement_publishers/departments_seed.csv
```

Known links:

```text
Department of Finance:
https://www.gov.ie/en/department-of-finance/collections/purchase-orders/

Department of Climate, Energy and the Environment:
https://www.gov.ie/en/department-of-climate-energy-and-the-environment/collections/payments-over-20000/

Department of Housing:
https://www.gov.ie/en/department-of-housing-local-government-and-heritage/organisation-information/procurement/

Department of Children, Disability and Equality:
https://www.gov.ie/en/department-of-children-disability-and-equality/collections/department-of-children-equality-disability-integration-and-youth-purchase-orders-for-20000-or-above/

Department of Enterprise, Tourism and Employment:
https://enterprise.gov.ie/en/publications/payments-over-20k.html

Department of Defence:
https://www.gov.ie/en/department-of-defence/collections/purchase-orders-over-20000/

Department of Justice:
https://www.gov.ie/en/department-of-justice-home-affairs-and-migration/collections/department-of-justice-purchase-orders-issued-over-20000-in-value/

Department of Public Expenditure / OGCIO / OGP:
https://www.gov.ie/en/department-of-public-expenditure-infrastructure-public-service-reform-and-digitalisation/collections/dpendr-ogcio-and-ogp-purchase-order-payments-2024/

Department of Culture, Communications and Sport:
https://www.gov.ie/en/department-of-culture-communications-and-sport/collections/purchase-orders/

IPAS payments:
https://www.gov.ie/en/international-protection-accommodation-services-ipas/publications/facts-and-figures/
```

---

### 5.4 Agencies, Regulators, and Statutory Bodies

Suggested file:

```text
data/_meta/procurement_publishers/agencies_seed.csv
```

Known links:

```text
National Transport Authority:
https://www.nationaltransport.ie/publications/2026-purchase-orders-e20000-and-over/

Transport Infrastructure Ireland:
https://websitecms.tii.ie/en/compliance/payments/

Citizens Information Board:
https://www.citizensinformationboard.ie/en/freedom_of_information/financial_information/payments_or_purchase_orders_for_goods_and_services.html

Health and Safety Authority:
https://www.hsa.ie/eng/about_us/public_sector_information/purchase_orders_in_excess_of_-20_000/

Marine Institute:
https://marine.ie/site-area/about-us/purchase-orders

Revenue:
https://www.revenue.ie/en/corporate/statutory-obligations/freedom-of-information/section8/procurement.aspx

Tusla:
https://www.tusla.ie/about/your-personal-information/new-freedom-of-information/financial-information/

Road Safety Authority:
https://www.rsa.ie/about/reporting

Screen Ireland:
https://www.screenireland.ie/about/policies/purchase-orders-for-20000-or-above/2025

Arts Council:
https://artscouncil.ie/
```

Do not assume every page has the same format.

---

### 5.5 Health Bodies and Hospitals

Suggested file:

```text
data/_meta/procurement_publishers/health_seed.csv
```

Known links:

```text
HSE procurement:
https://healthservice.hse.ie/staff/information-healthcare-workers/procurement/

National Treatment Purchase Fund:
https://www.ntpf.ie/

HIQA:
https://www.hiqa.ie/

Tallaght University Hospital:
https://www.tuh.ie/

St Vincent’s University Hospital:
https://www.stvincents.ie/about-us/financial-statements/
```

Health spend is high public-interest and likely supplier-rich.

Treat hospital data carefully because formats may be inconsistent and may include private suppliers, individual practitioners, or special categories requiring caution.

---

### 5.6 Education, Higher Education, and ETBs

Suggested file:

```text
data/_meta/procurement_publishers/education_seed.csv
```

Known links:

```text
Higher Education Authority:
https://hea.ie/about-us/public-sector-information/

Atlantic Technological University:
https://www.atu.ie/freedom-of-information/freedom-of-information-financial-information

Laois & Offaly ETB:
https://loetb.ie/organisation-support-development/finance/purchase-orders-over-20000/

Louth & Meath ETB:
https://www.lmetb.ie/category/finance/purchase-orders-over-e20000/

City of Dublin ETB:
https://www.cityofdublinetb.ie/about-us/finance-and-procurement/procurement/

National Council for Special Education:
https://ncse.ie/
```

---

### 5.7 Local Authorities

The local-authority sources have already been seeded/probed.

Relevant files:

```text
pipeline_sandbox/procurement_la_seed.py
pipeline_sandbox/procurement_la_registry.py
```

Do not redo source discovery from scratch.

Next step should be:

```text
probe registry
→ enumerate files
→ sample parse
→ coverage report
→ parser classes
```

Do not immediately write a giant local-authority ETL.

---

### 5.8 FOI / AIE / OCEI Leads

Some bodies likely have the data but do not expose it cleanly.

Suggested file:

```text
data/_meta/procurement_publishers/foi_aie_leads.csv
```

Known clue sources:

```text
OCEI decisions:
https://ocei.ie/

Enterprise Ireland FOI logs:
https://www.enterprise-ireland.com/

Fáilte Ireland FOI logs:
https://www.failteireland.ie/

TheStory / Right to Know public-spend investigations:
https://www.thestory.ie/
```

Fields:

```text
body
clue_url
evidence_type
years_mentioned
status
notes
```

Do not ETL these until a lawful, repeatable, source-linked route exists.

---

## 6. Probe-First Implementation Plan for Claude

Claude should follow the steps below.

### Phase 0 — Inspect existing code

Before creating anything, inspect:

```text
pipeline_sandbox/procurement_etenders_extract.py
pipeline_sandbox/procurement_lobbying_xref.py
pipeline_sandbox/procurement_la_seed.py
pipeline_sandbox/procurement_la_registry.py
doc/PROCUREMENT_BUILD_PLAN.md
doc/PROCUREMENT_INVESTIGATION.md
data/_meta/procurement_coverage.json
data/_meta/procurement_lobbying_overlap_coverage.json
sql_views/procurement_awards.sql
sql_views/procurement_lobbying_overlap.sql
sql_views/lobbying_org_procurement.sql
utility/data_access/lobbying_data.py
utility/pages_code/lobbying_3.py
```

Answer:

```text
What procurement data is already gold?
What is still sandbox?
What metadata already exists?
What caveats are already used?
What value semantics already exist?
How are supplier names classified?
How are personal names quarantined?
How is CRO matching done?
How is lobbying overlap done?
```

Do not assume. Inspect.

---

### Phase 1 — Create seed registry files only

Create seed registry files under:

```text
data/_meta/procurement_publishers/
```

Suggested files:

```text
semistate_seed.csv
property_land_seed.csv
departments_seed.csv
agencies_seed.csv
health_seed.csv
education_seed.csv
foi_aie_leads.csv
```

Each registry row should include:

```text
publisher_id
publisher_name
publisher_type
sector
landing_url
source_status
source_format
grain
years_available
latest_period
supplier_level_available
amount_available
paid_flag_available
source_caveat
privacy_risk
notes
```

This phase should not attempt to download or parse all files.

Tests:

```text
seed files load as CSV
required columns exist
publisher_id is unique
landing_url is present where source_status is not NOT_FOUND
status values are from the allowed enum
grain values are from the allowed enum
```

---

### Phase 2 — Build a probe script

Create:

```text
pipeline_sandbox/probe_procurement_publishers.py
```

The probe script should:

```text
read seed CSVs
fetch landing pages where allowed
detect file links
classify file extensions
sample a small number of files per publisher
write a coverage report
avoid heavy parsing
avoid OCR
avoid large downloads unless explicitly requested
```

Output:

```text
data/_meta/procurement_publishers/procurement_publishers_probe.json
```

The report should include:

```text
publishers_total
publishers_confirmed_supplier_level
publishers_confirmed_category_level
publishers_annual_report_only
publishers_foi_aie_confirmed_exists
publishers_needs_manual_check
files_seen
formats_seen
top_failures
privacy_warnings
recommended_next_publishers
```

Tests:

```text
probe runs in dry-run mode
probe output validates against expected JSON keys
no network call is required for unit tests
sample HTML fixtures can be used
```

---

### Phase 3 — Sample extraction, not full ETL

Create one or more very small sample parsers only after the probe identifies the easiest source.

Do not start with ESB or Iarnród Éireann if they are not supplier-level.

Good first candidates are likely:

```text
OPW
Tailte Éireann
Teagasc
Bord Bia
IDA Ireland
NTA
Citizens Information Board
```

Create sample output:

```text
data/_meta/procurement_publishers/sample_extraction_report.json
```

The report should include:

```text
publisher
file_url
format
rows_extracted
columns_detected
supplier_column
amount_column
period_column
description_column
caveat_text_detected
personal_name_risk
strange_values
parser_confidence
```

“Strange values” should check:

```text
negative amounts
zero amounts
very large amounts
repeated identical values
missing supplier
missing amount
currency symbols mixed with text
VAT caveats
paid/not-paid flags
duplicate rows
category totals masquerading as supplier rows
```

---

### Phase 4 — Parser classes

Only after sample extraction passes should parser classes be created.

Suggested file:

```text
pipeline_sandbox/procurement_public_body_parsers.py
```

Potential parser interface:

```python
@dataclass(frozen=True)
class ParsedPaymentFile:
    publisher_id: str
    source_file_url: str
    source_file_hash: str
    rows: pd.DataFrame
    warnings: list[str]
    parser_name: str
    parser_version: str

class PaymentFileParser(Protocol):
    name: str
    supported_formats: tuple[str, ...]
    def can_parse(self, file_info: dict) -> bool: ...
    def parse(self, path: Path, file_info: dict) -> ParsedPaymentFile: ...
```

Support formats incrementally:

```text
HTML_TABLE
XLSX
CSV
PDF_DIGITAL
```

Do **not** add OCR unless probes prove a high-value source requires it.

---

### Phase 5 — Gold parquet candidate

Only after the parser is stable, create a gold candidate table.

Potential output:

```text
data/gold/parquet/public_payments_fact.parquet
```

or a narrower first table:

```text
data/gold/parquet/procurement_public_body_payments.parquet
```

Required columns:

```text
publisher_id
publisher_name
publisher_type
sector
source_landing_url
source_file_url
source_file_hash
period
year
quarter
supplier_raw
supplier_normalised
amount_eur
amount_semantics
description
po_number
paid_flag
source_row_number
source_page_number
parser_name
parser_version
extraction_status
extraction_confidence
supplier_class
privacy_status
public_display
source_caveat
```

Write with Zstd compression and statistics, consistent with the project’s Parquet practices.

---

### Phase 6 — Coverage metadata

Write:

```text
data/_meta/public_payments_coverage.json
```

Include:

```text
publishers_attempted
publishers_successful
publishers_failed
rows_extracted
rows_public_display
rows_quarantined
rows_supplier_level
rows_category_level
rows_with_amount
rows_missing_amount
rows_missing_supplier
amount_total_safe_to_sum
amount_total_not_safe_to_sum
top_publishers_by_rows
top_failure_reasons
privacy_quarantine_counts
schema_version
generated_at
```

Do not ship gold without metadata.

---

### Phase 7 — SQL views

Only after gold is stable, add SQL views.

Suggested views:

```text
sql_views/public_payments_fact.sql
sql_views/public_payment_supplier_summary.sql
sql_views/public_payment_publisher_summary.sql
sql_views/public_payment_lobbying_overlap.sql
sql_views/public_payment_cro_company_overlap.sql
sql_views/public_payment_corporate_notice_overlap.sql
```

Rules:

- SQL views should preserve caveats.
- Do not sum unsafe values.
- Do not expose quarantined personal-name rows in UI-facing views.
- Keep source file URL available.
- Keep publisher type and source status available.

---

### Phase 8 — Tests

Add tests before any page work.

Suggested tests:

```text
test_procurement_publisher_seeds.py
test_procurement_public_body_probe.py
test_procurement_public_body_parsers.py
test_public_payments_gold_contract.py
test_public_payments_sql_views.py
```

Test goals:

```text
seed registries have required fields
enum values are valid
probe report has expected keys
sample parser handles fixture files
sample parser detects caveats
sample parser flags strange values
gold parquet has required columns
gold parquet contains no public_display=True rows for quarantined personal names
safe-to-sum totals do not include unsafe amount semantics
SQL views register
SQL views expose required columns
```

---

### Phase 9 — Optional Streamlit Procurement page

Only after gold + SQL + tests exist should a page be created.

Suggested page:

```text
utility/pages_code/procurement.py
```

Suggested navigation label:

```text
Procurement
```

Initial page scope:

```text
public-body payments / purchase orders
supplier search
publisher search
source file drilldown
lobbying overlap
CRO/company match
caveats
```

Do not lead with a giant total unless the value is genuinely safe to sum.

Recommended UI language:

```text
Public-body payment / purchase-order records
Source-linked supplier records
Safe-to-sum amount
Not-safe-to-sum amount
Quarantined personal-name records excluded from public display
Co-occurrence only; no influence claim
```

---

## 7. Enrichment Checks

For every supplier-level row, attempt safe enrichment only after supplier classification.

Potential enrichments:

```text
CRO company match
charity match
lobbying registrant/client match
corporate notice match
CBI/regulated firm match
existing procurement award match
```

Each enrichment must include:

```text
match_method
match_confidence
match_status
source_system
source_url
manual_review_required
```

Do not use fuzzy matches in public UI until reviewed.

Exact normalised-name matches may be used as a conservative first pass, but must be labelled as undercounting.

---

## 8. Claude Prompt

Use this prompt when asking Claude to implement or plan the feature.

```text
You are an expert Python data engineer, civic-data architect, DuckDB/SQL reviewer, procurement-data analyst, and cautious public-records pipeline designer.

Repository:

https://github.com/peweet/dail_tracker/tree/seanad-app-parity

Task:

Plan and begin a probe-first expansion of Dáil Tracker’s procurement/public-payments data beyond the current eTenders and local-authority exploration.

Do not build a finished Streamlit page yet.
Do not wire new chains into pipeline.py yet.
Do not create a giant ETL immediately.
Keep new exploratory work in pipeline_sandbox/ and metadata seed files under data/_meta/procurement_publishers/.
The goal is a high-fidelity, source-linked, caveated gold parquet table later, with eventual display on a Procurement Streamlit page.

First inspect the existing procurement code and docs:

- pipeline_sandbox/procurement_etenders_extract.py
- pipeline_sandbox/procurement_lobbying_xref.py
- pipeline_sandbox/procurement_la_seed.py
- pipeline_sandbox/procurement_la_registry.py
- doc/PROCUREMENT_BUILD_PLAN.md
- doc/PROCUREMENT_INVESTIGATION.md
- data/_meta/procurement_coverage.json
- data/_meta/procurement_lobbying_overlap_coverage.json
- sql_views/procurement_awards.sql
- sql_views/procurement_lobbying_overlap.sql
- sql_views/lobbying_org_procurement.sql
- utility/pages_code/lobbying_3.py
- utility/data_access/lobbying_data.py

Then answer:

1. What procurement work already exists?
2. What is still sandbox?
3. What is already gold?
4. What metadata/caveats already exist?
5. What should not be duplicated?
6. What is the safest first new registry/probe PR?

The new work should focus on semi-state, state-body, health, education, agency, department, and property/land public-payment sources.

Source groups to investigate:

- semi-state / commercial state bodies:
  CIÉ, Iarnród Éireann, Bus Éireann, Dublin Bus, daa, Dublin Port, ESB, ESB Networks, EirGrid, Gas Networks Ireland, Uisce Éireann, Bord na Móna, Coillte, Teagasc, Bord Bia, BIM, RTÉ, TG4, Enterprise Ireland, IDA Ireland, Fáilte Ireland, Tourism Ireland.

- OPW / land / property:
  OPW, Tailte Éireann.

- central departments:
  Finance, Health, Social Protection, Defence, Justice, Transport, Housing, Climate/Energy, Enterprise, Public Expenditure/OGP/OGCIO, Culture, IPAS.

- agencies/regulators:
  NTA, TII, Citizens Information Board, HSA, Marine Institute, Revenue, Tusla, RSA, Screen Ireland, Arts Council.

- health:
  HSE, NTPF, HIQA, Tallaght University Hospital, St Vincent’s University Hospital.

- education:
  HEA, ATU, ETBs, NCSE.

Use a registry-first design.

Create or propose seed files:

data/_meta/procurement_publishers/semistate_seed.csv
data/_meta/procurement_publishers/property_land_seed.csv
data/_meta/procurement_publishers/departments_seed.csv
data/_meta/procurement_publishers/agencies_seed.csv
data/_meta/procurement_publishers/health_seed.csv
data/_meta/procurement_publishers/education_seed.csv
data/_meta/procurement_publishers/foi_aie_leads.csv

Each row should include:

publisher_id
publisher_name
publisher_type
sector
landing_url
source_status
source_format
grain
years_available
latest_period
supplier_level_available
amount_available
paid_flag_available
source_caveat
privacy_risk
notes

Use these source_status values only:

CONFIRMED_SUPPLIER_LEVEL
CONFIRMED_CATEGORY_LEVEL
CONFIRMED_PUBLIC_CONTRACTS_ONLY
ANNUAL_REPORT_ONLY
FOI_AIE_CONFIRMED_EXISTS
FOI_CLUE_ONLY
NEEDS_MANUAL_CHECK
NOT_FOUND
NOT_APPLICABLE

Use these grain values only:

payment
purchase_order
contract_award
framework_ceiling
category_total
annual_summary
mixed
unknown

After seed files, design a probe script:

pipeline_sandbox/probe_procurement_publishers.py

The probe should read seed files, fetch landing pages where safe, identify file links, classify file formats, sample a small number of files, and write:

data/_meta/procurement_publishers/procurement_publishers_probe.json

Do not parse the entire corpus in the first PR.

The probe should report:

publishers_total
publishers_confirmed_supplier_level
publishers_confirmed_category_level
publishers_annual_report_only
publishers_foi_aie_confirmed_exists
publishers_needs_manual_check
files_seen
formats_seen
top_failures
privacy_warnings
recommended_next_publishers

Before any full parser, create sample extraction reports for a few easy sources, likely OPW, Tailte Éireann, Teagasc, Bord Bia, IDA Ireland, NTA, or Citizens Information Board.

Sample extraction should check:

supplier column
amount column
period/date
description
PO number
paid flag
source caveats
negative amounts
zero amounts
very large amounts
repeated identical values
missing supplier
missing amount
VAT caveats
personal-name risks
category totals masquerading as supplier rows

Only after sample extraction succeeds should parser classes be proposed.

Potential gold table later:

data/gold/parquet/public_payments_fact.parquet

Required columns:

publisher_id
publisher_name
publisher_type
sector
source_landing_url
source_file_url
source_file_hash
period
year
quarter
supplier_raw
supplier_normalised
amount_eur
amount_semantics
description
po_number
paid_flag
source_row_number
source_page_number
parser_name
parser_version
extraction_status
extraction_confidence
supplier_class
privacy_status
public_display
source_caveat

Gold output should use Zstd compression and write a coverage file:

data/_meta/public_payments_coverage.json

Do not create a Streamlit page until gold + SQL + tests exist.

Potential SQL views later:

sql_views/public_payments_fact.sql
sql_views/public_payment_supplier_summary.sql
sql_views/public_payment_publisher_summary.sql
sql_views/public_payment_lobbying_overlap.sql
sql_views/public_payment_cro_company_overlap.sql
sql_views/public_payment_corporate_notice_overlap.sql

Testing requirements:

Add tests for seeds, probe output, parser fixtures, gold contracts, privacy quarantine, safe-to-sum semantics, and SQL registration.

Do not claim a source is supplier-level unless inspected.
Do not claim a value is spend unless the source says it is payment/spend.
Do not expose likely personal-name supplier rows publicly.
Do not imply lobbying influenced procurement.
Do not wire into pipeline.py until the feature is proven.
Prefer small PRs.
Be explicit about what is confirmed, what is partial, and what needs manual checking.
```

---

## 9. Recommended First PR

The safest first PR is not an ETL.

The safest first PR is:

```text
1. Add seed CSVs under data/_meta/procurement_publishers/.
2. Add enums/validation test for those seed CSVs.
3. Add a dry-run probe skeleton that can read seeds and produce an empty/partial probe report.
4. Do not download large files.
5. Do not modify pipeline.py.
6. Do not modify Streamlit UI.
```

Acceptance criteria:

```text
[ ] Seed CSVs exist.
[ ] Required columns exist.
[ ] Enum values validate.
[ ] Publisher IDs are unique.
[ ] Known source links are captured.
[ ] Probe script has --dry-run.
[ ] Probe report schema is defined.
[ ] Tests pass.
[ ] No new pipeline chain is added.
[ ] No user-facing UI is changed.
```

---

## 10. Blunt Implementation Guidance

Do not try to “beat” any existing public payments dashboard by building another dashboard first.

Build the better data layer first.

The valuable product is:

```text
source-linked public payment / purchase-order records
+ supplier entity resolution
+ safe amount semantics
+ privacy quarantine
+ CRO/charity/lobbying/corporate enrichment
+ eventual Procurement page
```

The moat is not the chart.

The moat is the cleaned, source-linked, high-fidelity Irish public-money corpus.
