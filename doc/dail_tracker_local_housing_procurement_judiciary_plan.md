# Dáil Tracker — Local Council, Housing, Procurement, and Judiciary Product/Architecture Plan

**Prepared:** 2026-06-05  
**Repository reviewed:** `https://github.com/peweet/dail_tracker`  
**Scope requested:** deeper plan for local council, housing, procurement, and judiciary, with attention to a second planned tile.

---

## 0. Executive decision

The strongest near-term product move is **not** to add four unrelated pages.

The repo is already converging toward a richer civic-accountability model:

```text
politician / constituency context
  + local authority context
  + public money lifecycle
  + procurement and supplier records
  + housing need and delivery indicators
  + legal/judiciary/public-record signals
  + audit/accountability overlays
```

The product should therefore add **two planned tiles / feature clusters**, plus keep Judiciary as a controlled legal-transparency lane:

```text
Tile 1 — Public Money & Procurement
  A supplier/body/procurement explorer.
  Sources: eTenders, TED, public-body payments, local-authority PO/payments,
  procurement-lobbying overlap, CRO matching, later PAC/C&AG/local-audit context.

Tile 2 — Local Authority & Housing
  A council/constituency civic-context explorer.
  Sources: Local Authority AFS, LA budgets, LA PO/payments, SSHA, housing grants,
  social/affordable delivery, homelessness, rents, census/deprivation denominators.

Existing / adjacent tile — Courts & Judiciary
  Current repo now includes a routed Courts & Judiciary page.
  It should remain source-cautious and privacy-first, and later connect to
  CPO / planning / infrastructure legal signals only at scheme level.
```

The critical distinction:

- **Procurement** is already backend-heavy and should be surfaced soon.
- **Local council finance** is partly built as AFS + LA procurement/payment work, but needs a profile model.
- **Housing** is currently strongest as documentation/scoping and should be built as a cross-cutting local-authority/constituency lens, not a standalone scrape.
- **Judiciary** is now visible in the app navigation on `main`, but it needs hard privacy and pipeline-readiness checks before being treated as stable.

---

## 1. Evidence reviewed

This plan is based on direct inspection of repository files and docs available through GitHub raw/tree views. I did not clone the repo locally, so this is not a complete file-by-file audit. Evidence most relevant to this plan:

### App surface

- `utility/app.py`
  - Imports `pages_code.judiciary.judiciary_page`.
  - Routes `Courts & Judiciary` at `rankings-judiciary`.
  - Does **not** route Procurement or Housing as top-nav pages.
  - Uses dimension pages with `rankings-` prefixes that funnel into member/profile pages.

### Procurement and public-money sources

- `pipeline.py`
  - Includes `afs`, `procurement`, `procurement_lobbying`, and `ted` chains.
  - Describes `afs` as local-authority Annual Financial Statements by service division.
  - Describes procurement as eTenders/OGP awards + supplier-to-CRO match.
  - Describes procurement-lobbying as supplier-to-lobbying overlap.
  - Describes TED as EU procurement award notices for Ireland, not yet exposed to UI.

- `extractors/procurement_etenders_extract.py`
  - Builds `procurement_awards.parquet`.
  - Builds `procurement_supplier_cro_match.parquet`.
  - Explicitly flags value semantics: awarded value is not spend.
  - Quarantines sole traders / individuals from CRO matching.
  - Flags frameworks, DPS ceilings, multi-supplier repeated values, and `value_safe_to_sum`.

- `extractors/procurement_la_seed.py`
  - Seeds all 31 local authorities for PO/payment-over-€20k discovery.
  - Documents inconsistent council file formats: XLSX, CSV, digital PDF, JS-rendered lists, stale catalogues, no clean landing page, etc.
  - Goal is to avoid 31 bespoke scrapers by turning councils into config rows.

- `extractors/procurement_la_payments_extract.py`
  - Builds a 31-council per-transaction fact from local-authority purchase orders/payments over €20k.
  - Writes to `data/silver/parquet/la_payments_fact.parquet`.
  - Not wired into `pipeline.py` yet.
  - Aligns schema with generic public-payments fact so the facts can union later.
  - Distinguishes `po_committed` and `payment_actual`.
  - Applies supplier privacy classification.

- `extractors/procurement_public_body_extract.py`
  - Generic public-body PO/payment extractor.
  - Writes a gold-candidate sandbox fact, not main promoted gold.
  - Explicitly states HSE/Tusla and local-authority payments are owned by separate bespoke contexts.
  - Has a privacy classification system, but notes quarantine is deferred in that extractor.

- `extractors/ted_ireland_extract.py`
  - Present as a TED Ireland procurement lane.
  - Pipeline comment says it is silver, cleaned, not yet UI-exposed.

- SQL views:
  - `sql_views/procurement_awards.sql`
  - `sql_views/procurement_supplier_summary.sql`
  - `sql_views/procurement_authority_summary.sql`
  - `sql_views/procurement_cpv_summary.sql`
  - `sql_views/procurement_lobbying_overlap.sql`
  - `sql_views/lobbying_org_procurement.sql`

- Query/data-access:
  - `dail_tracker_core/queries/procurement.py`
  - `utility/data_access/procurement_data.py`

### Local council and housing planning

- `doc/SSHA_social_housing_summary.md`
  - Summarises Social Housing Assessments 2024 and 2025.
  - Identifies SSHA as annual 31-local-authority net-need data.
  - Notes Housing Agency Data Hub machine-readable tables.
  - Flags the source as “for enrichment — not yet built.”
  - Proposes local-authority-to-constituency mapping.
  - Warns that SSHA net need excludes HAP/RAS/SHCEP and current support households.
  - Lists possible complementary sources: Construction Status Reports, homelessness data, HAP, CSO census, RTB rents, Pobal deprivation, boundaries.

- `doc/new_sources_value_and_features_claude_plan.md`
  - Frames combination profiles:
    - Infrastructure Project Profile
    - Public Body Profile
    - Local Authority Profile
    - Supplier Dossier
  - Specifically identifies Local Authority Profile as:
    `AFS actuals + LA budget tables + LA payments + Housing grants + LA statutory audit reports`.
  - Ranks LA budget-to-AFS planned-vs-actual and Housing Adaptation Grants highly.
  - Reinforces not mixing money meanings.

- `doc/new_public_money_legal_sources_claude_backlog.md`
  - Excludes existing/active local authority PO/payment, public-body payments, eTenders, TED, and LA AFS from its “new source” backlog.
  - Adds source candidates:
    - Project Ireland 2040
    - CPO cases
    - board minutes
    - C&AG / PAC
    - local authority statutory audit reports
    - grants/subventions
    - LA budget tables
    - FOI/AIE logs
  - Strong rule: probe first, do not wire immediately, preserve value kind and provenance.

### Judiciary

- `utility/app.py`
  - Current `main` appears to route `Courts & Judiciary`.

- `utility/pages_code/judiciary.py`
  - Judiciary page exists.
  - It is framed as public legal-diary transparency, not a misconduct page.

- `extractors/legal_diary_extract.py`
  - Legal diary extractor exists.
  - Intended tiers:
    - schedule,
    - aggregate counts,
    - anonymised case listings.

- SQL views:
  - `sql_views/judiciary_legal_diary_schedule.sql`
  - `sql_views/judiciary_legal_diary_counts.sql`
  - `sql_views/judiciary_legal_diary_cases.sql`

- Query/data-access:
  - `dail_tracker_core/queries/judiciary.py`
  - `utility/data_access/judiciary_data.py`

---

## 2. Current implementation status by domain

| Domain | Evidence of source ingestion | Evidence of SQL/query layer | Evidence of UI | Current read |
|---|---|---|---|---|
| Procurement / eTenders | Yes | Yes | No top-nav page | Backend mature enough for first UI skeleton |
| Procurement / lobbying overlap | Yes | Yes | Partly via lobbying, future procurement view | Valuable but high-risk wording |
| TED Ireland | Pipeline chain exists | unclear from opened files | No UI | Keep as non-surfaced silver enrichment |
| Local-authority PO/payments | Extractor exists; not pipeline-wired | no confirmed gold SQL views yet | No UI | Strong prototype, not promoted |
| Local-authority AFS | Pipeline chain exists | no confirmed SQL views in listing except broad public finance view | No dedicated LA page | Needs profile model |
| Housing / SSHA | Documentation/scoping only | no housing SQL views observed | No UI | Build as planned Tile 2 |
| Housing grants | Scoped in docs, not confirmed ETL | no SQL views observed | No UI | Best first housing-money source |
| Judiciary / legal diary | Extractor exists | Yes | Yes, routed on current main | Surfaced, but should remain guarded |
| CPO / planning legal signal | Scoped only | no ETL observed | No UI | Candidate bridge between housing, infrastructure, and judiciary |

---

## 3. Product model: two planned tiles

### Tile 1 — Public Money & Procurement

#### User question

> “Who is receiving public contracts/payments, from which bodies, for what categories, with what source caveats, and where does the same organisation also appear in lobbying or corporate records?”

#### Why this tile should exist

The repo already has a strong backend spine:

```text
eTenders / OGP awards
  -> supplier classification
  -> CRO exact-name match
  -> value semantics
  -> procurement SQL views
  -> procurement core queries

lobbying returns
  -> organisation normalization
  -> procurement-lobbying overlap

public-body / LA payments
  -> committed / spent fact shape
  -> future union layer

TED
  -> EU award notices
  -> future enrichment
```

This is one of the repo’s highest-value areas because it can produce something more useful than a flat procurement dashboard: a **supplier/body/public-money dossier** with caveats.

#### Tile structure

```text
Public Money & Procurement
  ├─ Overview
  │   ├─ data coverage
  │   ├─ source freshness
  │   ├─ value-kind legend
  │   └─ caveat: award ≠ spend
  │
  ├─ Suppliers
  │   ├─ n_awards
  │   ├─ n_authorities
  │   ├─ awarded_value_safe_eur
  │   ├─ CRO match status
  │   ├─ lobbying overlap badge
  │   └─ privacy-safe supplier class
  │
  ├─ Contracting authorities
  │   ├─ authority
  │   ├─ award count
  │   ├─ supplier count
  │   ├─ safe awarded value
  │   └─ source coverage
  │
  ├─ Categories / CPV
  │   ├─ CPV code
  │   ├─ CPV description
  │   ├─ award count
  │   └─ supplier count
  │
  ├─ Supplier detail
  │   ├─ all eTenders award rows
  │   ├─ value kind for each row
  │   ├─ whether value can be summed
  │   ├─ CRO match details
  │   ├─ lobbying co-occurrence
  │   └─ source URLs / source dataset
  │
  └─ Future: payments/spend tab
      ├─ public-body payments
      ├─ local-authority PO/payment facts
      ├─ HSE/Tusla bespoke payment facts
      └─ award-to-spend candidate matches
```

#### Required UI copy

Use:

> “Awarded value, not actual spend.”

Use:

> “This organisation appears in both procurement and lobbying records.”

Do **not** use:

> “Influenced”
> “Bought”
> “Conflict”
> “Corruption”
> “Received €X in public money” when the value is only an award/framework ceiling.

#### Backend work

##### P0 — Procurement page skeleton

Create:

```text
utility/pages_code/procurement.py
```

Wire:

```text
from pages_code.procurement import procurement_page
st.Page(procurement_page, title="Procurement", icon=":material/request_quote:", url_path="rankings-procurement")
```

Use existing:

```text
utility/data_access/procurement_data.py
dail_tracker_core/queries/procurement.py
sql_views/procurement_*.sql
```

Initial page should render only:

- supplier summary,
- authority summary,
- CPV summary,
- lobbying overlap,
- a clear source/caveat panel.

##### P1 — Core query contract improvement

Current `utility/data_access/procurement_data.py` unwraps `QueryResult` to an empty dataframe on source failure. For a public page, that is not enough. The page should distinguish:

```text
source unavailable
vs
data file missing
vs
query/view failure
vs
zero results
```

Recommended change:

```text
utility/data_access/procurement_data.py
  fetch_supplier_summary_result() -> QueryResult
  fetch_supplier_summary() -> DataFrame  # backwards compatible
```

Then the page can show a proper “source unavailable” state.

##### P1 — Value-kind guardrail component

Create reusable component:

```text
utility/ui/value_kind_legend.py
```

Inputs:

```python
value_kind_counts: DataFrame
safe_to_sum_total: float
naive_total_hidden: bool
```

Display:

```text
contract_award_value     can be summed only if value_safe_to_sum = true
framework_or_dps_ceiling do not sum
framework_call_off       not actual payment unless source says paid
payment_actual           actual spend/payment
po_committed             committed/ordered, not necessarily paid
budget_allocated         planned budget, not spend
afs_expenditure          audited/annual actual expenditure
grant_or_subvention      allocation/payment semantics required
```

##### P2 — Promote LA payments carefully

`procurement_la_payments_extract.py` currently says it writes `data/silver/parquet/la_payments_fact.parquet` and is not wired into `pipeline.py`.

Promotion steps:

1. Add tests around the schema.
2. Validate 31-council coverage.
3. Add privacy gating before public display.
4. Add source coverage JSON.
5. Promote to gold only after schema/freshness tests.
6. Add SQL view:

```text
sql_views/publicmoney_la_payments.sql
```

Recommended view name:

```sql
v_publicmoney_la_payments
```

Avoid naming it `procurement_la_spend` because some councils publish purchase orders rather than actual payments.

##### P3 — Award-to-payment matching

Only after both awards and payment facts are stable:

```text
extractors/procurement_award_spend_link.py
```

Should produce candidate links, not definitive links:

```text
supplier_norm
authority_norm
amount_similarity
date_window_days
description_similarity
match_confidence
match_reason
source_award_id
source_payment_id
review_status
```

UI wording:

> “Possible related payment/PO records.”

not:

> “This award was paid.”

---

### Tile 2 — Local Authority & Housing

#### User question

> “What is happening in my council or constituency: housing need, delivery, public spending, procurement/payment flows, budgets, audits, and local context?”

#### Why this tile should be separate from Procurement

Procurement is supplier/body oriented. Housing/local council is geography and service-need oriented.

A user thinks:

```text
Who is getting contracts?              -> Procurement tile
What is happening in my area/council?  -> Local Authority & Housing tile
```

The repo’s own docs point this direction:

```text
Local Authority Profile =
  AFS actuals
  + LA budget tables
  + LA payments
  + Housing grants
  + LA statutory audit reports
```

Housing should be the first major policy lens inside the Local Authority Profile, not a standalone isolated page.

#### Tile structure

```text
Local Authority & Housing
  ├─ Select geography
  │   ├─ council
  │   ├─ county
  │   ├─ constituency
  │   └─ member constituency context
  │
  ├─ Housing need
  │   ├─ SSHA net need
  │   ├─ trend since 2016
  │   ├─ homelessness basis / tenure indicators
  │   ├─ household composition
  │   └─ current-tenure mix
  │
  ├─ Housing delivery / supports
  │   ├─ social and affordable provision
  │   ├─ construction-status reports
  │   ├─ HAP/RAS/SHCEP context
  │   ├─ Housing Adaptation Grants
  │   └─ caveat: not all measures count the same thing
  │
  ├─ Local authority finance
  │   ├─ AFS actuals by service division
  │   ├─ budget allocated by service division
  │   ├─ planned vs actual variance
  │   └─ public-finance source notes
  │
  ├─ Local authority procurement/payments
  │   ├─ PO/payment over €20k fact
  │   ├─ supplier privacy gating
  │   ├─ value kind: committed vs paid
  │   └─ link to Procurement supplier detail
  │
  ├─ Audit and accountability
  │   ├─ Local Government Audit Service reports
  │   ├─ C&AG/PAC links where applicable
  │   └─ findings as source-backed audit findings only
  │
  └─ Constituency/member context
      ├─ TDs for this constituency
      ├─ member questions on housing
      ├─ debates and legislation touching housing
      └─ source-linked policy activity
```

#### Housing source priority

Build housing in this order:

##### H1 — SSHA local-authority net need

Use machine-readable Housing Agency Data Hub where possible. The docs say SSHA has 31-local-authority appendix tables with dimensions such as age, employment, income, household size, basis of need, accommodation requirements, tenure, waiting-list length, and citizenship.

Expected outputs:

```text
data/silver/parquet/housing_ssha_la_fact.parquet
data/_meta/housing_ssha_coverage.json
```

Recommended schema:

```text
year
count_date
local_authority
local_authority_code
metric_group
metric
submetric
households
percent
source_report
source_url
source_table
retrieved_utc
```

Initial SQL views:

```text
sql_views/housing_ssha_la_summary.sql
sql_views/housing_ssha_need_basis.sql
sql_views/housing_ssha_tenure.sql
sql_views/housing_ssha_waiting_time.sql
```

##### H2 — Housing Adaptation Grants

The new-source plan identifies Housing Adaptation Grants as a clean standalone money fact with low privacy risk because it is LA-aggregated.

Expected outputs:

```text
data/silver/parquet/housing_adaptation_grants_la.parquet
data/_meta/housing_adaptation_grants_coverage.json
```

Value-kind discipline:

```text
grant_allocated
grant_paid
grant_drawdown
unknown_grant_semantics
```

Do not mix allocated and paid.

##### H3 — Social and affordable delivery / construction status

Sources listed in docs:

```text
Social Housing Construction Status Reports
Overall social and affordable housing provision
HSA07 Social and Affordable Provision
```

Expected outputs:

```text
data/silver/parquet/housing_delivery_la_project_or_summary.parquet
data/_meta/housing_delivery_coverage.json
```

Do not join project rows to procurement spend unless a source-level identifier or high-confidence project name/body match exists.

##### H4 — Homelessness datasets

Use only aggregate data. Keep source caveat: SSHA homelessness figures differ from DHLGH monthly homelessness reports.

Expected outputs:

```text
data/silver/parquet/housing_homelessness_region_or_la.parquet
```

Depending on source grain, this may be region/county rather than local authority. Do not force a false LA mapping.

##### H5 — CSO census, RTB rents, Pobal deprivation

These are context/denominator datasets:

```text
population denominator
household tenure
rental pressure
deprivation context
```

They should not be presented as “performance” metrics for a TD or council. They are background conditions.

#### Geography model

This is the hardest piece.

Local authorities and Dáil constituencies do not map 1:1. The docs already flag this.

Recommended model:

```text
dimension_geography
  geography_id
  geography_type        # local_authority | constituency | county | small_area
  name
  source
  boundary_year

bridge_la_constituency
  local_authority_id
  constituency_id
  overlap_method        # population_weight | area_weight | manual | unknown
  overlap_weight
  source
  caveat

bridge_small_area_constituency
  small_area_id
  constituency_id
  population
  source
```

For v1, use local authority directly and only show constituency rollups where the weight is defensible.

#### Planned page

Create:

```text
utility/pages_code/local_authority_housing.py
utility/data_access/local_authority_housing_data.py
dail_tracker_core/queries/local_authority_housing.py
```

Initial route:

```text
st.Page(
    local_authority_housing_page,
    title="Local Authority & Housing",
    icon=":material/location_city:",
    url_path="rankings-local-authority-housing",
)
```

Alternative shorter title if top-nav is crowded:

```text
Local & Housing
```

#### Why not call this page just “Housing”?

Because the repo’s strongest housing data is local-authority-level context. A pure “Housing” page risks becoming a national-statistics dashboard detached from the rest of Dáil Tracker. A “Local Authority & Housing” tile keeps it joined to:

- councils,
- constituencies,
- TD profiles,
- spending,
- procurement,
- audit reports,
- public body profiles.

---

## 4. Judiciary / legal-transparency plan

### Current status correction

The current `main` version of `utility/app.py` appears to already route:

```text
Courts & Judiciary
rankings-judiciary
```

Therefore Judiciary is no longer completely unsurfaced in the inspected branch. It may still be “not surfaced” in a deployed app or another branch, but in the repository inspected it is top-nav routed.

### What Judiciary should and should not be

It should be:

```text
a source-linked, privacy-first view of public legal-diary activity
```

It should not be:

```text
a people-search product
a public register of litigants
a judicial performance ranking
a misconduct insinuation page
```

### Existing architecture

```text
Courts Service Legal Diary / source docs
  -> pdf_infra/legal_diary_poller.py
  -> extractors/legal_diary_extract.py
  -> gold parquets:
       judicial_legal_diary_schedule.parquet
       judicial_legal_diary_counts.parquet
       judicial_legal_diary_cases.parquet
  -> SQL:
       v_judiciary_legal_diary_schedule
       v_judiciary_legal_diary_counts
       v_judiciary_legal_diary_cases
  -> core query:
       dail_tracker_core/queries/judiciary.py
  -> Streamlit wrapper:
       utility/data_access/judiciary_data.py
  -> page:
       utility/pages_code/judiciary.py
```

### Required hardening before treating as stable

#### J1 — Replace privacy asserts with explicit exceptions

If any privacy invariant uses `assert`, replace with a permanent runtime check.

Bad:

```python
assert "raw_case" not in cases_df.columns
```

Good:

```python
if "raw_case" in cases_df.columns:
    raise PrivacyInvariantError("raw_case must never be written to public gold cases")
```

#### J2 — Golden privacy tests

Create tests:

```text
test/test_judiciary_privacy.py
```

Cases:

```text
family/minor/ward/childcare/asylum terms are dropped
natural persons reduced to initials
case references stripped
solicitor annotations stripped
raw_case never appears in public parquet schema
source_url/source_sha256 always present
```

#### J3 — Pipeline status

If legal diary extraction is not in `pipeline.py`, either:

- add a `judiciary` chain, or
- explicitly mark it as a separately scheduled poller.

Recommended chain:

```python
("judiciary", "extractors/legal_diary_extract.py")
```

But only after fixtures and privacy tests pass.

#### J4 — UI framing

The page should lead with method/caveat:

```text
This page summarizes public Legal Diary records.
Protected categories are excluded.
Natural-person names are anonymised.
Counts are listing activity, not judicial performance.
```

Avoid:

```text
busiest judge
top judge
case backlog by judge
```

Prefer:

```text
listed sessions
listed items
court/list activity
```

---

## 5. Cross-domain feature: where these become differentiated

The most differentiated opportunity is not “add procurement page” or “add housing page.” It is the combined lifecycle:

```text
Housing / infrastructure need
  -> local authority or department plan
  -> budget / AFS / public finance context
  -> CPO or planning/legal signal
  -> board or council approval
  -> procurement award
  -> purchase order / payment
  -> audit finding / PAC/C&AG review
  -> TD questions/debates/legislation around the issue
```

### Example: housing project or local housing pressure

```text
SSHA says net need is rising in a council
  -> local authority budget/AFS shows housing-service spend trend
  -> housing grants / delivery stats show support/delivery activity
  -> procurement awards show contractors / consultants
  -> LA PO/payments show actual paid/committed flows where available
  -> CPO/planning signals show land/project stage
  -> local audit reports flag governance/procurement issues if any
  -> TD questions/debates show political attention
```

This becomes a genuinely differentiated civic-tech product.

---

## 6. Data contracts

### Procurement award row

```text
tender_id
supplier
supplier_norm
supplier_class
name_truncated
contracting_authority
contracting_authority_norm
cpv_code
cpv_description
competition_type
award_date
value_eur
value_kind
is_framework_or_dps
value_shared_across_suppliers
value_safe_to_sum
source_dataset
source_url
retrieved_utc
```

### Public payment / PO row

```text
publisher_id
publisher_name
publisher_type
sector
local_authority
period_start
period_end
supplier
supplier_norm
supplier_class
privacy_status
public_display
amount_eur
value_kind              # payment_actual | po_committed
realisation_tier        # SPENT | COMMITTED
description
po_number
source_url
source_file_url
source_sha256
parser_version
parse_confidence
```

### Local authority AFS / budget row

```text
local_authority
year
service_division_code   # A-H
service_division_name
metric                  # income | expenditure | budget | actual
amount_eur
value_kind              # budget_allocated | afs_expenditure
source_url
source_table
retrieved_utc
```

### Housing SSHA row

```text
year
count_date
local_authority
metric_group
metric
submetric
households
percent
source_report
source_url
source_table
```

### Judiciary legal diary case row

```text
diary_date
court
judge
list_type
status
category
case_anonymised
source
source_url
source_sha256
```

No raw party names, no case references, no solicitor annotations.

---

## 7. SQL view plan

### Procurement

Existing views:

```text
v_procurement_awards
v_procurement_supplier_summary
v_procurement_authority_summary
v_procurement_cpv_summary
v_procurement_lobbying_overlap
```

Add after LA/public-body promotion:

```text
v_publicmoney_payments
v_publicmoney_la_payments
v_publicmoney_supplier_summary
v_publicmoney_body_summary
v_publicmoney_value_kind_summary
v_procurement_award_payment_candidates
```

### Local Authority & Housing

Add:

```text
v_local_authority_index
v_local_authority_afs_actuals
v_local_authority_budget_actual_variance
v_local_authority_procurement_payments
v_housing_ssha_la_summary
v_housing_ssha_need_basis
v_housing_ssha_tenure
v_housing_ssha_waiting_time
v_housing_grants_la_summary
v_housing_delivery_la_summary
v_local_authority_audit_reports
```

### Judiciary

Existing views:

```text
v_judiciary_legal_diary_schedule
v_judiciary_legal_diary_counts
v_judiciary_legal_diary_cases
```

Possible later legal/infrastructure bridge views:

```text
v_cpo_cases_scheme_level
v_cpo_cases_public_body_summary
v_legal_infrastructure_signals
```

Do not merge CPO into Judiciary automatically. CPO belongs more naturally to Infrastructure / Local Authority / Housing context, with Judiciary/Courts as a source/caveat reference only.

---

## 8. Test plan

### Procurement tests

```text
test/test_procurement_value_semantics.py
```

Must prove:

- framework/DPS ceilings are not safe to sum;
- multi-supplier repeated values are not safe to sum;
- null tender IDs are not used to validate de-duplication;
- sub-€1 noise is not safe to sum;
- `awarded_value_safe_eur` sums only `value_safe_to_sum`.

```text
test/test_procurement_privacy.py
```

Must prove:

- sole traders/individuals are not CRO-matched;
- public display rules are applied before UI;
- truncated source names are excluded from CRO matching;
- supplier-class counts are included in coverage metadata.

```text
test/test_procurement_page_smoke.py
```

Must prove:

- page imports;
- missing parquet produces source-unavailable state;
- zero data produces zero-record state, not failure;
- value-kind legend renders.

### Local Authority & Housing tests

```text
test/test_housing_ssha_extract.py
```

Must prove:

- each year has 31 local authorities;
- totals reconcile to national headline when applicable;
- tables preserve metric group/submetric;
- net-need caveat is attached.

```text
test/test_local_authority_geography.py
```

Must prove:

- local authority names normalize consistently;
- constituency joins are weighted or explicitly marked unknown;
- no unweighted LA→constituency rollup is shown as exact.

```text
test/test_housing_grants.py
```

Must prove:

- allocated and paid values are not unioned as one metric;
- grant types are normalized but original values preserved;
- no individual-level rows exist.

### Judiciary tests

```text
test/test_judiciary_privacy.py
```

Must prove:

- protected categories are removed;
- natural-person names are anonymised;
- raw case text is absent;
- source hash/url present;
- page does not render un-anonymised detail.

---

## 9. UI acceptance criteria

### Public Money & Procurement tile

Minimum viable page is acceptable when:

- supplier summary renders;
- authority summary renders;
- CPV summary renders;
- lobbying overlap renders with non-causal copy;
- value-kind caveat is visible before any money table;
- missing data state is explicit;
- every table has source/caveat text.

### Local Authority & Housing tile

Minimum viable page is acceptable when:

- user can select a local authority;
- SSHA net need summary renders;
- at least one trend chart/table renders;
- AFS or budget/actual placeholder state is clear;
- housing grant/delivery sections can be hidden if not ready;
- no false constituency mapping is shown;
- caveat explains SSHA net need exclusions.

### Judiciary tile

Stable only when:

- privacy tests pass;
- page copy avoids performance-ranking language;
- source unavailable state is explicit;
- protected categories are demonstrably absent from public output.

---

## 10. Suggested implementation sequence

### Sprint 0 — Hygiene and routing decisions

1. Confirm whether Judiciary is supposed to be visible in the deployed app.
2. Decide names:
   - `Public Money & Procurement`
   - `Local Authority & Housing`
3. Add route placeholders only if missing data states are clean.
4. Create common UI component for source/caveat panels.

### Sprint 1 — Procurement page skeleton

Deliver:

```text
utility/pages_code/procurement.py
```

Use existing data-access functions.

Acceptance:

- supplier table,
- authority table,
- CPV table,
- lobbying overlap table,
- caveat panel,
- no naive sum.

### Sprint 2 — Housing SSHA extractor

Deliver:

```text
extractors/housing_ssha_extract.py
data/silver/parquet/housing_ssha_la_fact.parquet
data/_meta/housing_ssha_coverage.json
sql_views/housing_ssha_la_summary.sql
dail_tracker_core/queries/local_authority_housing.py
```

Do not create a broad page yet; first validate the source.

### Sprint 3 — Local Authority & Housing page skeleton

Deliver:

```text
utility/pages_code/local_authority_housing.py
utility/data_access/local_authority_housing_data.py
```

Render:

- local authority selector,
- SSHA headline,
- trend,
- caveat,
- placeholders for AFS/procurement/audit.

### Sprint 4 — LA payments promotion

Deliver:

```text
sql_views/publicmoney_la_payments.sql
test/test_la_payments_schema.py
test/test_la_payments_privacy.py
```

Only then add LA payments to Local Authority & Housing page.

### Sprint 5 — Housing grants

Deliver:

```text
extractors/housing_adaptation_grants_extract.py
sql_views/housing_grants_la_summary.sql
```

Add to Local Authority & Housing page.

### Sprint 6 — Public Body / Supplier Dossier

Turn Procurement page from ranking tables into dossiers:

```text
Supplier detail page/state
Public body detail page/state
```

Later use these for:

- C&AG,
- PAC,
- board minutes,
- local audit,
- FOI leads.

### Sprint 7 — Judiciary hardening and CPO probe

1. Harden legal diary privacy tests.
2. Probe CPO cases at scheme level only.
3. Keep CPO out of public UI until privacy and address-leak guards pass.
4. Later connect CPO to Local Authority & Housing / Infrastructure profiles.

---

## 11. Risk register

| Risk | Severity | Domain | Mitigation |
|---|---:|---|---|
| Awarded values misread as spend | High | Procurement | Value-kind legend; never show naive totals; sum only `value_safe_to_sum` |
| Lobbying overlap misread as causation | High | Procurement/Lobbying | Neutral “appears in both records” copy; no causal wording |
| Sole traders / individuals exposed | High | Procurement/LA payments | Privacy classification; public_display flag; tests |
| SSHA net need misread as total housing need | Medium | Housing | Caveat: excludes HAP/RAS/SHCEP/current supports/transfers |
| LA-to-constituency joins overstated | High | Housing/Local | Use weighted bridge or label approximate/unknown |
| Judiciary exposes personal case details | Critical | Judiciary | Hard privacy exceptions, no asserts, golden tests |
| Courts page becomes judge ranking | Medium | Judiciary | Use neutral listing/session language |
| Public money values mixed across meanings | High | All money domains | Mandatory `value_kind` and `realisation_tier` |
| Data source outages look like zero records | Medium | All | QueryResult-aware page states |
| 31 local authorities drift formats | Medium | LA procurement | Config-driven parsers, per-council coverage JSON |

---

## 12. Naming recommendations

### Routes

```text
/rankings-procurement
/rankings-local-authority-housing
/rankings-judiciary
```

### Page titles

```text
Public Money & Procurement
Local Authority & Housing
Courts & Judiciary
```

### Future profile names

```text
Supplier Dossier
Public Body Profile
Local Authority Profile
Infrastructure Project Profile
Housing Context
```

---

## 13. Final recommendation

Build the second planned tile as **Local Authority & Housing**, not just “Housing.”

Reason:

1. The repo already has local-authority financial source work: AFS, LA PO/payments, LA budget scoping, LA audit scoping.
2. Housing data is mostly local-authority-level context.
3. A pure Housing page would duplicate public statistical dashboards.
4. A Local Authority & Housing tile becomes a unique civic accountability product by combining:
   - local need,
   - local spending,
   - local procurement/payments,
   - local audits,
   - local TD/constituency political activity.

Build **Public Money & Procurement** first because its backend is closest to ready.

Then build **Local Authority & Housing** because it is the best strategic second tile.

Keep **Courts & Judiciary** visible only if privacy hardening is complete; otherwise treat it as beta/guarded.

---

## 14. One-page implementation checklist

```text
[ ] Confirm Judiciary deployed/surfaced status
[ ] Add procurement page skeleton
[ ] Add value-kind legend component
[ ] Make procurement data-access expose QueryResult-aware failures
[ ] Add procurement page smoke tests
[ ] Build SSHA extractor from machine-readable Housing Agency tables
[ ] Add housing SSHA SQL views
[ ] Add Local Authority & Housing page skeleton
[ ] Add local authority selector and caveat panel
[ ] Add LA/constituency geography bridge design doc
[ ] Promote LA payments only after privacy/schema tests
[ ] Add Housing Adaptation Grants extractor
[ ] Add local authority audit metadata probe
[ ] Replace any judiciary privacy asserts with explicit exceptions
[ ] Add judiciary privacy golden tests
[ ] Probe CPO cases only at scheme level, with address/person leak guard
```
