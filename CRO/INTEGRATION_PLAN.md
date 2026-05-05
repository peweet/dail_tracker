# CRO Companies Register × Public Register of Charities — Integration Plan for Dáil Tracker

**Author:** Senior data analyst / data architect / analytics engineer brief
**Datasets:** `CRO/companies.csv` (815k rows), `CRO/20260426-public-register-of-charities.xlsx` (Public Register 14,448 rows + Annual Reports 82,894 rows)
**Project:** Dáil Tracker (Irish parliamentary civic accountability)
**Refresh date of source:** 2026-04-26 (charity register effective date) / 2026-05-04 (CRO snapshot file mtime)

---

## 1. Executive Summary

### What the datasets are

| Dataset | Grain | Useful for |
|---|---|---|
| **CRO companies** | one row per company (815k) | identity, status, sector code, registration date, address — **commercial entities** |
| **Charities Public Register** | one row per charity (14.4k) | identity, classification, governing form, CRO link, trustees — **civil-society entities** |
| **Charities Annual Reports** | one row per (charity, financial period) (83k) | gov funding share, gross income, employees buckets, surplus/deficit — **financial profile** |

### Can they be joined?

**Yes — three tiers, two of them deterministic.**

| Tier | Method | Match rate | Use |
|---|---|---|---|
| **Tier A (deterministic)** | `Charity.CRO Number = CRO.company_num` | **99.2%** of charities that publish a CRO number (5,686 / 5,731) | gold-standard cross-register identity link |
| **Tier B (deterministic)** | `Charity.RCN ↔ Lobbyist.lobbyist_name` after name normalisation | 16% exact, ~30% with fuzzy + manual override | links lobbying.ie filers to financial profile |
| **Tier C (deterministic)** | `CRO.company_name ↔ Lobbyist.lobbyist_name` after name normalisation | 36% exact, ~55% with fuzzy + manual override | links commercial lobbyists to status/sector |

A & B together cover ~52% of the 1,777 distinct lobbying organisations and a much higher share of *return volume* (top-200 active orgs are mostly findable).

### Most valuable entities

1. **Lobbyist organisation** — currently a free-text string on lobbying.ie returns. Becomes a first-class entity with status, age, sector, funding profile, trustees, and source-of-truth registry links.
2. **Charity** — a new first-class entity with financial time series, government dependence, and trustee names that can cross-reference to TDs.
3. **Trustee** — derivable from the charity register; previously absent from the project. Critical for conflict-of-interest analytics.

### Most valuable user-facing metrics (top 5)

1. **`gov_funded_share_latest`** — share of charity income from government / local authorities / other public bodies. Most policy-relevant single number this dataset can produce.
2. **`entity_age_years`** — `today − company_reg_date`. Reveals newly-incorporated lobbying vehicles.
3. **`entity_status`** — Normal / Liquidation / Dissolved / Deregistered. Trust signal on every org card.
4. **`gross_income_latest_eur`** — scale signal alongside lobbying activity.
5. **`td_trusteeship_link`** — flag on a TD record when they appear as a charity trustee.

### Biggest data quality risks

| Risk | Severity |
|---|---|
| Lobbyist-name → register match has ~50% ceiling. Long-tail orgs unmatched. | High |
| Annual-report financials are self-reported; coverage drops to 65% in 2025 (lag). | Medium |
| `Number of Employees` is **bucketed text** (`1-9`, `10-19`, `5000+`), not numeric. | Medium |
| 22,962 CRO rows have `********NO ADDRESS DETAILS*******` placeholder. | Low |
| 157 CRO rows have `reg_date < 1900` (legacy garbage). | Low |
| Trustee names are free-text in one semicolon-delimited string per charity; parsing is heuristic. | Medium |
| HSE / HEA / Pobal / Tusla / hospitals all classified as charities — distinct from civil society. | High *for interpretation* |

### Best integration path

1. **Land bronze**: keep originals as-delivered (CSV + XLSX).
2. **Silver via `pipeline_sandbox/`**: per-source normalisation parquets + entity-resolution parquet + override CSV.
3. **Gold via `sql_views/`**: extend `v_lobbying_org_index`, `v_lobbying_clients`, `v_member_interests`; new `v_charity_register`, `v_member_charity_trusteeships`.
4. **UI**: surface enrichment as **pills + badges + drill-downs** — never as new pages early on. Pills are the lowest-risk, highest-trust delivery.

### Highest-impact UX opportunities

1. **Funding-source pill** on every lobbying org card (`State-funded 87%`, `Mostly donations`, `Self-funded`).
2. **Status pill** with trust colouring (active / dissolved / deregistered).
3. **Trusteeship strip** on member profile pages — a new public-interest disclosure surface.
4. **"Newly-formed lobbyist" callout** when `reg_date` is within 24 months of `first_return_date`.
5. **Sector facet** on the lobbying org index — replaces today's free-text sector hardcode.

### Recommended next step (one sentence)

Build the Week-1 MVP slice — `pipeline_sandbox/charity_normalise.py` + a single `gov_funded_share_latest` column on `v_lobbying_org_index` rendered as one funding-pill on the org leaderboard cards — to prove the join end-to-end before committing to the wider feature set.

---

## 2. Data Dictionary

### 2.1 `companies.csv` (CRO Companies Register)

```yaml
dataset_name: cro_companies
description: Snapshot of the Irish Companies Registration Office register at 2026-05-04
grain: one row per registered company
primary_entity: company
row_count: 814_897
column_count: 21
columns:
  company_num:
    inferred_type: integer
    semantic_type: canonical_company_id
    nullable: false
    null_rate: 0.0
    unique_count: 813_193
    example_values: [73848, 73914, 559001]
    suspected_role: primary key (note: 1,704 duplicates exist — same company_num across different historical name revisions)
    data_quality_notes: Should be unique; small duplicate count likely reflects amalgamated rows. Investigate before using as PK.
    recommended_app_usage: canonical join key to charity register CRO Number
  company_name:
    inferred_type: string
    semantic_type: legal_entity_name
    nullable: false
    null_rate: 0.0
    unique_count: 811_537
    example_values: ["VIDEO RELAYS LIMITED", "WARD HOTELS LIMITED"]
    suspected_role: display name
    data_quality_notes: All upper case; encodes legal suffix (LIMITED / DAC / PLC / CLG)
    recommended_app_usage: title-case for display, normalise for fuzzy match against lobbyist names
  company_status_code:
    inferred_type: integer
    semantic_type: status_lookup_code
    nullable: false
    null_rate: 0.0
    unique_count: 33
    example_values: [1158, 1100]
    suspected_role: machine-readable status
    recommended_app_usage: prefer human label `company_status` for display; keep code for joins/exports
  company_status:
    inferred_type: string
    semantic_type: enum_status
    nullable: false
    null_rate: 0.0
    unique_count: 23
    example_values: [Dissolved, Normal, Liquidation, "Strike Off Listed"]
    suspected_role: status human label
    data_quality_notes: 55% Dissolved / 40% Normal / 5% other states. Trailing whitespace seen on "Normal " — strip on ingest.
    recommended_app_usage: drives status pill on lobbyist org cards
  company_type_code:
    inferred_type: integer
    semantic_type: type_lookup_code
    nullable: false
    null_rate: 0.0
    unique_count: 45
  company_type:
    inferred_type: string
    semantic_type: legal_form
    nullable: false
    null_rate: 0.0
    unique_count: 44
    example_values: ["LTD - Private Company Limited by Shares", "CLG - Company Limited by Guarantee", "PLC - Public Limited Company"]
    recommended_app_usage: faceted filter on lobbying org explorer; CLG identifies non-profit/charity-style company
  company_reg_date:
    inferred_type: date (string ISO)
    semantic_type: incorporation_date
    nullable: true
    null_rate: 2.0
    unique_count: 20_491
    example_values: ["1980-02-21"]
    data_quality_notes: 157 rows < 1900-01-01 (legacy garbage); range otherwise 1900–2026
    recommended_app_usage: derive `entity_age_years`; flag newly-incorporated lobbying vehicles (<2y old at first return)
  last_ar_date:
    semantic_type: latest_annual_return_date
    null_rate: 26.5
    recommended_app_usage: compliance staleness signal; if old, the company may be drifting toward strike-off
  company_address_1..4:
    semantic_type: postal_address_lines
    null_rate: [0.0, 14.8, 49.4, 3.1]
    data_quality_notes: 22,962 rows contain "********NO ADDRESS DETAILS*******" placeholder; filter on ingest
    recommended_app_usage: secondary detail; not primary
  comp_dissolved_date:
    semantic_type: dissolution_date
    null_rate: 42.8
    recommended_app_usage: provenance for Dissolved status; sparkline timeline
  nard:
    semantic_type: next_annual_return_date
    null_rate: 3.1
    recommended_app_usage: low-priority compliance signal
  last_accounts_date:
    semantic_type: latest_filed_accounts_date
    null_rate: 39.7
  company_status_date:
    semantic_type: status_change_date
    null_rate: 39.0
    recommended_app_usage: cross-reference with Iris Oifigiúil dates for provenance
  nace_v2_code:
    inferred_type: float (should be integer code)
    semantic_type: industry_classification_code
    null_rate: 69.0
    unique_count: 583
    example_values: [6420.0, 7022.0, 5610.0]
    data_quality_notes: Encoded as float — strip ".0" on read; NACE Rev.2 4-digit. Coverage skewed to recently-incorporated companies.
    recommended_app_usage: maps to NACE label table for sector pill / facet
  eircode:
    semantic_type: postal_code
    null_rate: 65.7
    unique_count: 142_417
    example_values: [D02R296, T12X7H0]
    data_quality_notes: Free-form text — some have inconsistent spacing/casing. Routing key (first 3 chars) drives geographic facets.
    recommended_app_usage: routing-key prefix → constituency lookup (when joined with eircode-to-Dáil-constituency table)
  company_name_eff_date:
    semantic_type: name_effective_date
    recommended_app_usage: provenance only
  company_type_eff_date:
    semantic_type: type_effective_date
    recommended_app_usage: provenance only
  princ_object_code:
    semantic_type: legacy_classification_code
    null_rate: 43.7
    unique_count: 547
    data_quality_notes: CRO's own legacy classification, predates NACE adoption. Lower information value than NACE.
    recommended_app_usage: fallback only when nace_v2_code is null
```

### 2.2 `Public Register` sheet (Charities Regulator)

```yaml
dataset_name: charities_public_register
description: Public register of charitable organisations in Ireland, effective 2026-04-26
grain: one row per registered or deregistered charity
primary_entity: charity
row_count: 14_448
column_count: 13
columns:
  Registered Charity Number:
    inferred_type: integer
    semantic_type: canonical_charity_id (RCN)
    nullable: false
    null_rate: 0.0
    unique_count: 14_448
    suspected_role: primary key (verified unique)
    recommended_app_usage: stable join key for charity ↔ annual reports ↔ trustees
  Registered Charity Name:
    inferred_type: string
    semantic_type: legal_entity_name
    nullable: false
    null_rate: 0.0
    unique_count: 14_216
    data_quality_notes: 232 collisions on name (different RCNs sharing a registered name; usually denominational schools). Use RCN, never name, as PK.
  "Also Known As":
    semantic_type: trading_or_alias_name
    null_rate: 75.7
    recommended_app_usage: include in name-match candidate set against lobbyist_name
  Status:
    semantic_type: enum_status
    null_rate: 0.0
    unique_count: 3
    example_values: ["Registered", "Deregistered S40 by Revenue", "Deregistered"]
    recommended_app_usage: 21% of charities are deregistered; flag visibly
  "Charity Classification: Primary [Secondary (Sub)]":
    semantic_type: hierarchical_classification
    null_rate: 63.0
    unique_count: 1_101
    example_values: ["Social and community services [Child or youth services (Child or youth organisation)]"]
    data_quality_notes: Three-level hierarchy encoded as a single string with brackets. Parse into primary / secondary / sub before use.
    recommended_app_usage: civic-society sector facet (parallel to NACE for commercial)
  Primary Address:
    semantic_type: postal_address
    null_rate: 1.6
    unique_count: 13_975
    data_quality_notes: Comma-separated free-form (street, locality, county, country). Last token is usually country.
    recommended_app_usage: derive county; geocode optionally
  "Also Operates In":
    semantic_type: secondary_jurisdictions
    null_rate: 97.4
    recommended_app_usage: low value
  "Governing Form":
    semantic_type: legal_form
    null_rate: 0.0
    unique_count: 30
    example_values: ["CLG - Company Limited by Guarantee", "Board of Management (Primary School)", "Association"]
    recommended_app_usage: distinguishes incorporated vs unincorporated; "Board of Management" identifies state schools
  CRO Number:
    semantic_type: foreign_key_to_cro_companies
    null_rate: 59.7
    unique_count: 5_802
    data_quality_notes: A handful of placeholder zeros and self-references (RCN written into CRO field). 99.2% of valid values match a CRO row.
    recommended_app_usage: deterministic Tier A join key
  "Country Established":
    null_rate: 0.0
    unique_count: 13
    recommended_app_usage: filter "established outside Ireland" cohort
  "Charitable Purpose":
    null_rate: 6.1
    unique_count: 824
    recommended_app_usage: classification supplement; mostly boilerplate categories
  "Charitable Objects":
    null_rate: 13.5
    unique_count: 8_569
    data_quality_notes: Long free-text; legal boilerplate. Low signal beyond classification.
    recommended_app_usage: full-text search only; not for cards
  "Trustees (Start Date)":
    null_rate: 13.8
    unique_count: 12_452
    example_values: ["Larry Frain (Trustee Chairperson) (14/03/2013); Noreen Duffy (14/03/2013); Mr Ian Gourlay (25/05/2021)"]
    data_quality_notes: Semicolon-delimited string; per-trustee format `<name> [(role)] (DD/MM/YYYY)`. Names sometimes carry titles (Mr/Ms/Dr) and double spaces.
    recommended_app_usage: parse into long table (RCN × trustee). Cross-match against TD names for conflict-of-interest analytics.
```

### 2.3 `Annual Reports` sheet

```yaml
dataset_name: charities_annual_reports
description: Per-period self-reported financial filings by charities
grain: one row per (charity, financial_period_end_date)
primary_entity: charity_filing
row_count: 82_894
column_count: 28
distinct_charities_filing: 10_387 of 14_448 (71.9%)
mean_filings_per_charity: 8.0
period_coverage: 2014–2026 (current-year coverage drops sharply to ~10% by 2025)

columns:
  Registered Charity Number (RCN):
    semantic_type: foreign_key_to_public_register
    suspected_role: composite-PK component
  Period Start Date / Period End Date:
    semantic_type: date
    suspected_role: composite-PK component (with RCN). Different charities have different financial year-ends.
  Report Activity:
    semantic_type: classification_text (mirrors charity classification)
    null_rate: low
  Activity Description:
    semantic_type: free_text_summary
    null_rate: low
    recommended_app_usage: full-text search; not for cards
  Beneficiaries:
    semantic_type: semicolon_delimited_tags
    recommended_app_usage: tag cloud; faceted filter
  "Income: Government or Local Authorities":
    semantic_type: monetary_eur
    null_rate: 22–58% by year (rising to ~78% coverage in 2024)
    recommended_app_usage: numerator of gov_funded_share
  "Income: Other Public Bodies":
    semantic_type: monetary_eur
    recommended_app_usage: second numerator term (HSE/HEA/local-authority subsidiaries)
  "Income: Philantrophic Organisations":
    semantic_type: monetary_eur (note: typo in source — "Philantrophic")
    recommended_app_usage: distinguishes foundation funding
  "Income: Donations":
    semantic_type: monetary_eur
  "Income: Trading and Commercial Activities":
    semantic_type: monetary_eur
  "Income: Other Sources":
    semantic_type: monetary_eur
  "Income: Bequests":
    semantic_type: monetary_eur
  Gross Income:
    semantic_type: monetary_eur
    suspected_role: denominator for share metrics
  Gross Expenditure:
    semantic_type: monetary_eur
  "Surplus / (Deficit) for the Period":
    semantic_type: monetary_eur (signed)
  Cash at Hand and in Bank / Other Assets / Total Assets / Total Liabilities / Net Assets:
    semantic_type: balance_sheet_eur
  "Number of Employees":
    inferred_type: string (BUCKETED, NOT NUMERIC)
    semantic_type: ordinal_band
    unique_count: 10
    example_values: ["1-9", "10-19", "20-49", "50-249", "250-499", "500-999", "1000-4999", "5000+", "NONE"]
    data_quality_notes: Bucketed self-report. Cannot compute exact headcount or per-employee ratios.
    recommended_app_usage: ordinal display only — pill or bar
  Number of Full-Time Employees / Number of Part-Time Employees:
    inferred_type: integer (numeric, populated since ~2024 only)
    null_rate: very high (only ~1k rows have it)
  Number of Volunteers:
    semantic_type: ordinal_band (same buckets as employees)
```

---

## 3. Entity & Relationship Discovery

### 3.1 Canonical entities

```yaml
entities:
  - name: Company
    source_dataset: cro_companies
    entity_type: legal_entity
    likely_primary_key: company_num
    alternate_keys: [company_name + reg_date]
    display_name_fields: [company_name]
    timestamp_fields: [company_reg_date, company_status_date, comp_dissolved_date, last_ar_date]
    status_fields: [company_status]
    relationship_fields: [eircode, nace_v2_code]
    confidence: high

  - name: Charity
    source_dataset: charities_public_register
    entity_type: civil_society_or_state_adjacent_entity
    likely_primary_key: rcn
    alternate_keys: [name + governing_form]
    display_name_fields: [registered_charity_name, also_known_as]
    timestamp_fields: []  # register has no temporal column
    status_fields: [status]
    relationship_fields: [cro_number, governing_form, classification]
    confidence: high

  - name: CharityFiling
    source_dataset: charities_annual_reports
    entity_type: financial_event
    likely_primary_key: (rcn, period_end_date)
    description: A self-reported financial period for a charity
    confidence: high

  - name: Trustee  (DERIVED)
    source_dataset: charities_public_register (parsed)
    entity_type: person
    likely_primary_key: surrogate_key (rcn, trustee_name, start_date)
    display_name_fields: [trustee_name]
    confidence: medium
    notes: |
      Names are free-text and contain titles (Mr/Ms/Dr) and inconsistent spacing.
      No stable person-ID; deduplication across charities is heuristic.

  - name: Lobbyist  (EXISTING in project)
    source_dataset: lobbying.ie returns
    entity_type: legal_entity_or_civil_society_or_individual
    likely_primary_key: lobbyist_name (free text, problematic)
    notes: |
      Today this is a string. The integration upgrades it to a resolved entity
      with optional links to Company and/or Charity.

  - name: Member  (EXISTING in project)
    source_dataset: dail member registry
    primary_key: unique_member_code
    notes: |
      Joinable to Trustee by name only — no shared ID. Match must be probabilistic with confidence bands.
```

### 3.2 Integrated entity-relationship model

```
                     ┌────────────────┐
                     │     Member     │  (existing)
                     └───────┬────────┘
                             │ name match (probabilistic)
                             │
              ┌──────────────┴──────────────┐
              │                             │
              │           ┌─────────────────▼──────────────────┐
              │           │             Trustee                │
              │           │   (rcn, trustee_name, role, start) │
              │           └─────────────────┬──────────────────┘
              │                             │ rcn
              │                             │
   ┌──────────▼─────────┐           ┌───────▼────────┐
   │     Lobbyist       │           │    Charity     │
   │  (resolved entity) │◀──────────┤   (RCN PK)     │
   └──────────┬─────────┘  rcn match └──────┬─────────┘
              │                              │ cro_number (99.2% deterministic)
              │ cro match                    │
              │                       ┌──────▼─────────┐
              └──────────────────────▶│    Company     │
                                      │ (company_num PK)│
                                      └────────────────┘
                                              │
                                              │ rcn (1:N)
                                      ┌───────▼────────┐
                                      │ CharityFiling  │
                                      │  (rcn × period)│
                                      └────────────────┘
```

### 3.3 Bridge & lookup tables

```yaml
bridge_tables:
  - lobbyist_entity_resolution:
      grain: one row per distinct lobbyist_name in lobbying.ie
      columns: [lobbyist_name, company_num_match, rcn_match, match_method,
                match_quality, evidence, manual_override]
      purpose: links the free-text lobbyist string to canonical company / charity rows

  - member_charity_trusteeships:
      grain: one row per (member_id, rcn)
      columns: [member_id, rcn, trustee_name_in_register, role, start_date, match_confidence]
      purpose: surfaces TD trusteeships on member profile, with evidence

lookup_tables:
  - nace_v2_labels:
      grain: NACE 4-digit code
      columns: [nace_code, nace_label, nace_section]
      source: external (Eurostat NACE Rev.2). One-time CSV import.

  - eircode_routing_to_constituency:
      grain: 3-char routing key
      columns: [routing_key, locality, county, dail_constituency]
      source: Ordnance Survey Ireland / external mapping
      coverage: partial — eircode itself is 34% present in CRO

  - charity_classification_hierarchy:
      grain: parsed primary / secondary / sub triple
      columns: [primary, secondary, sub]
      source: parsed in-place from charity register
```

---

## 4. Join Condition Discovery (ranked)

### 4.1 Tier A — deterministic, high confidence

```yaml
- join_candidate: charity_to_company_via_cro_number
  left_dataset: charities_public_register
  left_columns: [CRO Number]
  right_dataset: cro_companies
  right_columns: [company_num]
  join_type: many-to-one
  match_method: integer equality
  estimated_match_rate: 99.2% of charities that publish a CRO number (5,686 / 5,731)
  unmatched_left_count: 45 (placeholder zeros, self-references, edge cases)
  unmatched_right_count: ~809,000 (most companies are not charities — expected)
  duplicate_match_risk: very_low
  false_positive_risk: very_low
  false_negative_risk: 8,626 charities have no CRO number (e.g. trust-form, unincorporated)
  confidence: high
  recommended_use: PRIMARY join — every CLG/charity-form Lobbyist gets a single resolved row across both registers
  validation_checks:
    - distinct(charity.cro_number) ≤ count(joined_rows)
    - charity.cro_number = 0 OR null → unmatched (expected)
    - CRO Number that equals RCN → flag as data-quality issue
  fallback_strategy: name-match (Tier B/C)

- join_candidate: filing_to_charity_via_rcn
  left: charities_annual_reports
  left_columns: [Registered Charity Number (RCN)]
  right: charities_public_register
  right_columns: [Registered Charity Number]
  join_type: many-to-one
  match_method: integer equality
  estimated_match_rate: 100% (RCN is the canonical charity ID across the regulator's own files)
  confidence: high
  recommended_use: build CharityFiling fact table

- join_candidate: trustee_to_charity_via_rcn
  source: parsed Trustees (Start Date) field
  match_method: derived from same row
  confidence: high
  recommended_use: trustee long table; backbone of TD-conflict feature
```

### 4.2 Tier B — probabilistic name matches against existing project data

```yaml
- join_candidate: lobbyist_to_charity_via_name
  left_dataset: data/silver/lobbying/reach_by_lobbyist
  left_columns: [lobbyist_name]
  right_dataset: charities_public_register
  right_columns: [Registered Charity Name, Also Known As]
  join_type: many-to-one (after dedupe on charity name)
  match_method: normalised string equality, then fuzzy
  normalization_steps:
    - upper-case
    - strip punctuation [.,&'"]
    - drop legal suffixes [LIMITED, LTD, DAC, PLC, CLG, COMPANY, COMPANY LIMITED BY GUARANTEE, UNLIMITED COMPANY, IRELAND, GROUP, HOLDINGS, THE, OF]
    - collapse whitespace
  estimated_match_rate: 16% exact, ~30% with rapidfuzz token-set + manual override of top 50
  duplicate_match_risk: low (charity names are mostly unique)
  false_positive_risk: low for top-200, medium in long tail
  false_negative_risk: medium — unincorporated bodies, alliances, regional branches won't match
  confidence: medium
  recommended_use: layer over Tier A; fills gaps for charities that don't publish a CRO number
  validation_checks:
    - top-50 by return_count manually reviewed
    - reject ambiguous matches where two charities have same normalised name

- join_candidate: lobbyist_to_company_via_name
  left_dataset: data/silver/lobbying/reach_by_lobbyist
  right_dataset: cro_companies
  match_method: as above, plus filter to status=Normal in match candidates first
  estimated_match_rate: 36% exact, ~55% with fuzzy + manual override
  duplicate_match_risk: medium — corporate groups have many similarly-named subsidiaries (e.g. "ENTERPRISE HOLDINGS" → "ENTERPRISE IRELAND HOLDINGS")
  false_positive_risk: medium — must prefer Status=Normal AND most-recent reg_date when multiple candidates
  confidence: medium
  recommended_use: PRIMARY for commercial lobbyists; complement to Tier B for charity lobbyists
```

### 4.3 Tier C — exploratory / weak

```yaml
- join_candidate: trustee_to_member_via_name
  left: derived Trustee long table
  right: dail member registry
  match_method: normalised name match (using project's existing normalise_df_td_name helper)
  estimated_match_rate: very low — only TDs who are also charity trustees match
  false_positive_risk: high — common names; "John Murphy" appears in both registers many times
  confidence: low without disambiguation context
  recommended_use: ONLY surface when match score is high AND charity is non-trivial (income > €100k or classification ≠ school)
  fallback_strategy: hide unless user is on a member profile page and the system can show the evidence (charity name, role, start_date) for one-click manual confirmation

- join_candidate: cro_address_eircode_to_constituency
  left: cro_companies.eircode (34% coverage)
  right: external eircode→Dáil constituency lookup table (not yet sourced)
  match_method: routing-key prefix lookup
  confidence: medium — 34% partial coverage limits utility
  recommended_use: secondary facet on constituency profile pages only; not headline feature
```

### 4.4 Match-confidence bands

```yaml
match_confidence:
  exact_id_match: 1.00          # Tier A (CRO Number → company_num)
  exact_name_normalised: 0.92    # Tier B/C (after stop-word strip)
  fuzzy_token_set_high: 0.78     # rapidfuzz token_set_ratio ≥ 92
  fuzzy_token_set_low: 0.55      # ratio 80–92 — must show in review queue
  weak_match_requires_review: true   # below 80
display_threshold:
  user_facing_default: 0.78      # only show resolved entity if confidence ≥ this
  reviewer_queue: [0.55, 0.78]   # band shown to a maintainer for manual decision
explainability_field: match_evidence  # always populated: "exact CRO Number 19920" / "name-normalised: macra na feirme"
```

### 4.5 SQL sketch for the recommended Tier A join

```sql
-- silver/charity_resolved.sql (DuckDB / Polars compatible)
WITH charity_clean AS (
    SELECT
        rcn                                                    AS rcn,
        TRIM(registered_charity_name)                          AS charity_name,
        NULLIF(also_known_as, '')                              AS also_known_as,
        UPPER(status)                                          AS status,
        governing_form,
        TRY_CAST(NULLIF(cro_number, 0) AS INTEGER)             AS cro_number_clean,
        primary_address,
        country_established,
        classification_primary,
        classification_secondary,
        classification_sub
    FROM read_parquet('data/silver/charities/register.parquet')
)
SELECT
    c.rcn,
    c.charity_name,
    c.also_known_as,
    c.status,
    c.governing_form,
    c.classification_primary,
    c.classification_secondary,
    c.classification_sub,
    c.cro_number_clean                          AS cro_number,
    co.company_status,
    co.company_type,
    co.company_reg_date,
    co.nace_v2_code,
    co.eircode,
    CASE WHEN c.cro_number_clean IS NOT NULL AND co.company_num IS NOT NULL
         THEN 'cro_number_exact'
         ELSE NULL
    END                                          AS link_method
FROM charity_clean c
LEFT JOIN read_parquet('data/silver/cro/companies.parquet') co
       ON c.cro_number_clean = co.company_num;
```

---

## 5. Data Quality Issues

### 5.1 Blocking issues (must fix before integration)

```yaml
- data_quality_issue:
    severity: blocking
    affected_dataset: cro_companies
    affected_columns: [company_address_1]
    affected_rows_estimate: 22_962
    description: Placeholder string "********NO ADDRESS DETAILS*******"
    user_experience_impact: Card detail panels would render asterisks
    recommended_fix: Replace with NULL on ingest
    transformation_rule: address_1 = NULL WHERE address_1 LIKE '*%NO ADDRESS%*'
    should_block_integration: yes

- data_quality_issue:
    severity: blocking
    affected_dataset: charities_annual_reports
    affected_columns: [Number of Employees, Number of Volunteers]
    description: Stored as text band ('1-9', '5000+'), not numeric
    user_experience_impact: Naïve "average employees" calculation produces nonsense or NaN
    recommended_fix: Parse into ordinal band (employees_band_min, employees_band_max, employees_band_label) and forbid arithmetic
    should_block_integration: yes
```

### 5.2 Warning-level

```yaml
- data_quality_issue:
    severity: warning
    affected_dataset: cro_companies
    affected_columns: [company_reg_date]
    affected_rows_estimate: 157
    description: Dates < 1900-01-01 (legacy artefacts)
    recommended_fix: Drop or quarantine; flag with reg_date_invalid_flag

- data_quality_issue:
    severity: warning
    affected_dataset: cro_companies
    affected_columns: [company_status]
    description: Trailing whitespace on "Normal "
    recommended_fix: TRIM() on ingest

- data_quality_issue:
    severity: warning
    affected_dataset: charities_public_register
    affected_columns: [CRO Number]
    description: Some rows have CRO Number = 0 or = RCN value (charity self-reference)
    affected_rows_estimate: ~45
    recommended_fix: NULLIF(cro_number, 0); drop rows where cro_number = rcn before join

- data_quality_issue:
    severity: warning
    affected_dataset: charities_public_register
    affected_columns: [Charity Classification: Primary [Secondary (Sub)]]
    description: Three-level hierarchy collapsed into one delimited string with 63% null rate
    recommended_fix: Parse into three columns; consider classification optional facet not headline

- data_quality_issue:
    severity: warning
    affected_dataset: charities_public_register
    affected_columns: [Trustees (Start Date)]
    description: Free-text; titles, double spaces, inconsistent date format
    recommended_fix: Robust regex parser with fallthrough to "raw" preservation; mark parsed-or-raw flag
```

### 5.3 Acceptable quirks

- 65% of CRO `nace_v2_code` are null (older companies pre-date NACE adoption). Accept; render `Unclassified` pill.
- 75% of charity `Also Known As` is null. Expected.
- ~21% of charities are deregistered. Treat as a first-class category, not noise.
- HSE / hospitals appear as charities. Surface as `state_adjacent` rather than treating them as ordinary NGOs.

### 5.4 Privacy & compliance

- Trustee names are public per the Charities Act 2009. No re-publication concern.
- `Also Known As` may include personal trading names of small operators — same.
- No need to mask any field at the registry level. UI should still treat the trustee→TD match as a *suggested link with evidence* rather than an authoritative claim.

---

## 6. Feature & Metric Catalogue

### 6.1 User-facing metrics

```yaml
metrics:
  - name: gov_funded_share_latest
    label: "Government-funded share"
    description: Share of charity's gross income from government and other public bodies in its latest filed period.
    formula: (income_government + income_other_public_bodies) / gross_income
    source_columns: [Income: Government or Local Authorities, Income: Other Public Bodies, Gross Income]
    required_joins: charity_resolved
    aggregation_level: charity (latest period)
    refresh_frequency: weekly (when register refreshed)
    user_value: Single most policy-relevant number — instantly tells reader whether a lobbying charity is state-underwritten
    caveats: Self-reported; coverage drops for current year (use latest_period_with_data, not current_year)
    display_format: percent_0dp ("87%")
    recommended_visualization: badge with traffic-light colour band
    explainability_template: "Of €{gross_income:,.0f} gross income filed for period ending {period_end}, {gov_eur:,.0f} ({gov_share}%) came from government or other public bodies."

  - name: entity_age_years
    label: "Years on register"
    formula: years_between(today, company_reg_date) -- or rcn_first_filing_date for charities
    source_columns: [company_reg_date]
    user_value: Distinguishes established institutions from newly-formed entities
    display_format: integer with suffix ("12 yrs")
    caveats: Re-registrations after restructuring reset the date

  - name: years_lobbying
    label: "Years filing lobbying returns"
    formula: years_between(last_return_date, first_return_date)
    user_value: Activity persistence

  - name: gross_income_latest_eur
    label: "Latest reported income"
    display_format: currency_eur_short ("€11.4m")
    caveats: Self-reported; may lag

  - name: surplus_deficit_latest
    label: "Surplus / (Deficit)"
    display_format: signed_currency_eur_short ("+€340k", "−€127k")

  - name: returns_per_year
    label: "Lobbying returns per year (avg)"
    formula: return_count / years_lobbying
    user_value: Activity intensity

  - name: politicians_to_returns_ratio
    label: "Reach per return"
    formula: distinct_politicians_targeted / return_count
    user_value: Distinguishes broad-net vs targeted lobbying styles

  - name: state_adjacent_flag
    label: "State-adjacent body"
    formula: gov_funded_share_latest >= 0.80 AND gross_income_latest_eur >= 100_000_000
    user_value: Distinguishes HSE-style government bodies from civil-society NGOs
```

### 6.2 Internal ranking & flags

```yaml
features:
  - name: newly_incorporated_lobby_vehicle_flag
    type: boolean
    description: Company first filed a lobbying return within 24 months of incorporation
    formula: months_between(first_return_date, company_reg_date) <= 24
    output_type: boolean
    interpretation: Possible single-purpose lobbying vehicle; warrants editor inspection
    explainability: "Incorporated {reg_date}, first lobbying return {first_return}; {months} months apart."

  - name: deregistered_but_still_filing_flag
    type: boolean
    formula: charity.status LIKE 'Deregistered%' AND lobbying.last_return_date > charity.deregistration_date
    interpretation: Compliance / data-quality flag
    explainability: "Charity status: {status}; last lobbying return: {last_return}."

  - name: dissolved_company_active_lobby_flag
    type: boolean
    formula: cro.company_status IN ('Dissolved','Liquidation','Strike Off Listed') AND lobbying.last_return_date >= cro.company_status_date
    interpretation: Dissolved company appears on a recent lobbying return — flag for editor

  - name: trust_score
    type: ordinal_0_to_100
    formula: weighted_sum(
              status_active=40,
              has_recent_annual_return=20,
              gross_income_known=15,
              classification_known=10,
              registration_age_capped=15
             )
    interpretation: Display as colour-coded chip; not a substantive rating
    caveats: Proxy only — does not reflect substantive trustworthiness, only data-completeness/recency

  - name: funding_concentration_signal
    type: categorical
    values: [state_funded, mostly_donations, mostly_trading, mixed, undisclosed]
    formula: argmax over income share, with thresholds
    interpretation: Single-pill summary of charity funding profile

  - name: trustee_count
    type: integer
    formula: count(parsed trustees per RCN)
    user_value: Governance density signal
```

### 6.3 Segments / cohorts

```yaml
segments:
  - civil_society_active:        Status=Registered AND last_filing_year >= current_year - 2
  - state_adjacent_body:         state_adjacent_flag = true
  - newly_incorporated_lobbyist: newly_incorporated_lobby_vehicle_flag = true
  - dissolved_or_strikeoff:      company_status IN dissolved_set
  - deregistered_charity:        status LIKE 'Deregistered%'
  - schools_and_academies:       governing_form LIKE 'Board of Management%'
  - foreign_established:         country_established != 'Ireland'
  - high_government_dependence:  gov_funded_share_latest >= 0.50
  - lobby_active_charity:        rcn matched on lobbyist_entity_resolution
```

### 6.4 Search / filter / discovery

```yaml
search_fields:
  - lobbyist_name (existing)
  - company_name
  - charity_name
  - also_known_as
  - trustee_name (for power-user searches)

filter_facets:
  - entity_kind: [commercial, civil_society, state_adjacent, school, unknown]
  - status:      [active, dissolved, deregistered, in_liquidation]
  - sector:      <NACE section labels for commercial> ∪ <Charity primary classification>
  - county:      derived from address
  - funding_profile: [state_funded, mostly_donations, mostly_trading, mixed, undisclosed]
  - age_band:    [<2y, 2-5y, 5-15y, 15+y]
  - employees_band:  [NONE, 1-9, 10-19, 20-49, 50-249, 250+]

sort_options:
  - return_count desc          (default for lobbying org index)
  - gross_income desc
  - gov_funded_share desc
  - reg_date asc/desc
  - last_return_date desc

badges:
  - "State-funded 87%"           (gov_share)
  - "Newly formed"               (newly_incorporated_lobby_vehicle_flag)
  - "Dissolved"                  (status pill)
  - "Deregistered (Revenue)"     (charity status)
  - "Trustee of N charities"     (on member profile)
  - "Schools — board of management" (governing_form)
  - "State-adjacent body"        (HSE etc.)

related_items:
  - on_lobbyist_org: peer organisations sharing dominant policy area + funding profile
  - on_member: charity trusteeships + interest declarations
  - on_charity: lobbying returns filed by this charity (when Tier B match)
```

### 6.5 Aggregates / rollups

```yaml
rollups:
  - lobbying_returns_by_funding_profile_x_policy_area:
      grain: (funding_profile, public_policy_area, year)
      metrics: [return_count, distinct_orgs]
      use: cross-tab on the lobbying landing page — "how is each policy area being lobbied, and by whom"

  - state_adjacent_lobbying_share:
      grain: (year)
      formula: returns_from_state_adjacent / total_returns
      use: trend line on Transparency callout

  - charity_filings_by_classification_x_year:
      grain: (classification_primary, year)
      metrics: [filings, total_gross_income, mean_gov_share]
      use: "civil society health check" page (optional later phase)

  - cro_incorporations_by_year:
      grain: (year)
      metric: count
      use: optional macroeconomic context only (not core)
```

### 6.6 Explainability templates

```yaml
explanations:
  - feature_name: state_adjacent_flag
    user_facing_label: "State-adjacent body"
    explanation_template: >
      Classified state-adjacent because government and public-body income
      ({gov_share}%) is at least 80% of gross income, and gross income ({gross_income_eur})
      exceeds €100m. Latest filing period: {period_end}.
    evidence_fields: [gov_funded_share_latest, gross_income_latest_eur, period_end]
    uncertainty_fields: [filing_year_lag]

  - feature_name: newly_incorporated_lobby_vehicle_flag
    explanation_template: >
      Company incorporated {reg_date}; first lobbying return {first_return}.
      That is {months_gap} months — within the 24-month window we flag for review.
    evidence_fields: [reg_date, first_return_date]
    uncertainty_fields: [name_match_confidence]

  - feature_name: trustee_to_member_match
    explanation_template: >
      A trustee in the charity register named {trustee_name_in_register}
      (start date {start_date}, role {role}) matches this member's name.
      Match confidence: {match_confidence}.
    evidence_fields: [trustee_name_in_register, role, start_date]
    uncertainty_fields: [match_confidence, name_collision_risk]
```

---

## 7. Enrichment Plan

### 7.1 Internal-only (using only the two CSVs + existing project data)

| Enrichment | Source | Output column | Effort |
|---|---|---|---|
| Normalised company / charity names | regex pipeline | `name_norm` | low |
| Title-cased display names | string transform | `display_name` | low |
| Parsed classification triple | regex on charity classification | `classification_primary/secondary/sub` | low |
| Parsed trustee long table | regex split on `;` then per-token regex | `trustees_long.parquet` | medium |
| Funding profile category | thresholds on income shares | `funding_profile` | low |
| Entity age | date diff | `entity_age_years` | low |
| Latest filing snapshot | window pick by RCN | `*_latest` columns on charity_resolved | medium |
| Newly-incorporated lobby vehicle flag | join + date comparison | `newly_incorporated_flag` | medium |
| Deregistered-but-still-filing flag | join + date comparison | `deregistered_active_flag` | medium |
| State-adjacent flag | derived from gov_share + income | `state_adjacent_flag` | low |
| Trust score | weighted formula | `trust_score` | low |
| Lobbyist entity resolution | name normalisation + manual override | `entity_resolution.parquet` | medium-high |
| Member ↔ trustee fuzzy link | name match + manual confirmation queue | `member_charity_trusteeships.parquet` | medium |
| Eircode → routing key | string slice | `routing_key` | low |

### 7.2 External (separated, deprioritised)

```yaml
external_enrichment:
  - name: nace_label_table
    purpose: human-readable sector pill
    required_external_source: Eurostat NACE Rev.2 reference (one-time CSV)
    expected_user_value: high (today the sector is a 4-digit code)
    privacy_or_compliance_risk: none
    implementation_complexity: trivial
    priority: P1

  - name: eircode_to_constituency
    purpose: geographic facet
    required_external_source: Ordnance Survey / Eircode Routing Key dataset
    expected_user_value: medium — limited by 34% eircode coverage
    privacy_or_compliance_risk: none
    implementation_complexity: low
    priority: P3

  - name: rbo_beneficial_owners
    purpose: who actually owns / controls the lobbying companies
    required_external_source: Register of Beneficial Ownership (RBO)
    expected_user_value: very high
    privacy_or_compliance_risk: medium — restricted access regime; not freely re-publishable
    implementation_complexity: high
    priority: P4 (out of scope for this plan)

  - name: cro_b1_b2_filings
    purpose: fine-grained company event history (director changes, charges)
    required_external_source: CORE (CRO online filings)
    expected_user_value: medium
    privacy_or_compliance_risk: low — public
    implementation_complexity: medium
    priority: P3
```

---

## 8. Application Integration Design

### 8.1 Backend / data model

```yaml
canonical_silver_tables:
  - name: silver.cro.companies
    primary_key: company_num
    fields: <all 21 cleaned + nace_label + entity_age_years + status_pill_value>
    indexes: [company_num, name_norm]
    refresh: monthly

  - name: silver.charities.register
    primary_key: rcn
    fields: <all 13 cleaned + classification_primary/secondary/sub + has_cro_number_flag>
    indexes: [rcn, cro_number_clean, name_norm]
    refresh: weekly

  - name: silver.charities.annual_reports
    primary_key: (rcn, period_end_date)
    fields: <financials + employees_band + parsed period_year>
    indexes: [rcn, period_year]
    refresh: weekly

  - name: silver.charities.trustees_long
    primary_key: surrogate (rcn, trustee_name_norm, start_date)
    fields: [rcn, raw_trustee_token, trustee_name, trustee_name_norm, role, start_date, parse_quality]
    indexes: [rcn, trustee_name_norm]

  - name: silver.charities.charity_latest
    primary_key: rcn
    description: One row per charity with latest-period financials + derived metrics
    fields: [rcn, gov_share_latest, gross_income_latest, employees_band_latest, funding_profile,
             state_adjacent_flag, period_end_latest, period_year_latest]
    refresh: weekly

  - name: silver.lobbying.entity_resolution
    primary_key: lobbyist_name (canonical)
    fields: [lobbyist_name, company_num, rcn, match_method, match_quality, match_evidence,
             manual_override_flag]
    refresh: on every lobbying refresh, re-running silver matchers

mapping_files (committed to git):
  - data/_meta/lobbyist_name_overrides.csv:   manual lobbyist→company_num/rcn pins
  - data/_meta/member_trustee_confirmations.csv: editor-confirmed member↔trustee links

audit_fields_on_every_silver_table:
  - run_id
  - source_snapshot_date
  - row_hash
  - last_modified_utc
```

### 8.2 SQL views (gold) — extend or create

```yaml
modify_existing:
  - sql_views/lobbying_org_index.sql:
      add_columns: [sector_label, gov_funded_share_latest, gross_income_latest_eur,
                    employees_band_latest, status, deregistered_flag, state_adjacent_flag,
                    funding_profile, entity_age_years, newly_incorporated_flag,
                    rcn, company_num]
      remove: TODO_PIPELINE_REQUIRED comment at top of file
  - sql_views/lobbying_clients.sql:        same set of enrichment columns
  - sql_views/member_interests_views.sql:
      populate: directorship_flag, shareholding_flag using interest_category mapping
      add: linked_company_status (for resolved interests)

new_views:
  - sql_views/charity_register.sql:
      grain: one row per charity
      columns: [rcn, charity_name, status, governing_form, classification_primary,
                classification_secondary, classification_sub, cro_number, county,
                gov_funded_share_latest, gross_income_latest_eur, funding_profile,
                trustee_count, state_adjacent_flag, profile_url]
  - sql_views/charity_finance_timeseries.sql:
      grain: (rcn, period_year)
      columns: [rcn, period_year, gross_income, gov_share, surplus_deficit, employees_band]
      use: sparklines on charity profile page
  - sql_views/member_charity_trusteeships.sql:
      grain: (member_id, rcn)
      columns: [member_id, member_name, rcn, charity_name, role, start_date,
                charity_funding_profile, charity_gov_share, match_confidence]
      use: card section on member overview
  - sql_views/lobbying_funding_concentration.sql:
      grain: (public_policy_area, funding_profile, year)
      columns: [public_policy_area, funding_profile, year, return_count, distinct_orgs]
      use: cross-tab section on lobbying landing
  - sql_views/cro_companies.sql:           thin lookup view — only used to support drill-down detail
```

### 8.3 Frontend / app surfaces

```yaml
primary_user_entities_after_integration:
  - lobbyist_organisation:    upgraded from string to resolved entity
  - charity:                  new first-class entity
  - trustee_link_on_member:   new disclosure surface

list_pages:
  - lobbying_2 → org index:
      cards display: [rank, name, sector_pill, status_pill, funding_pill, return_count]
      filters:        [date_range, sector, funding_profile, status]
  - charity_index (new, optional Phase 3):
      cards: [name, classification, funding_pill, status_pill, latest_income, lobby_active_flag]
      filters: [classification, county, funding_profile, status]

detail_pages:
  - lobbying org Stage 2 profile (existing):
      add: identity strip — incorporated YYYY · sector · county · employees band
      add: funding profile mini-bar (income source breakdown for latest year)
      add: official-source-link to CRO and Charity Regulator listings
  - member overview (existing):
      add: card section "Charity trusteeships" — only when ≥1 confirmed link
      add: badge "Trustee of N charities" on identity strip
  - interests (existing):
      add: per-declaration small status indicator when interest text resolves to a CRO or charity row

dashboards:
  - charity_finance_sparkline on charity profile (5–10 year income, surplus, employees)
  - lobbying_intensity_x_funding cross-tab on lobbying landing (Phase 2)

empty_states:
  - "No charity register match — entity is unincorporated or below registration threshold"
  - "No CRO match — likely operates as an unincorporated association"
  - "No financial filings — charity has registered but has not yet filed an annual report"

uncertainty_indicators:
  - dashed border on cards where match_confidence < 0.78
  - hover/tap → tooltip with match evidence string
  - explicit "low confidence" pill where match_confidence < 0.65
```

### 8.4 UX recommendations (decisive)

- **Default landing on lobbying** stays the same. Add the funding pill to the existing leaderboard cards. **Do not introduce a new top-level page in v1.**
- **Status pill** on every lobbyist card — single most trustworthy quick signal.
- **Funding pill** colour-band: ≥80% red ("State-funded"), 50–80% amber ("Mostly state"), 20–50% neutral ("Mixed"), <20% green ("Independent"), unknown grey ("Funding not disclosed"). The colour is a *signal*, not a *judgement*; copy must match.
- **Trusteeship disclosure on member overview** is the highest civic-value addition. Place it directly under interests, not in a separate tab.
- **Hide from primary view, expose on detail**: NACE 4-digit code, principal_object_code, raw addresses, full trustee list.
- **Search** — promote charity_name and company_name into the lobbying search; do **not** add a separate company-search affordance until post-v1.
- **Always show evidence** — every derived flag must be hover-explainable with the formula and the underlying values, not a mystery score.

---

## 9. YAML Contract Updates

### 9.1 New page contract: `charity.yaml` (Phase 3 — defer until UI demands it)

```yaml
contract_version: 5

page:
  id: charity
  title: "Charities"
  route: "utility/pages_code/charity.py"
  user_question: "Which Irish charities are most active, where does their money come from, and which lobby government?"
  existing_page_is: greenfield_no_existing_page

data_access:
  mode: duckdb_in_process_registered_analytical_views
  persistent_duckdb_file: null
  streamlit_may_register_views: false
  streamlit_may_read_parquet: false

approved_registered_views:
  - name: v_charity_register
    status: TODO_PIPELINE_VIEW_REQUIRED
    purpose: Ranked charity index
    required_columns: [rcn, charity_name, status, classification_primary, county,
                        gov_funded_share_latest, gross_income_latest_eur, funding_profile,
                        trustee_count, state_adjacent_flag, lobby_active_flag, profile_url]
    approved_filters:
      - {column: classification_primary, operators: [=]}
      - {column: county,                  operators: [=]}
      - {column: status,                  operators: [=]}
      - {column: funding_profile,         operators: [=]}
    approved_order: "ORDER BY gross_income_latest_eur DESC NULLS LAST"

  - name: v_charity_finance_timeseries
    status: TODO_PIPELINE_VIEW_REQUIRED
    required_columns: [rcn, period_year, gross_income, gov_share, surplus_deficit,
                        employees_band]
    approved_filters: [{column: rcn, operators: [=]}]
    approved_order: "ORDER BY period_year ASC"

retrieval_sql_policy: <inherits the project standard>

ui_creativity_budget:
  level: very_high
  must_be_materially_different_from_existing_page: true

interaction_model:
  required_flow:
    - editorial_hero
    - faceted_filter_strip
    - ranked_charity_cards
    - state_adjacent_callout
    - newly_active_callout
    - profile_drilldown_with_finance_sparkline_and_trustee_list
    - csv_export
    - provenance

acceptance_tests:
  - cards_use_funding_pill_colour_band
  - state_adjacent_callout_top_of_page
  - profile_uses_finance_sparkline_helper
  - trustee_links_to_member_when_match_confidence_high
  - empty_state_for_unmatched_lobby_orgs
```

### 9.2 Lobbying contract amendments (drop in to existing `lobbying.yaml`)

```yaml
# Change to v_lobbying_org_index — promote from TODO to required and expand columns
- name: v_lobbying_org_index
  status: required          # was TODO_PIPELINE_VIEW_REQUIRED
  required_columns:
    - lobbyist_name
    - rcn                                  # NEW (nullable)
    - company_num                          # NEW (nullable)
    - sector_label                         # NEW (renamed from sector)
    - status                               # NEW (Normal | Dissolved | Deregistered | Unknown)
    - funding_profile                      # NEW
    - gov_funded_share_latest              # NEW
    - gross_income_latest_eur              # NEW
    - employees_band_latest                # NEW
    - entity_age_years                     # NEW
    - newly_incorporated_flag              # NEW
    - state_adjacent_flag                  # NEW
    - return_count                         # existing
    - politicians_targeted                 # existing
    - distinct_policy_areas                # existing
    - profile_url                          # existing
    - first_period
    - last_period
  approved_filters:
    - {column: period_start_date, operators: [BETWEEN]}
    - {column: status,            operators: [=]}        # NEW
    - {column: sector_label,      operators: [=]}        # NEW
    - {column: funding_profile,   operators: [=]}        # NEW
  approved_order: "ORDER BY return_count DESC"

# v_lobbying_clients — same enrichment

# v_lobbying_recent_returns — no change

# NEW
- name: v_lobbying_funding_concentration
  status: TODO_PIPELINE_VIEW_REQUIRED
  required_columns: [public_policy_area, funding_profile, year, return_count, distinct_orgs]
  approved_filters:
    - {column: year, operators: [=, BETWEEN]}
  approved_order: "ORDER BY year DESC, return_count DESC"
```

### 9.3 Member overview contract amendments

```yaml
approved_registered_views:
  stage_2_profile:
    - v_member_charity_trusteeships     # NEW
      # required_columns: [member_id, rcn, charity_name, role, start_date,
      #                    charity_funding_profile, charity_gov_share, match_confidence]
      # filter: [{column: member_id, operators: [=]}]

# UI section to add:
# - charity_trusteeships_card_section:
#     visibility: only when row count > 0
#     order: directly below interests section, above legislation
#     low_confidence_treatment: dashed border + "review needed" pill
```

### 9.4 Interests contract amendments

```yaml
v_member_interests:
  add_columns:
    - directorship_flag        # populated, was always FALSE
    - shareholding_flag        # populated, was always FALSE
    - linked_company_num       # nullable — when interest text resolves to a CRO row
    - linked_company_status    # nullable
```

### 9.5 New silver-dataset contracts (dataset-level, separate from page contracts)

```yaml
datasets:
  - name: silver_cro_companies
    source_file: data/bronze/cro/companies_YYYYMMDD.csv
    grain: one row per company
    primary_key: company_num
    natural_keys: [company_name + company_reg_date]
    required_columns: [company_num, company_name, company_status, company_type,
                        company_reg_date, name_norm]
    optional_columns: [nace_v2_code, nace_label, eircode, routing_key, address_*,
                        comp_dissolved_date, last_ar_date]
    derived_columns: [name_norm, entity_age_years, status_pill_value]
    quality_rules:
      - company_num must be unique-per-snapshot
      - company_reg_date must be >= 1900-01-01
      - company_address_1 must not contain 'NO ADDRESS'
    freshness_rules:
      max_age_days: 60   # CRO publishes monthly bulk
    privacy_classification: public

  - name: silver_charities_register
    source_file: data/bronze/charities/public_register_YYYYMMDD.xlsx (sheet=Public Register)
    grain: one row per charity
    primary_key: rcn
    required_columns: [rcn, registered_charity_name, status, governing_form, name_norm]
    optional_columns: [also_known_as, classification_primary/secondary/sub, cro_number,
                        primary_address, country_established, charitable_purpose]
    derived_columns: [name_norm, has_cro_number_flag, county_parsed, state_adjacent_flag_candidate]
    quality_rules:
      - rcn must be unique
      - cro_number = 0 → null
      - cro_number = rcn → null (self-reference)
    freshness_rules:
      max_age_days: 14   # weekly publication
    privacy_classification: public

  - name: silver_charities_annual_reports
    grain: one row per (rcn, period_end_date)
    primary_key: (rcn, period_end_date)
    quality_rules:
      - both period dates required
      - employees_band must be in approved_set
      - gov_share computed only when gross_income > 0

  - name: silver_charities_trustees_long
    grain: one row per (rcn, trustee_name_norm, start_date)
    derived: true
    quality_rules:
      - parse_quality in [strict, partial, raw]
      - raw rows preserved for audit but not joined into trustee→member matcher
```

### 9.6 New cross-cutting contract sections

```yaml
joins:
  - name: charity_to_company
    left: silver_charities_register
    right: silver_cro_companies
    join_type: many_to_one
    keys: [{left: cro_number, right: company_num}]
    match_method: exact_id
    confidence: 0.99
    validation_rules:
      - assert.silver_charities_register.cro_number is not null → match_rate >= 0.97

  - name: lobbyist_to_charity
    left: silver_lobbying_orgs
    right: silver_charities_register
    join_type: many_to_one
    keys: [{left: name_norm, right: name_norm}]
    match_method: normalised_name_then_fuzzy
    confidence_bands: <see Phase 4>
    fallback: lobbyist_name_overrides.csv

  - name: lobbyist_to_company   (analogous)

  - name: member_to_trustee
    left: silver_members
    right: silver_charities_trustees_long
    match_method: project's normalise_df_td_name + manual confirmation
    confidence: variable; UI must surface match_confidence column

quality_rules:
  - name: dissolved_recent_filer_flag
    severity: warning
    rule: company_status in ('Dissolved','Liquidation') AND last_lobbying_return_date >= company_status_date
    action: surface in editor review queue

  - name: deregistered_recent_filer_flag
    severity: warning
    rule: charity.status LIKE 'Deregistered%' AND last_lobbying_return_date > deregistration_date
    action: surface in editor review queue

ux:
  navigation:                     unchanged at v1; one new section on member overview
  default_views:                  unchanged
  search:                         lobbying search additionally indexes charity_name + company_name
  filters:                        sector + funding_profile + status added to lobbying explorer
  record_detail:                  org cards gain three pills + finance mini-bar
  empty_states:                   "Match not found" copy as defined above
  uncertainty_display:            dashed-border + tooltip + low-confidence pill
  progressive_disclosure:         raw NACE / object codes / full trustee list behind expander
```

---

## 10. Transformation & Implementation Plan

### 10.1 Pipeline order

```
1. LOAD raw artefacts                          (bronze, untouched)
   ├─ data/bronze/cro/companies_<DATE>.csv
   └─ data/bronze/charities/public_register_<DATE>.xlsx

2. VALIDATE schema                              (sandbox script start)
   ├─ Confirm CRO header matches contract
   └─ Confirm xlsx sheet names + first-row header

3. NORMALISE per source                         (pipeline_sandbox/*_normalise.py)
   ├─ trim, replace placeholders, NULLIF, type coercions
   ├─ build name_norm using project's existing normalisation lineage
   ├─ parse charity classification triple
   └─ parse trustees → long table

4. GENERATE canonical IDs                       (carry source PKs as-is)
   ├─ company_num, rcn, surrogate trustee key
   └─ no synthetic IDs needed

5. DEDUPE
   ├─ CRO: detect duplicate company_num (1,704 cases) and pick most-recent name effective row
   └─ Charity: RCN should already be unique; assert

6. JOIN datasets
   ├─ Tier A: charity_resolved.parquet     (charity ⨝ cro on cro_number)
   ├─ Tier B: lobbyist_to_charity.parquet  (lobbying ⨝ charity on name_norm)
   └─ Tier C: lobbyist_to_company.parquet  (lobbying ⨝ cro on name_norm)
       — fold in lobbyist_name_overrides.csv at the start

7. GENERATE enriched entities
   ├─ silver/charities/charity_latest.parquet  (per-RCN aggregates over annual_reports)
   ├─ silver/lobbying/org_profile.parquet      (per-lobbyist enrichment)
   └─ silver/members/charity_trusteeships.parquet (member ⨝ trustee_long, with confidence)

8. GENERATE metrics & flags                    (window/aggregate steps)
   ├─ gov_funded_share_latest, funding_profile, state_adjacent_flag
   ├─ entity_age_years, newly_incorporated_flag, deregistered_active_flag
   └─ trust_score

9. VALIDATE outputs
   ├─ Per-table quality rules (see contracts)
   ├─ Tier A match rate ≥ 0.97
   └─ Tier B+C top-200 lobbyists must be 100% resolved (by override or auto)

10. EXPOSE via SQL views
    ├─ Modify v_lobbying_org_index, v_lobbying_clients, v_member_interests
    └─ Create v_charity_register, v_charity_finance_timeseries,
              v_member_charity_trusteeships, v_lobbying_funding_concentration
```

### 10.2 Reference Polars sketch (sandbox)

```python
# pipeline_sandbox/charity_normalise.py  — illustrative
import polars as pl, re
from pathlib import Path

BRONZE = Path('data/bronze/charities/public_register_20260426.xlsx')
SILVER = Path('data/silver/charities')
SILVER.mkdir(parents=True, exist_ok=True)

def name_norm_expr(col: str) -> pl.Expr:
    return (
        pl.col(col).str.to_uppercase()
        .str.replace_all(r"[\.,&'\"]", " ")
        .str.replace_all(r"\b(THE|LIMITED|LTD|DAC|PLC|CLG|UC|COMPANY|"
                         r"DESIGNATED ACTIVITY COMPANY|"
                         r"COMPANY LIMITED BY GUARANTEE|"
                         r"UNLIMITED COMPANY|GROUP|HOLDINGS|IRELAND|IRL|OF)\b", " ")
        .str.replace_all(r"[^A-Z0-9 ]", " ")
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
    )

def classification_split() -> list[pl.Expr]:
    pat = r"^(?P<primary>[^\[]+?)\s*(?:\[(?P<secondary>[^\(\]]+?)\s*(?:\((?P<sub>[^\)]+)\))?\s*\])?$"
    return [
        pl.col("classification_raw").str.extract(pat, 1).str.strip_chars().alias("classification_primary"),
        pl.col("classification_raw").str.extract(pat, 2).str.strip_chars().alias("classification_secondary"),
        pl.col("classification_raw").str.extract(pat, 3).str.strip_chars().alias("classification_sub"),
    ]

def parse_trustees(raw: str | None, rcn: int) -> list[dict]:
    if not raw: return []
    out = []
    for token in raw.split(';'):
        t = token.strip()
        if not t: continue
        m = re.match(r"^(?P<name>.+?)(?:\s+\((?P<role>[^)]+?)\))?\s+\((?P<dt>\d{1,2}/\d{1,2}/\d{4})\)\s*$", t)
        if m:
            out.append({
                'rcn': rcn,
                'trustee_name': re.sub(r"\s+", " ", m['name'].strip()),
                'role': m['role'],
                'start_date': m['dt'],
                'parse_quality': 'strict',
                'raw_token': t,
            })
        else:
            out.append({'rcn': rcn, 'trustee_name': None, 'role': None,
                        'start_date': None, 'parse_quality': 'raw', 'raw_token': t})
    return out

# ─── Public Register ─────────────────────────────────────────────
register = (
    pl.read_excel(BRONZE, sheet_name='Public Register', read_options={'header_row': 1})
      .rename({'Registered Charity Number': 'rcn',
               'Registered Charity Name':   'registered_charity_name',
               'Also Known As':              'also_known_as',
               'Status':                      'status',
               'Charity Classification: Primary [Secondary (Sub)]': 'classification_raw',
               'Primary Address':             'primary_address',
               'Also Operates In':            'also_operates_in',
               'Governing Form':              'governing_form',
               'CRO Number':                  'cro_number_raw',
               'Country Established':         'country_established',
               'Charitable Purpose':          'charitable_purpose',
               'Charitable Objects':          'charitable_objects',
               'Trustees (Start Date)':       'trustees_raw'})
      .with_columns(
          name_norm = name_norm_expr('registered_charity_name'),
          aka_norm  = name_norm_expr('also_known_as'),
          cro_number = pl.when(pl.col('cro_number_raw').cast(pl.Int64, strict=False) == 0)
                         .then(None)
                         .otherwise(pl.col('cro_number_raw').cast(pl.Int64, strict=False)),
          *classification_split(),
          county = pl.col('primary_address').str.split(',').list.get(-2).str.strip_chars(),
      )
      .with_columns(
          has_cro_number_flag = pl.col('cro_number').is_not_null() & (pl.col('cro_number') != pl.col('rcn'))
      )
)
register.write_parquet(SILVER / 'register.parquet')

# ─── Annual Reports ──────────────────────────────────────────────
ar = (
    pl.read_excel(BRONZE, sheet_name='Annual Reports', read_options={'header_row': 1})
      .rename(lambda c: c.replace('\n',' ').strip())
      .with_columns(
          period_year = pl.col('Period End Date').str.to_date(strict=False).dt.year(),
          gov_share = (
              (pl.col('Income: Government or Local Authorities').cast(pl.Float64, strict=False).fill_null(0)
             + pl.col('Income: Other Public Bodies').cast(pl.Float64, strict=False).fill_null(0))
              / pl.col('Gross Income').cast(pl.Float64, strict=False)
          ),
          employees_band = pl.col('Number of Employees').cast(pl.Utf8).fill_null('UNKNOWN'),
      )
)
ar.write_parquet(SILVER / 'annual_reports.parquet')

# Latest snapshot per RCN
latest = (
    ar.sort(['Registered Charity Number (RCN)', 'period_year'], descending=[False, True])
      .group_by('Registered Charity Number (RCN)')
      .first()
      .rename({'Registered Charity Number (RCN)': 'rcn'})
      .with_columns(
          funding_profile = pl.when(pl.col('gov_share') >= 0.5).then(pl.lit('state_funded'))
                              .when(pl.col('Income: Donations').cast(pl.Float64, strict=False) /
                                    pl.col('Gross Income').cast(pl.Float64, strict=False) >= 0.5)
                                  .then(pl.lit('mostly_donations'))
                              .when(pl.col('Income: Trading and Commercial Activities').cast(pl.Float64, strict=False) /
                                    pl.col('Gross Income').cast(pl.Float64, strict=False) >= 0.5)
                                  .then(pl.lit('mostly_trading'))
                              .otherwise(pl.lit('mixed')),
      )
)
latest.write_parquet(SILVER / 'charity_latest.parquet')

# ─── Trustees long ───────────────────────────────────────────────
trustees = []
for row in register.select(['rcn','trustees_raw']).iter_rows(named=True):
    trustees.extend(parse_trustees(row['trustees_raw'], row['rcn']))
pl.DataFrame(trustees).write_parquet(SILVER / 'trustees_long.parquet')
```

### 10.3 Sample output schema (silver.charity_latest)

```yaml
- rcn: 20015263
  charity_name: "Macra Na Feirme CLG"
  status: "Registered"
  governing_form: "CLG - Company Limited by Guarantee"
  cro_number: 19920
  classification_primary: "Advancement of education"
  county: "Carlow"
  period_year_latest: 2023
  gross_income_latest_eur: 5_142_000
  gov_funded_share_latest: 0.62
  funding_profile: "state_funded"
  employees_band_latest: "20-49"
  state_adjacent_flag: false
  trustee_count: 7
  has_cro_number_flag: true
  link_method: "cro_number_exact"
```

### 10.4 Test cases (assertion-style)

```yaml
tests:
  unit:
    - name_norm("Macra na Feirme") == name_norm("MACRA NA FEIRME CLG")
    - parse_trustees("A B (Chair) (01/01/2020)") yields one strict row
    - parse_trustees("badly formed token") yields one raw row
    - classification_split('A [B (C; D)]') -> ('A','B','C; D')
  integration:
    - register.parquet.row_count == 14_448
    - annual_reports.parquet.row_count == 82_894
    - charity_latest.row_count <= register.row_count
    - charity_resolved on cro_number: match_rate >= 0.97 of non-null cro_number rows
  ux_data_contracts:
    - v_lobbying_org_index returns funding_profile in approved_set
    - v_member_charity_trusteeships only returns rows where match_confidence >= 0.78
    - no card surface ever shows raw NACE 4-digit code without label fallback
```

---

## 11. Adaptive Handling — snags identified

```yaml
- snag:
    type: source_format
    severity: low
    description: Charity register is xlsx not csv as the brief stated
    why_it_matters: requires openpyxl/calamine in the sandbox loader
    affected_outputs: charities_normalise.py only
    recommended_adaptation: keep xlsx in bronze (untouched, snapshot-dated), convert to parquet in silver
    yaml_contract_change: bronze artefact path documents .xlsx
    ux_change: none
    open_questions: none
    continue_strategy: continue with the recommended Polars/Pandas xlsx loader

- snag:
    type: bucketed_employee_data
    severity: medium
    description: Number of Employees / Volunteers are bucketed text, not numeric
    why_it_matters: any per-employee ratio metric is impossible
    affected_outputs: features that assumed numeric headcount
    recommended_adaptation: treat as ordinal band; never expose arithmetic; use as filter facet only
    yaml_contract_change: explicit data type "ordinal_band" with approved_set listed
    ux_change: display as pill, not number
    continue_strategy: continue — drop "income per employee" type metrics from the catalogue

- snag:
    type: lobbyist_long_tail_unmatched
    severity: medium
    description: Even with both registers, ~50% of distinct lobbyist names won't auto-match
    why_it_matters: visible as "missing pills" on cards in the long tail
    affected_outputs: lobbying_org_index UX
    recommended_adaptation: introduce a manual override CSV; commit to maintaining top-50 by hand; show "—" for unmatched and never block the card
    yaml_contract_change: lobbyist_name_overrides.csv documented as a controlled mapping artefact
    ux_change: gracefully render absent enrichments — no scary "ERROR" or "UNKNOWN" placeholders
    open_questions: who owns the override CSV — the project maintainer
    continue_strategy: continue with a degrade-gracefully UI

- snag:
    type: state_adjacent_classification
    severity: high (for interpretation)
    description: HSE / hospitals / Pobal / Tusla appear as charities and dwarf real NGOs by income
    why_it_matters: a naive "top charity by income" leaderboard is misleading — HSE dominates
    recommended_adaptation: flag state_adjacent_flag and either segment them or exclude from civil-society top-N by default
    yaml_contract_change: state_adjacent_flag is a required column on v_charity_register
    ux_change: by default, lists exclude state_adjacent rows; toggle "include state-adjacent" available
    continue_strategy: continue — treat as a feature

- snag:
    type: trustee_to_td_collision
    severity: high (false-positive risk)
    description: Common names ("John Murphy") will match many entries
    why_it_matters: surfacing low-confidence matches as facts on member profiles damages trust
    recommended_adaptation: introduce a confirmation file (member_trustee_confirmations.csv); only show on profile when confirmed OR confidence ≥ very high (charity is non-trivial AND name is uncommon)
    yaml_contract_change: member_charity_trusteeships only exposes rows above display_threshold
    ux_change: low-confidence matches go to a "potential matches — review" list, not the user-facing card
    continue_strategy: continue — degrade defaults toward conservatism

- snag:
    type: address_freeform_geo
    severity: low
    description: Charity primary_address is comma-separated free text, not a structured field
    why_it_matters: county facet may be wrong on minority of rows
    recommended_adaptation: parse last-but-one token as county heuristic; surface "Unknown" when ambiguous
    yaml_contract_change: county column marked optional + parse_quality_flag
    ux_change: county facet shows "Unknown" bucket
    continue_strategy: continue
```

---

## 12. UX-First Discovery Design

```yaml
discovery_experience:
  primary_user_questions:
    - question: "Who is lobbying my TD, and how trustworthy is the entity?"
      app_surface: lobbying member profile (Stage 2)
      supporting_data: enriched v_lobbying_org_index columns
      recommended_metric_or_view: status_pill + funding_pill on each org row

    - question: "Is this lobbying organisation funded by the same government it is lobbying?"
      app_surface: lobbying org card / org Stage 2 profile
      supporting_data: gov_funded_share_latest
      recommended_metric_or_view: funding_pill with hover explanation

    - question: "Has my TD declared all the charity boards they sit on?"
      app_surface: member overview — new "Charity trusteeships" card section
      supporting_data: v_member_charity_trusteeships
      recommended_metric_or_view: card list with match-confidence indicator

    - question: "Is this lobbying client even a real, live company?"
      app_surface: lobbying org / client cards
      supporting_data: company_status, company_reg_date
      recommended_metric_or_view: status_pill (active / dissolved / etc.)

    - question: "How long has this organisation been around?"
      app_surface: org Stage 2 profile identity strip
      supporting_data: entity_age_years
      recommended_metric_or_view: years-on-register inline meta

    - question: "Are there newly-formed entities suddenly active in lobbying?"
      app_surface: lobbying landing — Transparency callout, Phase 2
      supporting_data: newly_incorporated_lobby_vehicle_flag
      recommended_metric_or_view: callout list with link to org profile

  default_landing_page: lobbying (unchanged)

  guided_exploration_paths:
    - "Pick a TD → see lobbyists targeting them → click a lobbyist → see funding profile + trustees"
    - "Browse policy area → pick health → see funding-profile cross-tab → click a state-funded charity"
    - "Open a member profile → see interests + trusteeships + lobbying interactions in one screen"

  search_strategy:
    fields: [member name, lobbyist name, charity name, company name, trustee name (advanced)]
    suggestions: top 200 entities prefixed with their entity_kind icon

  filter_strategy:
    primary_filters_visible:    [date_range, status, funding_profile, sector]
    advanced_filters_in_drawer: [classification, governing_form, county, employees_band, age_band]

  recommendation_strategy:
    related_orgs:    same dominant policy_area + same funding_profile
    related_members: same constituency

  comparison_strategy:
    enabled_for: org-level (compare two lobbyist organisations side-by-side, Phase 3)
    metrics_compared: [return_count, gov_funded_share, gross_income, status, age]

  detail_page_strategy:
    org_profile:
      identity_strip: name · entity_kind · status · age · employees_band · county
      finance_minibar: stacked bar of latest income breakdown
      activity_section: returns over time (existing)
      explainability: every pill has hover/tap tooltip with formula and inputs

  progressive_disclosure:
    primary_view:    pills + 4 KPIs + ranked list
    expanded:        finance time-series + trustee list + raw classification
    advanced:        NACE 4-digit code + principal_object_code + raw addresses

  empty_state_strategy:
    no_match: "Not in CRO or charity register — likely an unincorporated body, alliance, or individual"
    no_filings: "Charity is registered but has not yet filed an annual return"
    no_eircode: omit county facet rather than show 'Unknown'

  uncertainty_strategy:
    high_confidence:        no extra UI
    medium_confidence:      tooltip explaining the match evidence
    low_confidence:         dashed border + explicit "low confidence" pill, not surfaced on member profile
```

---

## 13. Risk Register

```yaml
risks:
  - risk: false-positive name match exposes the wrong charity / company on a card
    severity: high
    likelihood: medium
    impact: erodes trust — single biggest danger of name-based joins
    mitigation: confidence band + manual override CSV + display threshold ≥ 0.78 + always show evidence

  - risk: HSE / hospitals appear as top "charities" and obscure civil-society view
    severity: medium
    likelihood: certain
    impact: misleading by default
    mitigation: state_adjacent_flag with default exclusion + explicit toggle

  - risk: trustee→TD match collisions ("John Murphy")
    severity: high
    likelihood: medium
    impact: defamatory or misleading suggestion of conflict
    mitigation: editor-confirmation file; never surface untrusted matches on member profile

  - risk: gov_funded_share computed on a year missing income breakdown → null swallowed silently
    severity: medium
    likelihood: medium
    impact: charity appears unfunded when it is actually undisclosed
    mitigation: `funding_profile = 'undisclosed'` is a first-class category, surfaced as a grey pill

  - risk: source refresh introduces breaking schema changes
    severity: medium
    likelihood: low
    impact: pipeline failure
    mitigation: schema validation step; fail-loud rather than silently producing nulls

  - risk: deregistered charity continues to file lobbying returns and we don't notice
    severity: medium
    likelihood: medium
    impact: missed editorial story
    mitigation: deregistered_active_flag → editor review queue

  - risk: legal challenge from a flagged organisation ("incorrectly tagged as state-adjacent")
    severity: medium
    likelihood: low
    impact: reputational
    mitigation: every pill is hover-explainable with formula + raw values; no opaque scoring; right-of-reply contact in provenance footer

  - risk: refresh cadence drift — CRO becomes stale by months
    severity: low
    likelihood: medium
    impact: status pill no longer reflects reality
    mitigation: source_snapshot_date in provenance footer; freshness rule

  - risk: trustee parse_quality=raw rows silently dropped
    severity: low
    likelihood: certain
    impact: missing trustee links
    mitigation: parse_quality is a first-class column; raw rows preserved; surface count in admin/QA view
```

---

## 14. Open Questions (only material to implementation)

1. **Refresh cadence policy.** Manual snapshot drops indefinitely, or automated fetcher in v1? (Recommend manual for v1; revisit when source schema has stabilised across 3+ snapshots.)
2. **State-adjacent threshold.** Is `gov_share ≥ 80% AND gross_income ≥ €100m` the right line, or should we let the UI expose the raw share and let users decide? (Recommend the explicit flag — readers want a default verdict.)
3. **Trustee-name privacy posture.** Surface every TD-trusteeship link automatically (high-confidence only), or only after editor confirmation? (Recommend confirmation gate for v1; relax once confidence calibration is proven.)
4. **Override file ownership.** Hand-edited CSV in git, or admin Streamlit page? (Recommend CSV in git — version control + auditable diffs.)
5. **Charity page yes/no for v1.** Build a dedicated charity index page in Phase 3, or only enrich existing lobbying pages? (Recommend enrich-only for v1; defer dedicated page until traffic justifies it.)

---

## 15. Recommended Next Actions

```yaml
next_actions:
  immediate:    # this week
    - move CRO/companies.csv → data/bronze/cro/companies_20260504.csv
    - move CRO/20260426-public-register-of-charities.xlsx → data/bronze/charities/public_register_20260426.xlsx
    - write pipeline_sandbox/charity_normalise.py producing register.parquet, annual_reports.parquet, charity_latest.parquet
    - write pipeline_sandbox/cro_normalise.py producing companies.parquet
    - write pipeline_sandbox/charity_resolved.py producing charity_resolved.parquet (Tier A 99.2% join)
    - extend sql_views/lobbying_org_index.sql with funding_profile + gov_funded_share_latest + status (Tier A only)
    - update lobbying.yaml contract — promote v_lobbying_org_index from TODO to required and list new columns
    - render funding_pill on lobbying_2 org cards via a new helper in components.py
    - add provenance footer entries naming both sources, snapshot dates, and the override file path
    - update doc/DATA_LIMITATIONS.md with: bucketed employee data, state-adjacent presence, name-match ceiling

  short_term:   # weeks 2–3
    - write pipeline_sandbox/charity_trustees_parse.py
    - write pipeline_sandbox/lobbyist_entity_match.py with override-CSV layer + rapidfuzz fuzzy
    - extend lobbying contracts with status / sector / funding filters
    - add status_pill + sector_pill helpers; update org cards
    - sql_views/member_charity_trusteeships.sql — only confirmed/high-confidence rows
    - member_overview.yaml — declare v_member_charity_trusteeships
    - member_overview UI — Charity trusteeships card section under interests
    - sql_views/charity_register.sql + sql_views/charity_finance_timeseries.sql

  medium_term:  # weeks 4–6
    - new charity.py page driven by charity.yaml — only if the data has earned its own index page
    - lobbying_funding_concentration cross-tab on lobbying landing
    - newly-incorporated callout
    - editor review queue (deregistered_active_flag, dissolved_recent_filer_flag)
    - external nace_label table import + sector facet labels

  optional:     # later
    - eircode → constituency lookup
    - rbo / core integration
    - charity comparison (side-by-side) view
    - automated weekly fetcher for charity register
    - automated monthly fetcher for CRO bulk
```

---

## Appendix A — Confidence summary on every major recommendation

```yaml
confidence:
  Tier A join (charity↔company on cro_number) is reliable:                           high
  Tier B / C name-match yields 50% combined coverage with manual top-50 overrides:  high
  gov_funded_share is the highest-value single metric the dataset can produce:      high
  state_adjacent flag is necessary to make charity leaderboards interpretable:      high
  trustee→TD match must be conservatively gated:                                    high
  Number of Employees is bucketed (no per-employee ratios possible):                high
  Charity classification parser will be brittle on long-tail edge cases:            medium
  Eircode→constituency lookup will deliver enough value to justify external sourcing: low
  Newly-incorporated lobby-vehicle flag will catch real cases (not just noise):     medium
  A standalone charity index page is justified in v1:                               low — defer
```

---

## Appendix B — Distinguishing join types

```yaml
deterministic_joins:    [charity.cro_number → cro.company_num,
                         annual_report.rcn → register.rcn]
probabilistic_joins:    [lobbyist_name → register.name_norm,
                         lobbyist_name → cro.name_norm,
                         trustee_name → member.full_name]
exploratory_joins:      [eircode → constituency,
                         charity primary_address → county]
unsafe_joins:           [trustee_name → member without disambiguation context — never surface]
```

---

End of integration plan.
