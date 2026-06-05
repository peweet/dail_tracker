# Gold Promotion — Full Column-Level Audit (Bronze/Silver → Gold)

**Date:** 2026-06-05
**Companion to:** [GOLD_PROMOTION_CANDIDATES.md](GOLD_PROMOTION_CANDIDATES.md) (the curated, tiered shortlist).
This doc is the **exhaustive** reference: every silver column that is *not* promoted, with a verdict.

**Method.** Profiled every parquet under `data/silver` + `data/gold` (schema, rows, null
fraction, sampled cardinality). A silver column is counted **promoted** if its name appears
in a gold parquet **or** is referenced (word-match) in any `sql_views/*.sql` or
`sql_queries/*.sql`. Everything else is **stranded**. Word-matching is deliberately
generous (a shared name like `party`/`status`/`year` counts as promoted), so the stranded
list is a *floor* — these are genuinely unused, not merely renamed.

**Bronze note.** Bronze holds raw source files only — `bronze/lobbying_csv_data/*.csv`,
`bronze/cro/*.csv`, `bronze/wikidata/*_raw.csv`, `bronze/legislation/flattened_bills.csv`,
plus PDF/JSON dirs. Each maps 1:1 onto a silver table that already extracts the useful
columns; there is **no analytical column living only in bronze**. The opportunity surface is
entirely silver→gold, so the audit below is silver-keyed.

---

## A. Read this first — three big "stranded" counts that are NOT opportunities

The raw diff flags these as massively stranded; they are **artifacts/redundancy**, excluded
from all candidate tiers:

| Table | Raw stranded | Why it's noise |
|---|---|---|
| `flattened_members.parquet` | 180/226 | The wide `committee_1..12_*`, `office_1..6_*`, `party_1..2_*` columns are a denormalised flattening of member memberships. The data is **already promoted in normalised form** via `silver/committees/committee_assignments.parquet` + the committee views. Sparse (mostly null, 1–3 distinct). Ignore. |
| `flattened_seanad_members.parquet` | 107/141 | Same as above, Seanad side. |
| `debates` / `events` / `versions` / `related_docs` / `most_recent_stage_event_dates` | 14–21 each | The `bill.*` nested columns are the **same bill metadata repeated** across every bill-document table (denormalised join payload). The bill spine is promoted once via `sponsors` + `stages` + the legislation views. Only the genuinely new fields in these tables (document URIs) are listed as candidates below. |

Single-value / all-null columns (`null=1.0`, `nuniq=1`) across all tables are also ignored.

---

## B. Genuine promotion candidates (stranded + analytically useful)

Ordered by value. Each row: the stranded columns that matter + a verdict.

### B1. Charity financial time-series — `silver/charities/annual_reports.parquet` (82,894 rows)
**Stranded (26/30):** `period_end_date`, `report_activity`, `activity_description`,
`beneficiaries`, `income_govt_or_la`, `income_other_public_bodies`,
`income_philanthropic_orgs`, `income_donations`, `income_trading`, `income_other`,
`income_bequests`, `gross_income`, `gross_expenditure`, `surplus_deficit`, `cash_at_hand`,
`total_assets`, `total_liabilities`, `net_assets`, `employees_band`, `employees_full_time`,
`employees_part_time`, `volunteers_band`, `gov_share` (+ `other_assets`).
**Verdict: PROMOTE (high).** Only the *latest-year snapshot* reaches gold (`charity_latest`).
This is the entire multi-year series. **Story:** charity income/expenditure trend lines,
rising state-dependency over time, sector-level decade totals, multi-year deficit/insolvency
trajectory. Inputs already proven (`charity_latest.income_trend` derives from this file).
**Gold sketch:** `charity_financials_by_year.parquet` keyed `(rcn, period_year)`.

### B2. Ministerial tenure timeline — `silver/ministerial_tenure.parquet` (98 rows)
**Stranded (5/8):** `department_key`, `member_code`, `start_date`, `wikidata_person`,
`wikidata_position`. (`minister_name`, `department_label`, `end_date` are referenced — but
only by `attendance_missing_members.sql` to detect *current* office.)
**Verdict: PROMOTE (high).** Tiny table, huge join surface. **Story:** "who ran this
department, and when" timeline; `member_code` ties ministers to votes/payments/questions;
`wikidata_position` ties to the external-links / judiciary graph (revolving-door).
**Gold sketch:** promote as a `v_member_ministerial_tenure` view; no aggregation needed.

### B3. Bill amendment activity — `silver/parquet/bill_amendments.parquet` (1,763 rows)
**Stranded (3/13):** `stage_no`, `stage_show_as`, `bill_short_title_en`. (`amendment_type`,
`amendment_date`, `pdf_url` are referenced — but only by `legislation_pdfs.sql` to list
amendment PDFs as documents.)
**Verdict: PROMOTE (medium-high).** The PDFs are surfaced; the **count metric is not**.
**Story:** amendments-per-bill = legislative contestation; distribution by `stage_no`/chamber
shows where bills get reshaped. **Gold sketch:** `bill_amendment_counts.parquet` keyed
`(bill_id, stage_no, amendment_type)`.

### B4. Bill stage outcomes — `silver/parquet/stages.parquet` (7,370 rows)
**Stranded (key one):** `event.stageOutcome` (7 distinct, e.g. pass/fail/withdrawn),
plus `event.house.houseNo`, `event.stageURI`. The rest of the stranded set is the repeated
`bill.*`/`billSort.*` payload (noise per §A).
**Verdict: PROMOTE (medium).** **Story:** the legislation timeline can show **passed vs
failed per stage**, not just "reached stage N" — turns the pipeline strip into a pass/fail
narrative. Pairs naturally with B3.

### B5. Attendance per-date detail — `td_attendance_fact_table` / `seanad_attendance_fact_table`
**CORRECTION (2026-06-05): the sitting-date calendar already exists — do NOT rebuild.**
The audit's token-match keyed on the *parquet* file, but `v_attendance_timeline` already
promotes the per-date `iso_sitting_days_attendance` (from the CSV mirror
`aggregated_td_tables.csv`) and the member-overview attendance section renders it as an
Altair "Sitting calendar" tick strip. So the sitting dates are **not** stranded.
**Genuinely stranded:** only `iso_other_days_attendance` / `other_days_count` — the
existing timeline is sitting-only.
**Verdict: DO NOT PROMOTE (as-is).** The sole novel column ("other days") has **ambiguous
semantics** (the source PDF's second day-category is not clearly defined — see
`project_attendance_audit_2026_05_26`), so surfacing it would direct users to a conclusion
the data doesn't support (no-inference rule). Revisit only if "other days" is first defined
from the source. A code-keyed parquet-based replacement for the legacy name-keyed,
JOIN-in-view `v_attendance_timeline` is a separate *refactor*, not a promotion.

### B6. Lobbying campaign mechanics — `silver/lobbying/parquet/lobby_break_down_by_politician.parquet` (1.13M rows)
**Stranded (9/28):** `members_targeted` (reach band), `grassroots_directive` (the actual
"ask"), `was_this_lobbying_done_on_behalf_of_a_client`, `lobbying_period_end_date`,
`date_published_timestamp_dt`, `dpos_or_former_dpos_who_carried_out_lobbying`,
`client_address`, `email`, `telephone`.
**Verdict: PARTIAL PROMOTE (medium).** `members_targeted` + `grassroots_directive` are the
analytical wins — **story:** "what campaigns asked supporters to do" and reach sizing. **But**
`client_address`/`email`/`telephone` are **contact PII (97% null)** — suppress, do not promote.

### B7. Lobbying return narrative — `silver/lobbying/parquet/returns.parquet` (85,826 rows)
**Stranded (8/20):** `dpo_lobbied` (raw target string), `lobbying_activities` (full text),
`current_or_former_dpos`, `grassroots_directive`, `lobby_enterprise_uri`,
`was_this_lobbying_done_on_behalf_of_a_client`, `lobbying_period_end_date`.
**Verdict: LOW.** Mostly already exploded into the promoted detail tables
(`politician_returns_detail`, `revolving_door_returns_detail`). `lobbying_activities` free text
could feed a topic/keyword feature but is lower priority than B6.

### B8. Charity narrative & footprint — `silver/charities/register.parquet` (14,448 rows)
**Stranded (6/21):** `charitable_purpose`, `charitable_objects`, `also_operates_in`,
`primary_address`, `classification_raw`, `trustees_raw`.
**Verdict: LOW-MEDIUM.** **Story:** `also_operates_in` = "Irish charities operating abroad"
facet (212 distinct); `charitable_purpose` = human-readable mission line on the charity card.
`trustees_raw`/`primary_address` are PII-adjacent — skip.

### B9. Member interests detail — `dail_/seanad_member_interests_combined.parquet`
**Stranded (4/19):** `interest_code`, `is_occupation`, `occupation_description`,
`registration_status`.
**Verdict: LOW-MEDIUM.** `occupation_description` (366 distinct) is the interesting one —
**story:** "what TDs do for a living" beyond the landlord/property flags already promoted.
`registration_status` is single-value (noise).

### B10. CRO company depth — `silver/cro/companies.parquet` (815,945 rows)
**Stranded (12/31):** `company_address_1..4`, `last_ar_date`, `nard`, `last_accounts_date`,
`company_status_code`, `company_type_code`, `company_name_eff_date`, `company_type_eff_date`,
`princ_object_code`.
**Verdict: LOW.** The distress *flags* derived from these dates are already promoted (and used
by the lobbying enrichment). Raw addresses enable mapping but are bulky/PII-adjacent.
`princ_object_code` (NACE-like activity) is 94% null. Promote only if a company-map feature
is scoped.

---

## C. Deliberately gated — built in silver, promotion intentionally deferred

Do **not** promote without locking the relevant taxonomy first (see memory notes).

| Source | Rows | Stranded | Gate |
|---|---|---|---|
| `ted_ie_awards.parquet` | 13,126 | 19/27 (whole TED award source: `award_value_eur`, `cpv_division`, `winner_*`, `cro_company_num`, `buyer_name`…) | Procurement **value-taxonomy** must be locked (`value_kind`+`realisation_tier`+`value_safe_to_sum`) before merge — `project_procurement_phase_taxonomy`. |
| `la_payments_fact.parquet` | 11,091 | 21/31 (`amount_eur`, `publisher_*`, `supplier_normalised`, `privacy_status`, `public_display`…) | Same taxonomy gate; carries `privacy_status`/`public_display` rows needing the quarantine path. |
| `la_afs_divisions` / `la_afs_capital_divisions` / `afs_amalgamated_divisions` | ~390 | 10/19, 10/21, 5/10 (`gross_expenditure`, `income`, `net_expenditure`, `capital_*`, balances, `region`) | Figures are **operating-expenditure-by-division, not headline totals** — must not be mislabelled (`project_la_afs_metric_semantics`). |
| `silver/cro/financial_statements.parquet` | 335,318 | 5/9 (`submission_*` dates, `file_name`) | Filing-index metadata only (figures paywalled); recency already exposed via the enriched view. No further promotion value. |

---

## D. Privacy holds (never promote raw)

| Column(s) | Table | Reason |
|---|---|---|
| `trustee_name`, `raw_token`, `start_date_raw`, `trustee_name_norm` | `trustees_long.parquet` (81,545) | Private-citizen names. Only promotable as a **cross-reference** (trustee-also-a-TD / interlocking boards), never a raw list. |
| `client_address`, `email`, `telephone` | `lobby_break_down_by_politician` | Contact PII (97% null). |
| `primary_address`, `trustees_raw` | charity `register` | PII-adjacent. |

---

## E. One-line verdict table

| Candidate | Table | Value | Action |
|---|---|---|---|
| Charity financial time-series | `annual_reports` | High | Promote → `charity_financials_by_year` |
| Ministerial tenure timeline | `ministerial_tenure` | High | Promote → `v_member_ministerial_tenure` |
| Bill amendment counts | `bill_amendments` | Med-High | Promote → `bill_amendment_counts` |
| Bill stage outcomes | `stages.event.stageOutcome` | Med | Add to legislation timeline |
| Attendance per-date detail | `*_attendance_fact_table` | Med (niche) | Promote date arrays for heatmap |
| Lobbying campaign mechanics | `lobby_break_down_by_politician` | Med | Promote `members_targeted`+`grassroots_directive` only |
| Charity footprint/mission | `register` | Low-Med | Optional facet |
| Member occupation text | `*_interests_combined` | Low-Med | Optional |
| Lobbying activity free-text | `returns` | Low | Defer |
| CRO addresses/depth | `companies` | Low | Only if map feature scoped |
| TED / LA payments / LA AFS | (procurement) | Gated | Hold for taxonomy |
| Trustees / contact fields | (PII) | — | Never raw |

**Already shipped this round:** lobbyist organisation-register enrichment (`website`, CRO
number, registered name, main activities, org-page URL) — promoted from `split_lobbyists`
into gold `top_lobbyist_organisations.parquet` and both org-index views.
See [GOLD_PROMOTION_CANDIDATES.md](GOLD_PROMOTION_CANDIDATES.md) Tier 1.
