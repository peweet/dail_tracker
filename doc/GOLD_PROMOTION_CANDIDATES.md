# Gold Promotion Candidates — Stranded Silver Columns

**Date:** 2026-06-05
**Method:** Schema + cardinality/null profile of every parquet under `data/silver` and
`data/gold`, cross-checked against the 90 registered views in `sql_views/` (actual
`read_parquet(...)` calls, not incidental string matches). A "candidate" is a silver
column with analytical value that **no view currently exposes**.

**Build rule:** any new Python/Polars enrichment lands in `pipeline_sandbox/` first;
new SQL views go in `sql_views/` directly. Do not touch `pipeline.py` / `enrich.py`
until a candidate graduates. (See `project_pipeline_sandbox_rule`.)

The four "deliberately gated" sources at the bottom are **not** recommended for promotion
yet — they are noted so this doc is a complete map.

---

## Tier 1 — Quick win (data exists, a view already asks for it)

### 1. Lobbyist organisation enrichment
**Source:** `data/silver/lobbying/parquet/split_lobbyists.parquet` (2,554 rows, **0 views read it**)
**Stranded columns:** `website`, `company_registration_number`, `company_registered_name`,
`main_activities_of_organisation`, `lobby_org_link`, `lobby_requests_count`

**Why it's a quick win:** [lobbying_org_index.sql](../sql_views/lobbying_org_index.sql) already
hardcodes `'' AS sector`, `'' AS website`, `'' AS profile_url` with an explicit
`TODO_PIPELINE_REQUIRED` comment. The data is sitting in silver, unused.

**Feature / story it tells:**
- **"Who is this lobbyist, really?"** — turns the org index from a bare name + count into
  a profile card: what the organisation *does* (`main_activities_of_organisation`), its
  website, and a link back to lobbying.ie. Directly upgrades the Lobbying org-index page
  (audit 15/20, our best page) from counts to identity.
- **Lobbyist → CRO crosswalk.** `company_registration_number` lets us join a lobbying firm
  to the CRO company master (`silver/cro/companies.parquet`) — unlocking *"is this lobbyist
  a healthy company, in distress, or recently renamed?"* and feeds the existing
  procurement↔lobbying overlap work with a second identity anchor.

**Gold sketch:** extend `top_lobbyist_organisations.parquet` (or a sibling
`lobbyist_org_profile.parquet`) with the five columns, then drop the hardcoded `''`
literals in the view.

---

## Tier 2 — High value, currently dropped at the gold boundary

### 2. Charity financial time-series
**Source:** `data/silver/charities/annual_reports.parquet` (82,894 rows, **0 views read it**)
**Stranded columns:** per `rcn` × `period_year`: `gross_income`, `gross_expenditure`,
`surplus_deficit`, `gov_share`, `income_donations`, `income_trading`, `employees_full_time`,
`employees_part_time`, `total_assets`, `net_assets`

**Gap:** gold only carries the **latest snapshot** (`charity_latest` →
[charities_enriched.parquet](../data/gold/parquet/charities_enriched.parquet)). 13 years of
history is discarded. Note: `charity_latest` already derives `income_trend` and
`deficit_years_count` from this file, so the inputs are proven good.

**Feature / story it tells:**
- **Charity trend sparklines** — income/expenditure trajectory per charity instead of a
  single number. *"This charity's state funding tripled over five years."*
- **State-dependency over time** — track `gov_share` per charity year-on-year: which
  charities are becoming more reliant on the exchequer, which are diversifying.
- **Sector-level aggregation** — sum by `classification_primary` × year to show *"total
  state money flowing into housing/disability/arts charities over the last decade"* — a
  genuine public-finance story no current page tells.
- **Distress early-warning** — consecutive `surplus_deficit < 0` years + shrinking reserves
  is a leading indicator; the snapshot can't show the slope.

**Gold sketch:** `charity_financials_by_year.parquet` keyed on `(rcn, period_year)`, ~80k
rows, one view `legislation_…`-style for the charity page. Cheap; it's a clean projection.

### 3. Ministerial tenure timeline
**Source:** `data/silver/ministerial_tenure.parquet` (98 rows)
**Currently:** read by [attendance_missing_members.sql](../sql_views/attendance_missing_members.sql)
**only** to classify "currently holds office" (`end_date IS NULL`). The history is invisible.
**Stranded columns:** `department_label`, `start_date`, `end_date`, `member_code`,
`wikidata_position`, `wikidata_person`

**Feature / story it tells:**
- **"Who ran this department, and when?"** — a ministerial timeline per portfolio. The
  spine for accountability: tie a policy, payment, SI, or vote to whoever held the relevant
  ministry on that date.
- **Revolving-door / career arcs** — `member_code` joins to votes, payments, questions,
  interests; `wikidata_position` joins to the judiciary/external-links graph. *"Minister X
  signed these SIs, then lobbied in this sector"* becomes expressible.
- **Tenure-length context** — average time-in-post per department; who churned, who held on.

**Gold sketch:** tiny. Promote as-is to a `ministerial_tenure` view (`v_member_ministerial_tenure`)
joining `member_code → member_registry`. No aggregation needed; the value is the join surface.

### 4. Bill amendment activity
**Source:** `data/silver/parquet/bill_amendments.parquet` (1,763 rows)
**Currently:** read by [legislation_pdfs.sql](../sql_views/legislation_pdfs.sql) but only to
list amendment **PDFs as documents**. The analytical signal is not aggregated.
**Stranded columns:** `amendment_type` (numberedList / creamList), `stage_no`,
`amendment_date`, `chamber` — counted per bill.
**Companion:** `event.stageOutcome` in `stages.parquet` (pass/fail per stage) is used by **0**
views.

**Feature / story it tells:**
- **Legislative contestation metric** — amendments-per-bill ranks how *fought-over* a bill
  was. *"This bill drew 240 amendments across Committee and Report stage"* is a sharper
  signal than its stage label.
- **Where bills get reshaped** — distribution of amendments by `stage_no` / `chamber`
  shows whether the Dáil or Seanad does the heavy editing.
- **Stage outcomes** — `stageOutcome` lets the legislation timeline show *passed/failed*
  per stage, not just "reached stage N". Turns the pipeline strip into a pass/fail story.

**Gold sketch:** `bill_amendment_counts.parquet` keyed on `(bill_id, stage_no, amendment_type)`
+ a `v_legislation_amendment_intensity` view feeding the legislation detail page.

---

## Tier 3 — Real but niche / lower priority

### 5. Attendance per-date arrays
**Source:** `td_attendance_fact_table.parquet` / `seanad_attendance_fact_table.parquet`
**Stranded columns:** `iso_sitting_days_attendance`, `iso_other_days_attendance` (the actual
ISO date lists). Gold [attendance_by_td_year.parquet](../data/gold/parquet/attendance_by_td_year.parquet)
keeps only the counts.

**Feature / story it tells:** a **calendar heatmap** of a member's attendance — *when* in the
year they showed up, clustering, long gaps. The counts can't show pattern; only the date
lists can. Niche but genuinely unique to the data.

### 6. Lobbying intent / activity breakdown
**Source:** `lobby_break_down_by_politician.parquet` (1.13M rows)
**Stranded analytical columns:** `action` (activity type), `intended_results`,
`specific_details`, `members_targeted`, grassroots flag. `delivery_method_mix` only surfaces
the `delivery` channel.

**Feature / story it tells:** *"How do lobbyists actually reach this politician?"* — a
breakdown by **activity type** (meeting / submission / event), not just channel; plus
grassroots-campaign linkage per politician. Partly mitigated by `lobbying_topic_search` and
`lobbying_contact_detail`, hence medium priority.

### 7. Charity narrative & footprint
**Source:** `register.parquet`
**Stranded columns:** `charitable_purpose`, `charitable_objects`, `also_operates_in`

**Feature / story it tells:** `also_operates_in` (overseas operations) gives a free
**"international footprint"** facet — *Irish charities operating abroad*. `charitable_purpose`
adds a human-readable mission line to the charity card. Low-medium effort.

---

## Deliberately gated — flagged, NOT recommended yet

These are built in silver but promotion is intentionally deferred (see memory notes):

| Source | Rows | Why gated |
|---|---|---|
| `ted_ie_awards.parquet` | 13,126 | Procurement is at the **ingestion phase**; value-taxonomy (`value_kind` + `realisation_tier` + `value_safe_to_sum`) must be locked before merge so money-meanings aren't conflated (`project_procurement_phase_taxonomy`). |
| `la_payments_fact.parquet` | 11,091 | Same procurement taxonomy gate; also carries `privacy_status` rows needing the quarantine path. |
| `la_afs_divisions` / `la_afs_capital_divisions` / `afs_amalgamated_divisions` | ~390 | AFS Phase 0 shipped to silver; figures are **operating-expenditure-by-division, not headline totals** — must not be mislabelled (`project_la_afs_metric_semantics`). |
| `trustees_long.parquet` | 81,545 | **PII** — private-citizen names + dates. Only promotable as a cross-reference (trustee-also-a-TD / interlocking boards), never as a raw list (`feedback_personal_insolvency_privacy` spirit). |

---

## Suggested order

1. **#1 Lobbyist org enrichment** — hardcoded TODO, data ready, upgrades our best page.
2. **#3 Ministerial timeline** — tiny table, high accountability signal, pure join surface.
3. **#2 Charity time-series** — enables trends/sparklines; inputs already proven.
4. **#4 Bill amendments** — clean contestation metric for the legislation page.
