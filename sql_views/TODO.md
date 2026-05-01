# SQL Views — Refactoring TODO

Issues found by static audit. Grouped by category and priority.
Do not modify views marked ENRICH_MIGRATION_REQUIRED without the upstream enriched file existing and tested first.

---

## CAT-1 — Casts that belong in the pipeline, not in SQL

SQL should aggregate and filter. Type coercion is the pipeline's job.
Each cast here is a hidden contract between the pipeline and the view that has no test coverage.

### Datetime → Date (lobbying)
Lobbying parquets store dates as `Datetime(us)`. SQL truncates to `Date` at query time.
The display format (Date) should be emitted by the pipeline so the views receive the type they need.

| File | Line | Cast | Fix |
|---|---|---|---|
| `lobbying_clients.sql` | 15 | `lobbying_period_start_date::DATE` | Emit as `Date` from `lobby_processing.py` |
| `lobbying_contact_detail.sql` | 19 | `lobbying_period_start_date::DATE` | Same |
| `lobbying_dpo_returns.sql` | 13 | `lobbying_period_start_date::DATE` | Same |
| `lobbying_org_intensity.sql` | 15–16 | `relationship_start::DATE`, `relationship_last_seen::DATE` | Emit as `Date` from pipeline |
| `lobbying_persistence.sql` | 9–10 | `first_return_date::DATE`, `last_return_date::DATE` | Emit as `Date` from pipeline |

### Double cast (Datetime → Date → VARCHAR)
Casting the same column twice signals a data representation decision deferred to SQL.
The pipeline should decide and emit the right type once.

| File | Lines | Cast chain | Fix |
|---|---|---|---|
| `lobbying_index.sql` | 41–42, 61–62 | `Datetime → CAST(MIN(…) AS DATE)` then `CAST(… AS VARCHAR)` | Emit formatted date string from pipeline |
| `lobbying_org_index.sql` | 15–16, 27–28 | Same double-cast pattern | Same |

### String → Date (legislation / votes)
`date` and `context_date` columns are stored as strings in silver. SQL casts them at query time.
`TRY_CAST` silently returns NULL on bad data — masking parse failures.

| File | Line | Cast | Fix |
|---|---|---|---|
| `legislation_debates.sql` | 8 | `TRY_CAST(date AS DATE)` | Parse date to `Date` type in `legislation.py` |
| `legislation_detail.sql` | 15 | `TRY_CAST(context_date AS DATE)` | Same |
| `legislation_index.sql` | 13 | `TRY_CAST(context_date AS DATE)` | Same |
| `legislation_timeline.sql` | 9 | `TRY_CAST("event.dates"[1].date AS DATE)` | Flatten struct dates in pipeline; emit `Date` |
| `vote_index.sql` | 4 | `CAST(date AS DATE)` | Parse in `transform_votes.py` |
| `vote_member_detail.sql` | 9 | `CAST(date AS DATE)` | Same |
| `vote_result_summary.sql` | 5–6 | `CAST(date AS DATE)` | Same |
| `vote_td_year_summary.sql` | 5, 11 | `CAST(date AS DATE)::INTEGER` (double) | Parse date + extract year in pipeline |

### Numeric casts
| File | Line | Cast | Fix |
|---|---|---|---|
| `attendance_member_summary.sql` | 38 | `sitting_days_count::DOUBLE` for division | Emit as Float from pipeline or cast in enrichment |
| `attendance_member_year_summary.sql` | 18 | `CAST(year AS INTEGER)` | Emit as `Int64` from pipeline |
| `legislation_index.sql` | 15 | `TRY_CAST(most_recent_stage_event_progress_stage AS INTEGER)` | Already `Int64` in parquet — remove cast, it's redundant |
| `legislation_timeline.sql` | 10 | `TRY_CAST("event.progressStage" AS INTEGER)` | Already `Int64` — remove |
| `member_interests_views.sql` | 19, 37 | `TRY_CAST(year_declared AS INTEGER)` | Parse int in pipeline |
| `lobbying_dpo_returns.sql` | 8 | `primary_key::VARCHAR` | Decide in pipeline: store as VARCHAR or Int, don't convert in SQL |

### Boolean disguised as string
| File | Line | Cast | Fix |
|---|---|---|---|
| `member_interests_views.sql` | 22–23 | `CASE WHEN lower(TRY_CAST(is_landlord AS VARCHAR)) = 'true'` | Emit as `Boolean` from pipeline |
| `member_registry.sql` | 15 | `CASE WHEN LOWER(CAST(ministerial_office AS VARCHAR)) = 'true'` | Emit `is_minister: Boolean` from pipeline |

---

## CAT-2 — JOINs in SQL views that violate the no-join-in-views rule

Joins in SQL views create runtime coupling between silver tables with no schema enforcement.
They belong in the pipeline enrichment step.

| File | Join | Risk | Fix |
|---|---|---|---|
| `attendance_member_summary.sql` | LEFT JOIN `flattened_members` on `first_name + last_name` | Name collisions produce multiple rows; no unique key | Already flagged `ENRICH_MIGRATION_REQUIRED` — add party/constituency to enriched output upstream |
| `attendance_timeline.sql` | Same LEFT JOIN | Same risk | Same fix |
| `lobbying_index.sql` | LEFT JOIN `policy_areas` + `periods` on `full_name` | `full_name` is not a stable join key; duplicates possible | Pre-join in `lobby_processing.py` |
| `lobbying_org_index.sql` | LEFT JOIN `persistence` on `lobbyist_name` | Same | Pre-join in pipeline |
| `lobbying_policy_area_summary.sql` | LEFT JOIN `pol_counts` on `public_policy_area` | Low risk but still view-layer logic | Pre-aggregate in pipeline |

---

## CAT-3 — `read_csv_auto` that should be `read_parquet`

Parquet equivalents exist for all of these. `read_csv_auto` infers types at runtime — slower and less reliable.

| File | CSV path | Parquet equivalent |
|---|---|---|
| `attendance_member_summary.sql` | `data/silver/aggregated_td_tables.csv` | `data/silver/parquet/aggregated_td_tables.parquet` |
| `attendance_summary.sql` | Same | Same |
| `attendance_timeline.sql` | `data/silver/aggregated_td_tables.csv` + `flattened_members.csv` | Both have parquets |
| `lobbying_clients.sql` | `data/silver/lobbying/client_company_returns_detail.csv` | `data/silver/lobbying/parquet/client_company_returns_detail.parquet` |
| `member_interests_views.sql` | `data/silver/dail_member_interests_combined.csv` | No parquet yet — create one |

---

## CAT-4 — Template path placeholders

These views use `{PARQUET_PATH}` / `{MEMBER_PARQUET_PATH}` substituted at runtime by the app.
They cannot be statically tested or validated without the substitution layer.
Document the expected path and valid values; add an assertion in the app before executing.

| File | Placeholder | Expected runtime value |
|---|---|---|
| `member_registry.sql` | `{MEMBER_PARQUET_PATH}` | `data/silver/parquet/flattened_members.parquet` |
| `vote_index.sql` | `{PARQUET_PATH}` | `data/gold/parquet/pretty_votes.parquet` (confirm) |
| `vote_member_detail.sql` | `{PARQUET_PATH}` | Same |
| `vote_party_breakdown.sql` | `{PARQUET_PATH}` | Same |
| `vote_result_summary.sql` | `{PARQUET_PATH}` | Same |
| `vote_sources.sql` | `{PARQUET_PATH}` | Same |
| `vote_td_summary.sql` | `{PARQUET_PATH}` | Same |
| `vote_td_year_summary.sql` | `{PARQUET_PATH}` | Same |

Action: add a constant `VOTE_PARQUET_PATH` in `config.py` and resolve in a single place; do not scatter string interpolation across the app.

---

## CAT-5 — TRY_CAST masking data quality failures

`TRY_CAST` returns NULL silently when a value cannot be cast.
This is appropriate for genuinely optional fields but wrong for required ones —
it hides pipeline bugs as NULL values that pass through to the UI.

| File | Expression | Risk |
|---|---|---|
| `legislation_debates.sql` | `TRY_CAST(date AS DATE)` | A bad date string → NULL debate date, row appears undated |
| `legislation_detail.sql` | `TRY_CAST(context_date AS DATE)` | Same |
| `legislation_index.sql` | `TRY_CAST(context_date AS DATE)` | Same |
| `legislation_timeline.sql` | `TRY_CAST("event.dates"[1].date AS DATE)` | Struct index out of range → NULL silently |
| `member_interests_views.sql` | `TRY_CAST(year_declared AS INTEGER)` | Non-integer year → NULL; aggregation drops row |

For required columns: replace with `CAST` (will error loudly) or fix upstream so the cast is unnecessary.
For genuinely optional fields: add `WHERE col IS NOT NULL` downstream so NULLs don't reach the UI silently.

---

## CAT-6 — COALESCE hiding NULL propagation from broken joins

`COALESCE(unique_member_code, '')` and similar patterns hide NULL values from failed joins.
The app receives an empty string and renders a blank member — no error, invisible data loss.

| File | Expression | Risk |
|---|---|---|
| `attendance_member_year_summary.sql` | `COALESCE(unique_member_code, '')` | Empty string passes through; member link in UI points to nothing |
| `attendance_year_rank.sql` | `COALESCE(unique_member_code, '')` | Same |
| `lobbying_index.sql` | `COALESCE(unique_member_code, '')` | Same |
| `payments_member_detail.sql` | `COALESCE(unique_member_code, '')` | Same |
| `payments_yearly_evolution.sql` | `MAX(COALESCE(unique_member_code, ''))` | Aggregation of empty strings |

Fix: ensure `unique_member_code` is always populated before these views run (enrichment step).
Replace `COALESCE(…, '')` with `COALESCE(…, NULL)` as a temporary step so NULLs remain visible.

---

## CAT-7 — Unresolved NULL placeholder columns

These emit `NULL::VARCHAR` for columns that should eventually carry real data.
Low urgency but creates silent gaps in the source metadata the app shows.

| File | Columns | Note |
|---|---|---|
| `attendance_member_summary.sql` | `mart_version`, `code_version` | Populate from pipeline metadata |
| `attendance_summary.sql` | `avg_attendance_rate`, `mart_version`, `code_version` | `avg_attendance_rate` should be computed |
| `lobbying_recent_returns.sql` | `member_name` | Missing join to member table |
| `lobbying_summary.sql` | `first_period`, `last_period` | Should be computed from `bilateral_relationships` |
| `member_interests_views.sql` | `member_id`, `source_pdf_url`, `source_page_number` | Tracked in comments as TODOs |

---

## CAT-8 — SPLIT_PART on `::` delimiter in chamber field

`lobbying_revolving_door.sql:10`:
```sql
SPLIT_PART(COALESCE(current_or_former_dpos_chamber, ''), '::', 1) AS chamber_display
```

The `::` in the raw field value is a data issue — two values concatenated with a DuckDB cast operator as a separator. This is either a pipeline bug or an undocumented encoding. Fix in `lobby_processing.py` before this SQL is relied upon.

---

## Summary counts

| Category | Count | Priority |
|---|---|---|
| CAT-1 Casts to move to pipeline | 20 | High |
| CAT-2 Joins to move to pipeline | 5 | High |
| CAT-3 CSV → parquet | 5 | Medium |
| CAT-4 Template path constants | 8 views | Medium |
| CAT-5 TRY_CAST masking failures | 5 | Medium |
| CAT-6 COALESCE hiding join nulls | 5 | Medium |
| CAT-7 NULL placeholders | ~12 columns | Low |
| CAT-8 SPLIT_PART on `::` delimiter | 1 | High (data bug) |
