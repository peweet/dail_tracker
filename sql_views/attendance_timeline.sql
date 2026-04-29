-- TODO: Switch read_csv_auto to parquet once available

-- ENRICH_MIGRATION_REQUIRED: the LEFT JOIN on flattened_members.csv below violates the
-- no-join-in-views rule. The party + constituency columns should be resolved upstream in
-- enrich.py (or equivalent enrichment step) and written into a single denormalised CSV/
-- parquet before this view is created. Once that enriched file exists, remove the `mem`
-- CTE and the LEFT JOIN and read party/constituency directly from the enriched source.
-- Do not modify this view until the enriched source file is available and tested.

-- TODO_PIPELINE_VIEW_REQUIRED: deduplicate sitting dates — aggregated_td_tables.csv
-- contains multiple rows for the same (member, date) because the source PDF records
-- both plenary "sitting day" rows and "other day" rows per date. This view emits one
-- row per source CSV row, so a member attending a day with two session types appears
-- twice on the same date. The UI adds a row-number column as a stopgap.
-- Fix: expose a session_type column and deduplicate on DISTINCT (member_name, sitting_date)
-- keeping only the canonical plenary sitting-day row, OR expose the session_type so the
-- UI can filter. See also the sitting_days_count grain bug in attendance_member_year_summary.sql.

CREATE OR REPLACE VIEW v_attendance_timeline AS
WITH att AS (
    SELECT * FROM read_csv_auto('data/silver/aggregated_td_tables.csv')
),
mem AS (
    SELECT first_name, last_name, constituency_name, party
    FROM read_csv_auto('data/silver/flattened_members.csv')
),
joined AS (
    SELECT
        a.*, m.party, m.constituency_name
    FROM att a
    LEFT JOIN mem m
      ON a.first_name = m.first_name AND a.last_name = m.last_name
)
SELECT
    row_number() OVER ()                            AS attendance_timeline_id,
    iso_sitting_days_attendance                     AS sitting_date,
    identifier                                      AS member_id,
    CONCAT(first_name, ' ', last_name)              AS member_name,
    TRUE                                            AS present_flag,
    'Present'                                       AS attendance_status,
    COALESCE(party, '')                             AS party_name,
    COALESCE(constituency_name, '')                 AS constituency,
    'pipeline'                                      AS latest_run_id,
    current_timestamp                               AS latest_fetch_timestamp_utc,
    'data/silver/aggregated_td_tables.csv'          AS source_summary,
    NULL::VARCHAR                                   AS mart_version,
    NULL::VARCHAR                                   AS code_version
FROM joined;