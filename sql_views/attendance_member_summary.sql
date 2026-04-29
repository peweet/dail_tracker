-- TODO: Switch read_csv_auto to parquet once available

-- ENRICH_MIGRATION_REQUIRED: the LEFT JOIN on flattened_members.csv below violates the
-- no-join-in-views rule. The party + constituency columns should be resolved upstream in
-- enrich.py (or equivalent enrichment step) and written into a single denormalised CSV/
-- parquet before this view is created. Once that enriched file exists, remove the `mem`
-- CTE and the LEFT JOIN and read party/constituency directly from the enriched source.
-- Do not modify this view until the enriched source file is available and tested.

CREATE OR REPLACE VIEW v_attendance_member_summary AS
WITH att AS (
    SELECT DISTINCT identifier, first_name, last_name, sitting_days_count, iso_sitting_days_attendance
    FROM read_csv_auto('data/silver/aggregated_td_tables.csv')
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
),
total_sittings AS (
    SELECT COUNT(DISTINCT iso_sitting_days_attendance) AS total_sitting_count
    FROM read_csv_auto('data/silver/aggregated_td_tables.csv')
)
SELECT
    CONCAT(first_name, ' ', last_name)               AS member_name,
    identifier                                      AS member_id,
    COALESCE(party, '')                             AS party_name,
    COALESCE(constituency_name, '')                 AS constituency,
    sitting_days_count                              AS attended_count,
    total_sitting_count - sitting_days_count         AS absent_count,
    total_sitting_count                             AS sitting_count,
    sitting_days_count::DOUBLE / total_sitting_count AS attendance_rate,
    MIN(iso_sitting_days_attendance)                AS first_sitting_date,
    MAX(iso_sitting_days_attendance)                AS last_sitting_date,
    'pipeline'                                      AS latest_run_id,
    current_timestamp                               AS latest_fetch_timestamp_utc,
    'data/silver/aggregated_td_tables.csv'          AS source_summary,
    NULL::VARCHAR                                   AS mart_version,
    NULL::VARCHAR                                   AS code_version
FROM joined, total_sittings
GROUP BY first_name, last_name, identifier, party, constituency_name, sitting_days_count, total_sitting_count;