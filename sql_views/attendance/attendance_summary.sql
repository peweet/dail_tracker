-- TODO: Switch read_csv_auto to parquet once available
CREATE OR REPLACE VIEW v_attendance_summary AS
WITH att AS (
    SELECT * FROM read_csv_auto('data/silver/aggregated_td_tables.csv')
)
SELECT
    'pipeline'                                      AS latest_run_id,
    COUNT(DISTINCT CONCAT(first_name, ' ', last_name)) AS members_count,
    COUNT(DISTINCT iso_sitting_days_attendance)      AS sitting_count,
    NULL::DOUBLE                                    AS avg_attendance_rate,
    MIN(iso_sitting_days_attendance)                AS first_sitting_date,
    MAX(iso_sitting_days_attendance)                AS last_sitting_date,
    current_timestamp                               AS latest_fetch_timestamp_utc,
    'data/silver/aggregated_td_tables.csv'          AS source_summary,
    NULL::VARCHAR                                   AS mart_version,
    NULL::VARCHAR                                   AS code_version
FROM att;