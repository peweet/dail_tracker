-- TODO: Switch read_csv_auto to parquet once available
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