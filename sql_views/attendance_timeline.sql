-- TODO: Switch read_csv_auto to parquet once available

-- ENRICH_MIGRATION_REQUIRED: the LEFT JOIN on flattened_members.csv below violates the
-- no-join-in-views rule. The party + constituency columns should be resolved upstream in
-- enrich.py (or equivalent enrichment step) and written into a single denormalised CSV/
-- parquet before this view is created. Once that enriched file exists, remove the `mem`
-- CTE and the LEFT JOIN and read party/constituency directly from the enriched source.
-- Do not modify this view until the enriched source file is available and tested.

-- House-aware: unions both chambers' silver sitting tables and member files,
-- joining on (name, house) so a Senator's calendar resolves to Seanad sitting
-- dates. The panel queries by member_name + year.
CREATE OR REPLACE VIEW v_attendance_timeline AS
WITH att AS (
    SELECT *, 'Dáil' AS house,
        ROW_NUMBER() OVER (
            PARTITION BY identifier, iso_sitting_days_attendance ORDER BY iso_sitting_days_attendance
        ) AS _dedup_rn
    FROM read_csv_auto('data/silver/aggregated_td_tables.csv')
    UNION ALL BY NAME
    SELECT *, 'Seanad' AS house,
        ROW_NUMBER() OVER (
            PARTITION BY identifier, iso_sitting_days_attendance ORDER BY iso_sitting_days_attendance
        ) AS _dedup_rn
    FROM read_csv_auto('data/silver/seanad_aggregated_tables.csv')
),
deduped AS (
    SELECT * EXCLUDE (_dedup_rn) FROM att WHERE _dedup_rn = 1
),
mem AS (
    SELECT first_name, last_name, constituency_name, party, 'Dáil' AS house
    FROM read_csv_auto('data/silver/flattened_members.csv')
    UNION ALL BY NAME
    SELECT first_name, last_name, constituency_name, party, 'Seanad' AS house
    FROM read_csv_auto('data/silver/flattened_seanad_members.csv')
),
joined AS (
    SELECT
        a.*, m.party, m.constituency_name
    FROM deduped a
    LEFT JOIN mem m
      ON a.first_name = m.first_name AND a.last_name = m.last_name AND a.house = m.house
)
SELECT
    row_number() OVER ()                            AS attendance_timeline_id,
    iso_sitting_days_attendance                     AS sitting_date,
    strftime(iso_sitting_days_attendance, '%d %b %Y') AS date_str,
    strftime(iso_sitting_days_attendance, '%A')       AS weekday,
    identifier                                      AS member_id,
    CONCAT(first_name, ' ', last_name)              AS member_name,
    TRUE                                            AS present_flag,
    'Present'                                       AS attendance_status,
    COALESCE(party, '')                             AS party_name,
    COALESCE(constituency_name, '')                 AS constituency,
    house,
    'pipeline'                                      AS latest_run_id,
    current_timestamp                               AS latest_fetch_timestamp_utc,
    'data/silver/aggregated_td_tables.csv'          AS source_summary,
    NULL::VARCHAR                                   AS mart_version,
    NULL::VARCHAR                                   AS code_version
FROM joined;