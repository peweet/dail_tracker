-- v_attendance_member_summary — one row per member, aggregated across every year on record.
--
-- Reads from the gold parquet (data/gold/parquet/attendance_by_td_year.parquet) where
-- the (member, year) totals are already produced by the pipeline. The denominator
-- (sitting_count) is the distinct count of plenary sitting dates across all years from
-- the silver CSV — used for the attendance_rate metric.
--
-- attended_count = SUM(total_days) for consistency with v_attendance_member_year_summary
-- (per user decision 2026-04-30: total_days = sitting + committee).
-- attendance_rate = SUM(sitting_days) / sitting_count, so the rate stays a pure
-- "share of plenary sitting days" figure (denominator and numerator both sitting-only).

-- House-aware: per_member unions both chambers (tagged house); sitting_total is
-- computed PER HOUSE (Dáil sitting dates from aggregated_td_tables.csv, Seanad
-- from seanad_aggregated_tables.csv) and joined ON house, so attendance_rate
-- uses each chamber's own sitting-day denominator. The standalone /attendance
-- page filters house='Dáil' to stay TD-only; the member panel resolves by name.
CREATE OR REPLACE VIEW v_attendance_member_summary AS
WITH per_member AS (
    SELECT 'Dáil' AS house, unique_member_code, full_name, member_id, party_name,
           constituency, total_days, sitting_days, year
    FROM read_parquet('data/gold/parquet/attendance_by_td_year.parquet')
    WHERE full_name IS NOT NULL
    UNION ALL BY NAME
    SELECT 'Seanad' AS house, unique_member_code, full_name, member_id, party_name,
           constituency, total_days, sitting_days, year
    FROM read_parquet('data/gold/parquet/seanad_attendance_by_year.parquet')
    WHERE full_name IS NOT NULL
),
per_member_agg AS (
    SELECT
        house,
        COALESCE(unique_member_code, '') AS unique_member_code,
        full_name                        AS member_name,
        ANY_VALUE(member_id)             AS member_id,
        ANY_VALUE(party_name)            AS party_name,
        ANY_VALUE(constituency)          AS constituency,
        SUM(total_days)                  AS attended_count,
        SUM(sitting_days)                AS sitting_days_only,
        MIN(year)                        AS first_year,
        MAX(year)                        AS last_year
    FROM per_member
    GROUP BY house, unique_member_code, full_name
),
sitting_total AS (
    SELECT 'Dáil' AS house,
        COUNT(DISTINCT iso_sitting_days_attendance) AS sitting_count,
        MIN(iso_sitting_days_attendance)            AS first_sitting_date,
        MAX(iso_sitting_days_attendance)            AS last_sitting_date
    FROM read_csv_auto('data/silver/aggregated_td_tables.csv')
    WHERE iso_sitting_days_attendance IS NOT NULL
    UNION ALL
    SELECT 'Seanad' AS house,
        COUNT(DISTINCT iso_sitting_days_attendance),
        MIN(iso_sitting_days_attendance),
        MAX(iso_sitting_days_attendance)
    FROM read_csv_auto('data/silver/seanad_aggregated_tables.csv')
    WHERE iso_sitting_days_attendance IS NOT NULL
)
SELECT
    pm.house,
    pm.unique_member_code,
    pm.member_name,
    pm.member_id,
    COALESCE(pm.party_name, '')                                AS party_name,
    COALESCE(pm.constituency, '')                              AS constituency,
    pm.attended_count,
    st.sitting_count                                           AS sitting_count,
    GREATEST(st.sitting_count - pm.sitting_days_only, 0)       AS absent_count,
    pm.sitting_days_only::DOUBLE / NULLIF(st.sitting_count, 0) AS attendance_rate,
    st.first_sitting_date,
    st.last_sitting_date,
    pm.first_year,
    pm.last_year,
    'pipeline'                                                 AS latest_run_id,
    current_timestamp                                          AS latest_fetch_timestamp_utc,
    'data/gold/parquet/attendance_by_td_year.parquet'          AS source_summary,
    NULL::VARCHAR                                              AS mart_version,
    NULL::VARCHAR                                              AS code_version
FROM per_member_agg pm
JOIN sitting_total st USING (house);
