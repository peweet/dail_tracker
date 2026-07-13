-- Source: data/gold/parquet/attendance_by_td_year.parquet (written by
-- votes/enrich.py).
-- Join resolved upstream — no join needed here.
--
-- attended_count = total_days (sitting_days + other_days). The PDF
-- source publishes two categorical day columns per member-year:
--   * Sitting days  — chamber/plenary presence
--   * Other days    — committee / other recorded business
-- Both come straight from the Oireachtas member-attendance PDFs and
-- are deduped per-date in attendance.py before any counting, so each
-- count is a clean nunique of distinct ISO dates in its column.
--
-- A previous revision (2026-05-26) treated `max(total_days) > official
-- sitting days` (e.g. 131 > 83 in 2024) as evidence of double-counting
-- and dropped other_days from the headline. That inequality is
-- mathematically expected — committee work adds to the plenary total
-- without violating the 83-sitting-day chamber cap — and the change
-- silently halved the displayed attendance for most TDs. Reverted.
--
-- sitting_days and other_days are exposed separately so the UI can:
--   * show the total with a (plenary / other) sub-breakdown, and
--   * compute attendance-rate against the official chamber ceiling
--     using sitting_days only — numerator and denominator both
--     chamber-only, so bars don't exceed 100%.
--
-- IS_MINISTER IS DATE-BOUNDED (2026-07-13, MCP sweep defect 6). The gold
-- parquet's is_minister is a point-in-time snapshot taken at build time and was
-- retroactively wrong in both directions (e.g. Michael Healy-Rae true on
-- 2023/24 rows though his Minister-of-State post began 2025-01-29; a resigned
-- minister false on the years they actually served). The flag here is TRUE
-- only when a dated ministerial span overlaps the row's year, from two
-- published records: the Wikidata tenure spine (data/silver/
-- ministerial_tenure.parquet, senior ministers, code-keyed) and the Oireachtas
-- member-feed office slots (flattened_members / flattened_seanad_members,
-- start AND end dates, includes Ministers of State, Taoiseach, Tánaiste).
-- Members with no dated span record at all fall back to the gold snapshot
-- (typically former members no longer on the feed).

-- Reads both houses (identical schema from enrich._build_attendance_by_year).
-- A `house` literal is tagged per source so dependent views (rank, member
-- counts) can partition by house — without it, TD ranks would be computed
-- against a mixed Dáil+Seanad pool. The per-member panel filters by
-- member_name/unique_member_code, so a member resolves to their own rows.
CREATE OR REPLACE VIEW v_attendance_member_year_summary AS
WITH unioned AS (
    SELECT *, 'Dáil' AS house
    FROM read_parquet('data/gold/parquet/attendance_by_td_year.parquet')
    UNION ALL BY NAME
    SELECT *, 'Seanad' AS house
    FROM read_parquet('data/gold/parquet/seanad_attendance_by_year.parquet')
),
office_slots AS (
    SELECT unique_member_code, office_1_name AS office_name,
           TRY_CAST(office_1_start_date AS DATE) AS s, TRY_CAST(office_1_end_date AS DATE) AS e
    FROM read_parquet('data/silver/parquet/flattened_members.parquet') WHERE office_1_name IS NOT NULL
    UNION ALL
    SELECT unique_member_code, office_2_name,
           TRY_CAST(office_2_start_date AS DATE), TRY_CAST(office_2_end_date AS DATE)
    FROM read_parquet('data/silver/parquet/flattened_members.parquet') WHERE office_2_name IS NOT NULL
    UNION ALL
    SELECT unique_member_code, office_3_name,
           TRY_CAST(office_3_start_date AS DATE), TRY_CAST(office_3_end_date AS DATE)
    FROM read_parquet('data/silver/parquet/flattened_members.parquet') WHERE office_3_name IS NOT NULL
    UNION ALL
    SELECT unique_member_code, office_4_name,
           TRY_CAST(office_4_start_date AS DATE), TRY_CAST(office_4_end_date AS DATE)
    FROM read_parquet('data/silver/parquet/flattened_members.parquet') WHERE office_4_name IS NOT NULL
    UNION ALL
    SELECT unique_member_code, office_5_name,
           TRY_CAST(office_5_start_date AS DATE), TRY_CAST(office_5_end_date AS DATE)
    FROM read_parquet('data/silver/parquet/flattened_members.parquet') WHERE office_5_name IS NOT NULL
    UNION ALL
    SELECT unique_member_code, office_6_name,
           TRY_CAST(office_6_start_date AS DATE), TRY_CAST(office_6_end_date AS DATE)
    FROM read_parquet('data/silver/parquet/flattened_members.parquet') WHERE office_6_name IS NOT NULL
    UNION ALL
    SELECT unique_member_code, office_1_name,
           TRY_CAST(office_1_start_date AS DATE), TRY_CAST(office_1_end_date AS DATE)
    FROM read_parquet('data/silver/parquet/flattened_seanad_members.parquet') WHERE office_1_name IS NOT NULL
),
minister_spans AS (
    SELECT member_code AS unique_member_code,
           CAST(start_date AS DATE) AS s,
           COALESCE(CAST(end_date AS DATE), DATE '9999-12-31') AS e
    FROM read_parquet('data/silver/ministerial_tenure.parquet')
    WHERE member_code IS NOT NULL AND start_date IS NOT NULL
    UNION ALL
    SELECT unique_member_code, s, COALESCE(e, DATE '9999-12-31')
    FROM office_slots
    WHERE s IS NOT NULL
      AND (office_name LIKE 'Minister%' OR office_name IN ('Taoiseach', 'Tánaiste'))
),
years AS (SELECT DISTINCT CAST(year AS INTEGER) AS year FROM unioned WHERE year IS NOT NULL),
flag_minister AS (
    SELECT DISTINCT ms.unique_member_code, y.year
    FROM minister_spans ms
    CROSS JOIN years y
    WHERE ms.s <= make_date(y.year, 12, 31) AND ms.e >= make_date(y.year, 1, 1)
),
cov_minister AS (SELECT DISTINCT unique_member_code FROM minister_spans)
SELECT
    COALESCE(u.unique_member_code, '') AS unique_member_code,
    u.full_name                        AS member_name,
    u.member_id,
    CAST(u.year AS INTEGER)            AS year,
    u.total_days                       AS attended_count,
    u.sitting_days                     AS sitting_days,
    u.other_days                       AS other_days,
    u.total_days                       AS total_days,
    COALESCE(u.party_name,    '')      AS party_name,
    COALESCE(u.constituency,  '')      AS constituency,
    (CASE WHEN fm.unique_member_code IS NOT NULL THEN TRUE
          WHEN cm.unique_member_code IS NOT NULL THEN FALSE
          ELSE COALESCE(u.is_minister, FALSE) END) AS is_minister,
    u.house
FROM unioned u
LEFT JOIN flag_minister fm
       ON fm.unique_member_code = u.unique_member_code AND fm.year = CAST(u.year AS INTEGER)
LEFT JOIN cov_minister cm ON cm.unique_member_code = u.unique_member_code
WHERE u.full_name IS NOT NULL
  AND u.year IS NOT NULL;
