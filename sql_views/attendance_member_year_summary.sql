-- Source: data/gold/parquet/attendance_by_td_year.parquet (written by
-- pipeline_sandbox/attendance_member_enrichment.py).
-- Join resolved upstream — no join needed here.
--
-- attended_count = sitting_days (plenary sittings only).
--
-- Audit fix (2026-05-26): previously attended_count = total_days
-- (sitting + committee), which produced impossible Hall-of-Fame values —
-- max 114 days in a year (2024) with 83 official sitting days; max 139
-- in 2025 (82 sitting days). The audit doc cites these as a P0 because
-- the leaderboard numbers exceeded the Oireachtas-published total.
-- Switching to sitting_days only restores numerical sanity AND matches
-- the page's user-facing caveat ("Attendance figures reflect days a
-- member was recorded present in the Dáil chamber on scheduled sitting
-- days. The record does not capture committee hearings...").
-- The other_days column is still available upstream for any future
-- secondary view ("committee participation"). other_days +
-- sitting_days = total_days.

CREATE OR REPLACE VIEW v_attendance_member_year_summary AS
SELECT
    COALESCE(unique_member_code, '') AS unique_member_code,
    full_name                        AS member_name,
    member_id,
    CAST(year AS INTEGER)            AS year,
    sitting_days                     AS attended_count,
    other_days                       AS other_days,
    total_days                       AS total_days,
    COALESCE(party_name,    '')      AS party_name,
    COALESCE(constituency,  '')      AS constituency,
    COALESCE(is_minister, 'false')   AS is_minister
FROM read_parquet('data/gold/parquet/attendance_by_td_year.parquet')
WHERE full_name IS NOT NULL
  AND year IS NOT NULL;
