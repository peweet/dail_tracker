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
)
SELECT
    COALESCE(unique_member_code, '') AS unique_member_code,
    full_name                        AS member_name,
    member_id,
    CAST(year AS INTEGER)            AS year,
    total_days                       AS attended_count,
    sitting_days                     AS sitting_days,
    other_days                       AS other_days,
    total_days                       AS total_days,
    COALESCE(party_name,    '')      AS party_name,
    COALESCE(constituency,  '')      AS constituency,
    COALESCE(is_minister, 'false')   AS is_minister,
    house
FROM unioned
WHERE full_name IS NOT NULL
  AND year IS NOT NULL;
