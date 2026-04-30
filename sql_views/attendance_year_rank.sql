-- v_attendance_year_rank
-- One row per (member, year) ranked by sitting_days_count within each year.
-- is_minister is passed through from v_attendance_member_year_summary so the UI
-- can exclude ministers from Hall of Shame (lower plenary attendance is expected
-- for cabinet ministers who are constitutionally required to attend cabinet meetings,
-- represent Ireland abroad, and conduct ministerial duties on sitting days).

CREATE OR REPLACE VIEW v_attendance_year_rank AS
SELECT
    COALESCE(unique_member_code, '') AS unique_member_code,
    member_name,
    year,
    attended_count,
    party_name,
    constituency,
    is_minister,
    RANK() OVER (PARTITION BY year ORDER BY attended_count DESC) AS rank_high,
    RANK() OVER (PARTITION BY year ORDER BY attended_count ASC)  AS rank_low,
    RANK() OVER (PARTITION BY year ORDER BY CASE WHEN is_minister THEN NULL ELSE attended_count END ASC NULLS LAST) AS rank_low_exc_ministers
FROM v_attendance_member_year_summary;
