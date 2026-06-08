-- v_attendance_year_member_counts — per-year distinct-member counts
-- (denominator for "TDs who appeared in attendance records this year").
-- One row per year. Used by the attendance-overview year strip.
--
-- Source: v_attendance_member_year_summary (already registered).
-- File name starts with 'attendance_' so attendance_data.py picks it up.

CREATE OR REPLACE VIEW v_attendance_year_member_counts AS
SELECT CAST(year AS INTEGER)              AS year,
       house,
       COUNT(DISTINCT member_name)        AS members_count
FROM v_attendance_member_year_summary
WHERE year IS NOT NULL
GROUP BY year, house
ORDER BY year ASC;
