-- v_ministerial_diary_dept_period — per-department rollup of the broad meetings
-- landscape (v_ministerial_diary_meetings: every external meeting, no org match
-- required), at every period grain the page's Year/Month filter can ask for.
-- Graduated out of utility/pages_code/ministerial_diaries.py::_render_by_dept
-- (logic-firewall audit 2026-07-16).
--
-- Period-grain encoding (shared by all the ministerial_diary_zz_* rollups):
--   period_grain = 'all'   → whole corpus      (period_year, period_month NULL)
--   period_grain = 'year'  → one year          (period_month NULL)
--   period_grain = 'month' → one year + month
--
-- ministers counts DISTINCT named ministers only (unattributed entries — a
-- Minister-of-State file with no surname — don't inflate the card).
-- Counts are coverage-driven (diaries are self-curated, quarterly-in-arrears).
--
-- Grain: department × period.
-- Depends on v_ministerial_diary_meetings — the zz_ filename keeps it loading
-- after ministerial_diary_meetings.sql within the ministerial_diary_*.sql glob.
CREATE OR REPLACE VIEW v_ministerial_diary_dept_period AS
SELECT
    department,
    CASE
        WHEN GROUPING(period_year) = 1  THEN 'all'
        WHEN GROUPING(period_month) = 1 THEN 'year'
        ELSE 'month'
    END             AS period_grain,
    period_year,
    period_month,
    COUNT(*)        AS meetings,
    COUNT(DISTINCT minister) FILTER (WHERE minister IS NOT NULL AND minister <> '') AS ministers,
    MIN(entry_date) AS first_meeting,
    MAX(entry_date) AS last_meeting
FROM (
    SELECT
        minister,
        department,
        entry_date,
        CAST(EXTRACT(year FROM entry_date) AS INTEGER)  AS period_year,
        CAST(EXTRACT(month FROM entry_date) AS INTEGER) AS period_month
    FROM v_ministerial_diary_meetings
    WHERE department IS NOT NULL AND department <> ''
)
GROUP BY GROUPING SETS (
    (department),
    (department, period_year),
    (department, period_year, period_month)
)
ORDER BY meetings DESC;
