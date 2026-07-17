-- v_ministerial_diary_minister_period — per-minister rollup of the broad
-- meetings landscape (v_ministerial_diary_meetings: every external meeting, no
-- org match required), at every period grain the page's Year/Month filter can
-- ask for. Graduated out of
-- utility/pages_code/ministerial_diaries.py::_render_by_minister and
-- ::_minister_depts (logic-firewall audit 2026-07-16).
--
-- Period-grain encoding (shared by all the ministerial_diary_zz_* rollups):
--   period_grain = 'all'   → whole corpus      (period_year, period_month NULL)
--   period_grain = 'year'  → one year          (period_month NULL)
--   period_grain = 'month' → one year + month
-- Retrieval filters on period_grain + the period columns (WHERE-only), so the
-- page's period filter never re-aggregates in pandas.
--
-- depts = the minister's portfolio within the period, comma-joined and sorted
-- (a minister may hold several departments — e.g. Ryan = Transport + Climate).
-- Ministers are keyed by SURNAME as published in the diaries (the page's
-- member-registry link stays deferred until a member_code lands here).
-- Counts are coverage-driven (diaries are self-curated, quarterly-in-arrears).
--
-- Grain: minister × period.
-- Depends on v_ministerial_diary_meetings — the zz_ filename keeps it loading
-- after ministerial_diary_meetings.sql within the ministerial_diary_*.sql glob.
CREATE OR REPLACE VIEW v_ministerial_diary_minister_period AS
SELECT
    minister,
    CASE
        WHEN GROUPING(period_year) = 1  THEN 'all'
        WHEN GROUPING(period_month) = 1 THEN 'year'
        ELSE 'month'
    END             AS period_grain,
    period_year,
    period_month,
    COUNT(*)        AS meetings,
    MIN(entry_date) AS first_meeting,
    MAX(entry_date) AS last_meeting,
    string_agg(DISTINCT department, ',' ORDER BY department) AS depts
FROM (
    SELECT
        minister,
        department,
        entry_date,
        CAST(EXTRACT(year FROM entry_date) AS INTEGER)  AS period_year,
        CAST(EXTRACT(month FROM entry_date) AS INTEGER) AS period_month
    FROM v_ministerial_diary_meetings
    WHERE minister IS NOT NULL AND minister <> ''
)
GROUP BY GROUPING SETS (
    (minister),
    (minister, period_year),
    (minister, period_year, period_month)
)
ORDER BY meetings DESC;
