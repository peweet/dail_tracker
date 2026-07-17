-- v_ministerial_diary_dept_minister_period — the department drill-down rollup:
-- for one department, its ministers (current + former) ranked by external
-- meetings logged, at every period grain the page's Year/Month filter can ask
-- for. Graduated out of utility/pages_code/ministerial_diaries.py::_dept_drill
-- (logic-firewall audit 2026-07-16).
--
-- Period-grain encoding (shared by all the ministerial_diary_zz_* rollups):
--   period_grain = 'all'   → whole corpus      (period_year, period_month NULL)
--   period_grain = 'year'  → one year          (period_month NULL)
--   period_grain = 'month' → one year + month
-- Retrieval filters on period_grain + the period columns (WHERE-only), so the
-- page's period filter never re-aggregates in pandas.
--
-- depts = the minister's FULL portfolio within the same period (comma-joined,
-- sorted) — a minister may hold several departments (e.g. Ryan = Transport +
-- Climate), and the card badges show the whole portfolio, not just the
-- department being drilled.
--
-- Counts are coverage-driven (diaries are self-curated, quarterly-in-arrears)
-- — the page frames them as the published record, never a ranking of activity.
--
-- Grain: department × minister × period.
-- Depends on v_ministerial_diary_meetings — the zz_ filename keeps it loading
-- after ministerial_diary_meetings.sql within the ministerial_diary_*.sql glob.
CREATE OR REPLACE VIEW v_ministerial_diary_dept_minister_period AS
WITH base AS (
    SELECT
        minister,
        department,
        entry_date,
        CAST(EXTRACT(year FROM entry_date) AS INTEGER)  AS period_year,
        CAST(EXTRACT(month FROM entry_date) AS INTEGER) AS period_month
    FROM v_ministerial_diary_meetings
    WHERE minister IS NOT NULL AND minister <> ''
      AND department IS NOT NULL AND department <> ''
),
per AS (
    SELECT
        department,
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
        MAX(entry_date) AS last_meeting
    FROM base
    GROUP BY GROUPING SETS (
        (department, minister),
        (department, minister, period_year),
        (department, minister, period_year, period_month)
    )
),
portfolio AS (
    SELECT
        minister,
        CASE
            WHEN GROUPING(period_year) = 1  THEN 'all'
            WHEN GROUPING(period_month) = 1 THEN 'year'
            ELSE 'month'
        END AS period_grain,
        period_year,
        period_month,
        string_agg(DISTINCT department, ',' ORDER BY department) AS depts
    FROM base
    GROUP BY GROUPING SETS (
        (minister),
        (minister, period_year),
        (minister, period_year, period_month)
    )
)
SELECT
    p.department,
    p.minister,
    p.period_grain,
    p.period_year,
    p.period_month,
    p.meetings,
    p.first_meeting,
    p.last_meeting,
    pf.depts
FROM per p
JOIN portfolio pf
  ON p.minister = pf.minister
 AND p.period_grain = pf.period_grain
 AND p.period_year IS NOT DISTINCT FROM pf.period_year
 AND p.period_month IS NOT DISTINCT FROM pf.period_month
ORDER BY p.meetings DESC;
