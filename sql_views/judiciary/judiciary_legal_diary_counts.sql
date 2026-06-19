-- v_judiciary_legal_diary_counts — per-list case-item counts (Tier B).
-- Sources (UNIONED, disjoint by court — see doc/LEGAL_DIARY_OPENVIEW_BUILD_PLAN.md):
--   data/gold/parquet/judicial_legal_diary_counts.parquet            (.docx — HIGH COURT only)
--   data/gold/parquet/judicial_legal_diary_openview_schedule.parquet (OpenView — aggregated)
--
-- Grain: one row per (diary_date, court, judge, list_type) with >=1 item. Aggregate density
-- only — how busy each judge's list was on the day — with NO party data. The .docx path
-- ships a dedicated counts parquet (filtered to High Court here); the OpenView path has no
-- separate counts file, so its density is rolled up from its Tier A schedule. Safe by
-- construction.
CREATE OR REPLACE VIEW v_judiciary_legal_diary_counts AS
WITH unioned AS (
    SELECT diary_date, court, judge, list_type, n_items
    FROM read_parquet('data/gold/parquet/judicial_legal_diary_counts.parquet')
    WHERE court = 'High Court'
    UNION ALL
    SELECT diary_date, court, judge, list_type, SUM(n_items) AS n_items
    FROM read_parquet('data/gold/parquet/judicial_legal_diary_openview_schedule.parquet')
    GROUP BY diary_date, court, judge, list_type
    HAVING SUM(n_items) > 0
)
SELECT * FROM unioned
-- RECENT-WINDOW CAP (temporary, added 2026-06-19) — see judiciary_legal_diary_schedule.sql for
-- the rationale. Keep the window identical across the three legal-diary views.
WHERE CAST(diary_date AS DATE) BETWEEN current_date - 14 AND current_date + 7
ORDER BY diary_date DESC, n_items DESC;
