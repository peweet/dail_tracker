-- v_judiciary_legal_diary_schedule — daily judge sitting-schedule (Tier A).
-- Sources (UNIONED, disjoint by court — see doc/archive/LEGAL_DIARY_OPENVIEW_BUILD_PLAN.md):
--   data/gold/parquet/judicial_legal_diary_schedule.parquet           (.docx — HIGH COURT only)
--   data/gold/parquet/judicial_legal_diary_openview_schedule.parquet  (OpenView — Circuit +
--     Supreme/Appeal/Central Criminal)
--
-- Grain: one row per (diary_date, court, [venue], courtroom, judge, list_type, time) — a
-- judge's sitting session. Names ONLY public officials in their public function; there is
-- NO party data here, so no anonymisation is involved. n_items = case lines attributed to
-- that session. venue (Circuit town) and panel_size are OpenView-only (NULL / 1 for .docx).
CREATE OR REPLACE VIEW v_judiciary_legal_diary_schedule AS
WITH unioned AS (
    SELECT
        diary_date, court, courtroom, judge, list_type, time, n_items,
        NULL AS venue, 1 AS panel_size
    FROM read_parquet('data/gold/parquet/judicial_legal_diary_schedule.parquet')
    WHERE court = 'High Court'
    UNION ALL
    SELECT
        diary_date, court, courtroom, judge, list_type, time, n_items,
        venue, panel_size
    FROM read_parquet('data/gold/parquet/judicial_legal_diary_openview_schedule.parquet')
)
SELECT * FROM unioned
-- RECENT-WINDOW CAP (temporary, added 2026-06-19). The diary archive is ~1,833 days /
-- ~790k case rows; loading it whole made Courts & Judiciary the heaviest page in the app
-- and OOM-prone under Windows process pile-up. Until the page paginates by day server-side,
-- scope every legal-diary view to a rolling one-week window (seven days back through today),
-- anchored on current_date so it auto-rolls. Drop this WHERE to restore the full history.
WHERE CAST(diary_date AS DATE) BETWEEN current_date - 7 AND current_date
ORDER BY diary_date DESC, court, courtroom, judge;
