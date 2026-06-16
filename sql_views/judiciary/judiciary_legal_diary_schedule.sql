-- v_judiciary_legal_diary_schedule — daily judge sitting-schedule (Tier A).
-- Sources (UNIONED, disjoint by court — see doc/LEGAL_DIARY_OPENVIEW_BUILD_PLAN.md):
--   data/gold/parquet/judicial_legal_diary_schedule.parquet           (.docx — HIGH COURT only)
--   data/gold/parquet/judicial_legal_diary_openview_schedule.parquet  (OpenView — Circuit +
--     Supreme/Appeal/Central Criminal)
--
-- Grain: one row per (diary_date, court, [venue], courtroom, judge, list_type, time) — a
-- judge's sitting session. Names ONLY public officials in their public function; there is
-- NO party data here, so no anonymisation is involved. n_items = case lines attributed to
-- that session. venue (Circuit town) and panel_size are OpenView-only (NULL / 1 for .docx).
CREATE OR REPLACE VIEW v_judiciary_legal_diary_schedule AS
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
ORDER BY diary_date DESC, court, courtroom, judge;
