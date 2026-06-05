-- v_judiciary_legal_diary_schedule — daily judge sitting-schedule (Tier A).
-- Source: data/gold/parquet/judicial_legal_diary_schedule.parquet
--   (produced by extractors/legal_diary_extract.py from the Courts Service daily
--   Legal Diary .docx; the poller pdf_infra/legal_diary_poller.py archives one
--   file per day so successive diary_date rows accumulate).
--
-- Grain: one row per (diary_date, court, courtroom, judge, list_type, time) —
-- a judge's sitting session. Names ONLY public officials in their public
-- function; there is NO party data here, so no anonymisation is involved.
-- n_items = case lines attributed to that session (0 for schedule-only blocks).
CREATE OR REPLACE VIEW v_judiciary_legal_diary_schedule AS
SELECT
    diary_date,
    court,
    courtroom,
    judge,
    list_type,
    time,
    n_items
FROM read_parquet('data/gold/parquet/judicial_legal_diary_schedule.parquet')
ORDER BY diary_date DESC, court, courtroom, judge;
