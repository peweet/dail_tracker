-- v_judiciary_judge_sittings — Tier A sitting sessions PER ROSTER JUDGE.
-- Sources (UNIONED, disjoint by court — see doc/archive/LEGAL_DIARY_OPENVIEW_BUILD_PLAN.md):
--   data/gold/parquet/judicial_legal_diary_schedule.parquet           (.docx — HIGH COURT only)
--   data/gold/parquet/judicial_legal_diary_openview_schedule.parquet  (OpenView — Circuit +
--     Supreme/Appeal/Central Criminal)
--   data/gold/parquet/judiciary_diary_judge_map.parquet               (extractors/judiciary_diary_link.py)
--
-- The .docx side is HIGH-COURT-only; OpenView is canonical for the other courts (same
-- court-disjoint split as v_judiciary_judge_diary). Panel sittings fan out to every member
-- via the per-member map rows. Tier A carries no party data by construction. n_items is
-- list density — never a judge-performance metric.
CREATE OR REPLACE VIEW v_judiciary_judge_sittings AS
WITH sched AS (
    SELECT diary_date, court, courtroom, judge, list_type, time, n_items,
           NULL AS venue, 1 AS panel_size
    FROM read_parquet('data/gold/parquet/judicial_legal_diary_schedule.parquet')
    WHERE court = 'High Court'
    UNION ALL
    SELECT diary_date, court, courtroom, judge, list_type, time, n_items,
           venue, panel_size
    FROM read_parquet('data/gold/parquet/judicial_legal_diary_openview_schedule.parquet')
)
SELECT
    m.judge_key,
    m.judge_name,
    s.diary_date,
    s.court,
    s.courtroom,
    s.judge   AS diary_judge_label,
    s.venue,
    s.panel_size,
    s.list_type,
    s.time,
    s.n_items
FROM sched s
JOIN read_parquet('data/gold/parquet/judiciary_diary_judge_map.parquet') m
  ON s.judge = m.judge
 AND s.court IS NOT DISTINCT FROM m.court
WHERE m.judge_key IS NOT NULL
ORDER BY m.judge_key, s.diary_date DESC, s.time;
