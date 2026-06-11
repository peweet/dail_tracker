-- v_judiciary_judge_sittings — Tier A sitting sessions PER ROSTER JUDGE.
-- Sources:
--   data/gold/parquet/judicial_legal_diary_schedule.parquet   (Tier A — officials only)
--   data/gold/parquet/judiciary_diary_judge_map.parquet       (extractors/judiciary_diary_link.py)
--
-- Adds courtroom + time + list density to the judge profile so a practitioner can
-- see where and when a judge sat (or sits) without cross-referencing the diary tab.
-- Tier A carries no party data by construction. n_items is list density — never a
-- judge-performance metric.
CREATE OR REPLACE VIEW v_judiciary_judge_sittings AS
SELECT
    m.judge_key,
    m.judge_name,
    s.diary_date,
    s.court,
    s.courtroom,
    s.judge   AS diary_judge_label,
    s.list_type,
    s.time,
    s.n_items
FROM read_parquet('data/gold/parquet/judicial_legal_diary_schedule.parquet') s
JOIN read_parquet('data/gold/parquet/judiciary_diary_judge_map.parquet') m
  ON s.judge = m.judge
 AND s.court IS NOT DISTINCT FROM m.court
WHERE m.judge_key IS NOT NULL
ORDER BY m.judge_key, s.diary_date DESC, s.time;
