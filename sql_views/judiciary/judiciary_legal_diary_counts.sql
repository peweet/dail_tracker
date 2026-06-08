-- v_judiciary_legal_diary_counts — per-session case-item counts (Tier B).
-- Source: data/gold/parquet/judicial_legal_diary_counts.parquet
--   (extractors/legal_diary_extract.py).
--
-- Grain: one row per (diary_date, court, judge, list_type) that had >=1 case
-- item. Aggregate density only — how busy each judge's list was on the day —
-- with NO party data. Safe by construction.
CREATE OR REPLACE VIEW v_judiciary_legal_diary_counts AS
SELECT
    diary_date,
    court,
    judge,
    list_type,
    n_items
FROM read_parquet('data/gold/parquet/judicial_legal_diary_counts.parquet')
ORDER BY diary_date DESC, n_items DESC;
