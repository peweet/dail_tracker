-- v_judiciary_judge_diary — anonymised Legal Diary listings PER ROSTER JUDGE.
-- Sources:
--   data/gold/parquet/judicial_legal_diary_cases.parquet      (Tier C, ANONYMISED)
--   data/gold/parquet/judiciary_diary_judge_map.parquet       (extractors/judiciary_diary_link.py)
--
-- The diary names judges surname-only; the map resolves each diary (judge, court)
-- string to a roster judge_key (exact / surname-within-court / surname-unique;
-- ambiguous + honorific-conflict + office-title strings stay UNMATCHED and are
-- absent here). This view powers the judge profile's "Before the court" section —
-- the join lives HERE (logic firewall), never in Streamlit.
--
-- PRIVACY: same contract as v_judiciary_legal_diary_cases (this is the same Tier C
-- material, just keyed by judge): in-camera categories dropped at the extractor,
-- natural persons reduced to initials, orgs/State in clear, provenance attached.
-- Listing density is NOT a judge-performance metric and must not be framed as one.
CREATE OR REPLACE VIEW v_judiciary_judge_diary AS
SELECT
    m.judge_key,
    m.judge_name,
    m.match_method,
    c.diary_date,
    c.court,
    c.judge   AS diary_judge_label,
    c.list_type,
    c.status,
    c.category,
    c.case_anonymised,
    c.plaintiff,
    c.defendant,
    c.plaintiff_kind,
    c.source,
    c.source_url,
    c.source_sha256
FROM read_parquet('data/gold/parquet/judicial_legal_diary_cases.parquet') c
JOIN read_parquet('data/gold/parquet/judiciary_diary_judge_map.parquet') m
  ON c.judge = m.judge
 AND c.court IS NOT DISTINCT FROM m.court
WHERE m.judge_key IS NOT NULL
ORDER BY m.judge_key, c.diary_date DESC, c.list_type;
