-- v_judiciary_judge_diary — anonymised Legal Diary listings PER ROSTER JUDGE.
-- Sources (UNIONED, disjoint by court — see doc/LEGAL_DIARY_OPENVIEW_BUILD_PLAN.md):
--   data/gold/parquet/judicial_legal_diary_cases.parquet           (.docx — HIGH COURT only)
--   data/gold/parquet/judicial_legal_diary_openview_cases.parquet  (OpenView — Circuit +
--     Supreme/Appeal/Central Criminal, with full history)
--   data/gold/parquet/judiciary_diary_judge_map.parquet            (extractors/judiciary_diary_link.py)
--
-- The .docx covers only the Four Courts current day; the OpenView source carries the
-- Circuit Court (absent from the .docx) and the higher courts' history. To avoid double-
-- sourcing the three overlap courts (Supreme/Appeal/Central Criminal appear in BOTH), the
-- .docx side is filtered to the HIGH COURT only and OpenView is canonical for the rest.
--
-- The map resolves each diary (judge, court) string to a roster judge_key. PANEL sittings
-- (Supreme / Court of Appeal sit 3–5 judges, joined "A & B & C") have one map row PER
-- member, so this JOIN fans a panel matter out to EVERY member's profile (panel-attribution
-- decision). The join lives HERE (logic firewall), never in Streamlit.
--
-- PRIVACY: same contract as v_judiciary_legal_diary_cases — in-camera categories dropped at
-- the extractor, natural persons reduced to initials, orgs/State in clear, provenance
-- attached. Listing density is NOT a judge-performance metric and must not be framed as one.
CREATE OR REPLACE VIEW v_judiciary_judge_diary AS
WITH cases AS (
    SELECT diary_date, court, judge, list_type, status, category, case_anonymised,
           plaintiff, defendant, plaintiff_kind, source, source_url, source_sha256,
           NULL AS venue, 1 AS panel_size
    FROM read_parquet('data/gold/parquet/judicial_legal_diary_cases.parquet')
    WHERE court = 'High Court'
    UNION ALL
    SELECT diary_date, court, judge, list_type, status, category, case_anonymised,
           plaintiff, defendant, plaintiff_kind, source, source_url, NULL AS source_sha256,
           venue, panel_size
    FROM read_parquet('data/gold/parquet/judicial_legal_diary_openview_cases.parquet')
)
SELECT
    m.judge_key,
    m.judge_name,
    m.match_method,
    c.diary_date,
    c.court,
    c.judge   AS diary_judge_label,
    c.venue,
    c.panel_size,
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
FROM cases c
JOIN read_parquet('data/gold/parquet/judiciary_diary_judge_map.parquet') m
  ON c.judge = m.judge
 AND c.court IS NOT DISTINCT FROM m.court
WHERE m.judge_key IS NOT NULL
ORDER BY m.judge_key, c.diary_date DESC, c.list_type;
