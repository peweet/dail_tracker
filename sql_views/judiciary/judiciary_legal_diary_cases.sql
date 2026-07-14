-- v_judiciary_legal_diary_cases — ANONYMISED daily case listings (Tier C).
-- Sources (UNIONED, disjoint by court — see doc/archive/LEGAL_DIARY_OPENVIEW_BUILD_PLAN.md):
--   data/gold/parquet/judicial_legal_diary_cases.parquet           (.docx — HIGH COURT only)
--   data/gold/parquet/judicial_legal_diary_openview_cases.parquet  (OpenView — Circuit +
--     Supreme/Appeal/Central Criminal, with history; extractors/legal_diary_openview_extract.py)
--
-- PRIVACY CONTRACT (agreed 2026-06-05). This view is the PUBLISHABLE case layer:
--   * statutory in-camera categories (minors / family / wards / special care /
--     childcare / asylum) are DROPPED at the extractor — they never reach these files;
--   * every natural person is reduced to initials (case_anonymised); organisations
--     and State bodies are kept in clear (the accountability signal);
--   * case references + solicitor names are stripped (quasi-identifiers);
--   * each row carries source + source_url (+ source_sha256 for the .docx) so the primary
--     public record can be verified.
-- Both extractors run the SAME residual-name privacy gate before writing gold.
-- venue (Circuit Court town) and panel_size (Supreme/Appeal sit as panels) are OpenView-only
-- columns, NULL / 1 for the .docx High Court rows.
CREATE OR REPLACE VIEW v_judiciary_legal_diary_cases AS
WITH unioned AS (
    SELECT
        diary_date, court, judge, list_type, status, category,
        case_anonymised, plaintiff, defendant, plaintiff_kind,
        source, source_url, source_sha256,
        NULL AS venue, 1 AS panel_size
    FROM read_parquet('data/gold/parquet/judicial_legal_diary_cases.parquet')
    WHERE court = 'High Court'
    UNION ALL
    SELECT
        diary_date, court, judge, list_type, status, category,
        case_anonymised, plaintiff, defendant, plaintiff_kind,
        source, source_url, NULL AS source_sha256,
        venue, panel_size
    FROM read_parquet('data/gold/parquet/judicial_legal_diary_openview_cases.parquet')
)
SELECT * FROM unioned
-- RECENT-WINDOW CAP (temporary, added 2026-06-19) — see judiciary_legal_diary_schedule.sql for
-- the rationale. Keep the window identical across the three legal-diary views.
WHERE CAST(diary_date AS DATE) BETWEEN current_date - 7 AND current_date
ORDER BY diary_date DESC, court, judge;
