-- v_judiciary_legal_diary_cases — ANONYMISED daily case listings (Tier C).
-- Source: data/gold/parquet/judicial_legal_diary_cases.parquet
--   (extractors/legal_diary_extract.py).
--
-- PRIVACY CONTRACT (agreed 2026-06-05 — see memory
-- project_judiciary_feature_validation). This view is the PUBLISHABLE case layer:
--   * statutory in-camera categories (minors / family / wards / special care /
--     childcare / asylum) are DROPPED at the extractor — they never reach this file;
--   * every natural person is reduced to initials (case_anonymised); organisations
--     and State bodies are kept in clear (the accountability signal);
--   * case references + solicitor names are stripped (quasi-identifiers);
--   * each row carries source + source_url + source_sha256 so the primary public
--     record can be verified.
-- PARTY SPLIT (parser v1.1): plaintiff / defendant / plaintiff_kind are the SAME
-- anonymised material as case_anonymised — the first 'v' segment vs the rest, plus a
-- classification of the applicant side (state-prosecutor | organisation | state-body |
-- individual) computed in the extractor. plaintiff_kind is the "who brings the case"
-- accountability signal; individuals stay initials, named institutions stay in clear.
-- The extractor asserts no raw-name column reaches this parquet and gates every text
-- column (title + split) for residual names; there is deliberately NO un-anonymised
-- text column to select here.
CREATE OR REPLACE VIEW v_judiciary_legal_diary_cases AS
SELECT
    diary_date,
    court,
    judge,
    list_type,
    status,
    category,
    case_anonymised,
    plaintiff,
    defendant,
    plaintiff_kind,
    source,
    source_url,
    source_sha256
FROM read_parquet('data/gold/parquet/judicial_legal_diary_cases.parquet')
ORDER BY diary_date DESC, court, judge;
