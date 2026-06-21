-- v_ministerial_diary_org_overlap — the PRIMARY feed: organisations ranked by how
-- often ministers logged meeting them, cross-referenced against the lobbying register.
-- Source: data/gold/parquet/ministerial_diary_org_overlap.parquet
--   (extractors/diary_promote_gold.py <- the vetted sandbox overlap; diary chain =
--    ministerial_diaries_extract -> diary_entry_classify -> diary_org_match -> diary_lobbying_overlap).
--
-- READING THIS HONESTLY (surfaced in the page provenance, never as a claim of influence):
--   * a diary meeting is CO-OCCURRENCE, not a lobbying return — `corroborated` means the
--     org BOTH met AND filed a lobbying return naming the same minister (the strong signal);
--   * `is_state_body` splits government-agency access (IDA/HSE — expected, ~0 returns) from
--     outside-interest access — the page leads with outside interests;
--   * diaries are self-curated, non-exhaustive, published quarterly-in-arrears;
--   * meeting COUNTS are coverage-driven (more departments ingested over time), never a trend.
-- `corroborated` is the ONLY register-cross-ref flag exposed, and deliberately so: it is a
-- POSITIVE join (true = the org both met AND filed a return naming that minister — reliable).
-- The NEGATIVE ("met but never lobbied") is still NOT exposed — `total_lobbying_returns = 0`
-- can mean "unknown" (a residual name-join miss), not "did not lobby", and surfacing it would
-- defame heavy lobbyists. NOTE (2026-06-21): the worst miss class — orgs whose register name
-- tags the acronym ("Construction Industry Federation (CIF)", "The Irish Farmers' Association -
-- IFA") or files under the bare acronym ("Ibec") while the diary carries the expanded name — is
-- now RESOLVED by org_key() in extractors/diary_lobbying_overlap.py (reuses the curated ACRONYMS
-- map to converge both sides), so CIF/IFA/Ibec now corroborate correctly. Other name forms
-- (e.g. "Dublin Chamber" vs "Dublin Chamber of Commerce") remain unbridged → keep negative hidden
-- until a fuller org-identity alias map lands.
CREATE OR REPLACE VIEW v_ministerial_diary_org_overlap AS
SELECT
    matched_org_name AS organisation,
    sector,
    is_state_body,
    meetings,
    high_conf_meetings,
    ministers_met,
    ministers_lobbied_and_met,
    total_lobbying_returns,
    (ministers_lobbied_and_met > 0) AS corroborated,
    first_meeting,
    last_meeting
FROM read_parquet('data/gold/parquet/ministerial_diary_org_overlap.parquet')
ORDER BY meetings DESC, ministers_met DESC;
