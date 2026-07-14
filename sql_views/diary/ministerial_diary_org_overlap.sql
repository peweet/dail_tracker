-- v_ministerial_diary_org_overlap — the PRIMARY feed: organisations ranked by how
-- often ministers logged meeting them, cross-referenced against the lobbying register.
-- Source: data/gold/parquet/ministerial_diary_org_overlap.parquet
--   (extractors/diary_promote_gold.py <- the vetted sandbox overlap; diary chain =
--    ministerial_diaries_extract -> diary_entry_classify -> diary_org_match -> diary_lobbying_overlap).
--
-- READING THIS HONESTLY (surfaced in the page provenance, never as a claim of influence):
--   * a diary meeting is CO-OCCURRENCE, not a lobbying return — `corroborated` means the org BOTH
--     met AND appears on a lobbying return naming the same minister (the strong signal);
--   * `is_state_body` splits government-agency access (IDA/HSE — expected, ~0 returns) from
--     outside-interest access — the page leads with outside interests;
--   * diaries are self-curated, non-exhaustive, published quarterly-in-arrears;
--   * meeting COUNTS are coverage-driven (more departments ingested over time), never a trend.
-- `corroborated` is the ONLY register-cross-ref flag exposed, and deliberately so: it is a
-- POSITIVE join (true = the org both met AND is named on a return naming that minister — reliable).
-- The NEGATIVE ("met but never lobbied") is still NOT exposed — `total_lobbying_returns = 0`
-- can mean "unknown" (a residual name-join miss), not "did not lobby", and surfacing it would
-- defame heavy lobbyists. NOTE (2026-06-21): the worst miss class — orgs whose register name
-- tags the acronym ("Construction Industry Federation (CIF)", "The Irish Farmers' Association -
-- IFA") or files under the bare acronym ("Ibec") while the diary carries the expanded name — is
-- now RESOLVED by org_key() in extractors/diary_lobbying_overlap.py (reuses the curated ACRONYMS
-- map to converge both sides), so CIF/IFA/Ibec now corroborate correctly.
-- REGISTRANT **OR** CLIENT (2026-07-14): the second big miss class is now closed too. The register
-- join matched the return's REGISTRANT only, so any organisation that lobbies through a PR firm /
-- public-affairs consultancy read `total_lobbying_returns = 0` and never corroborated — Roadstone
-- lobbies via Drury and served 0 returns across this whole surface. A return now counts for an org
-- if the org is its registrant OR one of its named clients (+137 orgs gain a register link, +42
-- newly corroborated). These are PER-ORGANISATION association counts — NEVER sum them across
-- organisations, since one return legitimately attaches to its registrant AND to each client.
-- Residual (why the negative stays hidden): the join is an exact key match, so name forms the
-- normaliser cannot bridge (e.g. "Roadstone" vs the register's "Roadstone Wood Ltd") still miss.
--
-- STATE-BODY SUPPLEMENT (2026-07-13, MCP sweep DQ #3): gold derives is_state_body from the
-- sector keyword tag (sector == 'state-semi-state'), which deliberately leaves commercial
-- semi-states / cultural institutions / statutory agencies in their industry sector — so LDA,
-- NCSE, National Concert Hall, Heritage Council, Dublin Port Company, Arts Council etc. read
-- is_state_body=false and contaminate the "outside interest" ranking. Corrected HERE (view-level
-- override, gold untouched — the quarantine-in-view convention) from the hand-curated
-- data/_meta/diary_state_bodies_supplement.csv: exact as-printed org names, each with a statutory
-- basis. Curated list only — no fuzzy matching, no inference.
CREATE OR REPLACE VIEW v_ministerial_diary_org_overlap AS
SELECT
    o.matched_org_name AS organisation,
    o.sector,
    (o.is_state_body OR sb.organisation IS NOT NULL) AS is_state_body,
    o.meetings,
    o.high_conf_meetings,
    o.ministers_met,
    o.ministers_lobbied_and_met,
    o.total_lobbying_returns,
    (o.ministers_lobbied_and_met > 0) AS corroborated,
    o.first_meeting,
    o.last_meeting
FROM read_parquet('data/gold/parquet/ministerial_diary_org_overlap.parquet') o
LEFT JOIN read_csv('data/_meta/diary_state_bodies_supplement.csv', header = true, AUTO_DETECT = true) sb
    ON lower(trim(o.matched_org_name)) = lower(trim(sb.organisation))
ORDER BY o.meetings DESC, o.ministers_met DESC;
