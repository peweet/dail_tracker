-- v_stateboards_roster — every CURRENT state-board seat as a first-class row,
-- with HAND-CURATED Wikidata "outside role" identity columns.
-- Source: data/gold/parquet/stateboards_roster.parquet (produced by
-- extractors/stateboards_roster_extract.py from membership.stateboards.ie —
-- the DPER register of state-board membership, ~250 bodies, 20 departments —
-- joined with data/_meta/stateboards_wikidata_curated.csv).
--
-- Complements v_public_appointments: Iris carries appointment EVENTS; this is
-- the LIVE roster. The wikidata_* columns are populated ONLY for member names
-- whose identity a human verified (curated CSV; see wikidata_curation_note).
-- Automated name-matching was removed 2026-06-12: ~1 in 4 auto-matches was the
-- wrong same-named person. NULL wikidata_qid means "not curated", not
-- "no outside roles".
--
-- Grain: one row per (body, member seat). Pages/tools facet in pandas off
-- this single frame (display_only, logic-firewall-safe).
CREATE OR REPLACE VIEW v_stateboards_roster AS
SELECT
    department,
    body,
    body_full,
    member_name,
    position_type,           -- Board Member | Chairperson | ...
    basis_of_appointment,    -- PAS Process | Appointed by the Minister | nominating body | ...
    first_appointed,
    reappointed,
    expiry_date,
    wikidata_qid,            -- hand-curated identities only (66 as of 2026-06-12)
    wikidata_url,
    wikidata_label,
    wikidata_description,
    wikidata_occupations,    -- "; "-joined public-record occupations (P106)
    wikidata_employers,      -- (P108)
    wikidata_positions_held, -- (P39)
    wikidata_curation_note,  -- why this identity was accepted (verification trail)
    source_url
FROM read_parquet('data/gold/parquet/stateboards_roster.parquet')
ORDER BY department, body, member_name;
