-- v_stateboards_roster — every CURRENT state-board seat as a first-class row,
-- with optional Wikidata "outside role" enrichment.
-- Source: data/gold/parquet/stateboards_roster.parquet (produced by
-- wikidata/stateboards_wikidata_enrich.py over the silver roster scraped by
-- extractors/stateboards_roster_extract.py from membership.stateboards.ie —
-- the DPER register of state-board membership, ~250 bodies, 20 departments).
--
-- Complements v_public_appointments: Iris carries appointment EVENTS; this is
-- the LIVE roster. wikidata_match governs the enrichment columns:
--   matched   = exactly one living Irish-signal exact-name Wikidata human —
--               STILL a name-based match; UI must say "possible match", never
--               assert identity (no-inference rule).
--   ambiguous | none | skipped | NULL(not queried) = enrichment columns NULL.
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
    wikidata_match,          -- matched | ambiguous | none | skipped | NULL
    wikidata_qid,
    wikidata_url,
    wikidata_label,
    wikidata_description,
    wikidata_occupations,    -- "; "-joined public-record occupations (P106)
    wikidata_employers,      -- (P108)
    wikidata_positions_held, -- (P39)
    source_url
FROM read_parquet('data/gold/parquet/stateboards_roster.parquet')
ORDER BY department, body, member_name;
