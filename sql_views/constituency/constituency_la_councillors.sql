-- v_la_councillors — elected members of each of the 31 local authorities, by Local
-- Electoral Area (LEA). Source: data/_meta/la_councillors.csv (built by
-- extractors/councillors_promote_to_gold.py from the Wikipedia roster; ~96% complete —
-- a few councils undercounted, see v_la_council_meeting_coverage for per-council state).
-- There is NO central API for councillors (unlike the Oireachtas members); like
-- v_la_chief_executives the curated CSV is the identity source. local_authority matches
-- the v_la_* council key exactly.
CREATE OR REPLACE VIEW v_la_councillors AS
SELECT local_authority, lea, name, party, status, source
FROM read_csv('data/_meta/la_councillors.csv', header = true, AUTO_DETECT = true)
ORDER BY local_authority, lea, name;
