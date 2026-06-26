-- v_la_council_meeting_coverage — per-council data-state for the Your-Councillors page so
-- the UI degrades HONESTLY: tier ∈ {roll_call (named votes published, e.g. Carlow),
-- proposer_seconder (decides by agreement — most councils), scanned_pending, cmis_pending,
-- unseeded}. Source: data/_meta/la_council_meeting_coverage.csv.
CREATE OR REPLACE VIEW v_la_council_meeting_coverage AS
SELECT local_authority, tier, clean_minutes, roster_councillors, has_votes
FROM read_csv('data/_meta/la_council_meeting_coverage.csv', header = true, AUTO_DETECT = true)
ORDER BY local_authority;
