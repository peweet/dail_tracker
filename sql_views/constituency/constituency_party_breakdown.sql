-- v_constituency_party_breakdown — seats held per party in each constituency, for
-- the dossier's party-composition bar. One row per (constituency, party); n_seats
-- is the count of current Dáil TDs of that party in that constituency.
CREATE OR REPLACE VIEW v_constituency_party_breakdown AS
SELECT
    constituency                       AS constituency_name,
    COALESCE(party_name, 'Independent') AS party_name,
    COUNT(*)                           AS n_seats
FROM v_member_registry
WHERE house = 'Dáil'
  AND constituency IS NOT NULL
GROUP BY constituency, COALESCE(party_name, 'Independent')
ORDER BY constituency, n_seats DESC, party_name;
