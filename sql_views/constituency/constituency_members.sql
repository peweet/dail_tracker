-- v_constituency_members — the current (34th Dáil) TDs of each constituency, one
-- row per TD, for the constituency dossier's roster cards (each links to the
-- member-overview page via unique_member_code).
--
-- Sourced from v_member_registry (the authoritative member list), filtered to the
-- Dáil — constituency is a Dáil concept; Seanad members are panel/university based.
-- "Current TDs" includes any by-election absorptions already in the registry.
CREATE OR REPLACE VIEW v_constituency_members AS
SELECT
    constituency            AS constituency_name,
    unique_member_code,
    member_name,
    party_name,
    is_minister,
    year_elected
FROM v_member_registry
WHERE house = 'Dáil'
  AND constituency IS NOT NULL
ORDER BY constituency, member_name;
