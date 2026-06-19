-- v_constituency_la_crosswalk — the many-to-many bridge between the 43 Dáil
-- constituencies and the 31 local authorities, parsed from the Electoral
-- Commission Constituency Review Report 2023, **Appendix 1** (Specification of
-- Recommended Dáil Constituencies) by reference/ec_constituency_crosswalk_extract.py.
--
-- Source: data/_meta/constituency_la_crosswalk.csv (curated reference, git-tracked).
--   constituency_name matches v_member_registry.constituency / demographics exactly;
--   local_authority matches the LA spending facts exactly (e.g. "Dun Laoghaire-
--   Rathdown" with no fada, as in la_afs_divisions.council).
--
-- ⚠️ CONTEXT ONLY. Geography does NOT nest: a council area can span several
-- constituencies (Dublin City spans 5+) and a constituency can draw on several
-- councils (Sligo-Leitrim draws on Sligo, Leitrim and a Donegal sliver). This view
-- exists to name "the council(s) serving this area" — council euros are NEVER
-- apportioned into a per-constituency figure. link_type='partial' marks a sliver
-- (the council covers only part of this constituency); it is a transparent
-- name-based qualifier, not an area weight.
CREATE OR REPLACE VIEW v_constituency_la_crosswalk AS
SELECT
    constituency_name,
    local_authority,
    CAST(seats AS INTEGER)                       AS seats,
    link_type,                                   -- 'primary' | 'partial'
    la_serves_multiple_constituencies,
    constituency_multi_la,
    source_key
FROM read_csv('data/_meta/constituency_la_crosswalk.csv', header = true, AUTO_DETECT = true)
ORDER BY constituency_name, (link_type = 'primary') DESC, local_authority;
