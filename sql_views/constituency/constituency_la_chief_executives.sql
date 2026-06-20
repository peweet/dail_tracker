-- v_la_chief_executives — the appointed executive head of each of the 31 local
-- authorities (the "Chief Executive"; in Limerick, post the 2024 directly-elected
-- mayor reform, the "Director General"). This is the UNELECTED official who, by
-- law, performs the EXECUTIVE functions of the council — staff, contracts, planning
-- permissions, day-to-day spend — as distinct from the elected councillors, who hold
-- only the short statutory list of RESERVED functions (adopt budget/development
-- plan, borrow, appoint the CE). See Local Government Act 2001 Part 14, as
-- substituted by the Local Government Reform Act 2014.
--
-- Source: data/_meta/la_chief_executives.csv — HAND-CURATED, git-tracked. Each name
-- was verified against an authoritative web page (the council's own site preferred;
-- CCMA/LGMA or reputable news as backup); source_url carries that page. There is no
-- API for this — like v_stateboards_roster, the curated CSV is the only identity
-- source. confidence + as_of_date record the verification state.
--
-- local_authority matches v_constituency_la_crosswalk / la_afs_divisions.council
-- EXACTLY (e.g. "Dun Laoghaire-Rathdown" with no fada, "Limerick", "Waterford"), so
-- this view LEFT JOINs the council money/performance facts on that key.
--
-- ⚠️ Salary is NOT published per-council; salary_eur is intentionally blank. The CE
-- pay scale is a national band (~€132,511–€189,301) set by council grade — present
-- that as context, never as a per-person figure here. term_end is blank unless the
-- council itself stated it (the 7-year term is a general fact, not a per-row claim).
CREATE OR REPLACE VIEW v_la_chief_executives AS
SELECT
    local_authority,
    council_slug,
    council_name,
    council_type,            -- County | City | City and County
    head_title,              -- Chief Executive | Director General (Limerick)
    chief_executive,
    appointed_year,
    term_end,
    salary_eur,              -- blank: not published per-council (see header)
    profile_url,
    source_url,
    confidence,              -- verification confidence (all 'high' at first build)
    as_of_date
FROM read_csv('data/_meta/la_chief_executives.csv', header = true, AUTO_DETECT = true)
ORDER BY local_authority;
