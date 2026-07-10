-- v_la_lpt_adjustment — the Local Property Tax LOCAL ADJUSTMENT FACTOR each of the
-- 31 local authorities adopted per year: the +/-15% variation on the basic LPT rate
-- that the ELECTED COUNCILLORS vote (a reserved function under s.20 Finance (Local
-- Property Tax) Act 2012, as amended). One row per (local_authority, year). This is
-- a whole-council annual money DECISION — the cleanest recurring roll-call-adjacent
-- signal that exists for every council — not a money FACT: never join it into the
-- payments/awarded/budget grains as an amount.
--
-- Source: data/_meta/lpt_local_adjustment_factors.csv — git-tracked, produced by
-- extractors/lpt_laf_extract.py from Revenue's published LAF table (live page for
-- the current year; earlier years recovered from web.archive.org snapshots of the
-- predecessor Revenue page — source_url per row records which).
--
-- local_authority matches v_constituency_la_crosswalk / v_la_chief_executives
-- EXACTLY (e.g. "Dun Laoghaire-Rathdown" with no fada, "Cork County" vs "Cork
-- City", "Limerick" without the "City & County" suffix) — the extractor asserts
-- set-equality against the crosswalk's 31 before writing, so this view LEFT JOINs
-- the council money/performance facts on that key with no normalisation.
CREATE OR REPLACE VIEW v_la_lpt_adjustment AS
SELECT
    local_authority,
    year,                       -- LPT liability year the factor applies to
    adjustment_pct,             -- adopted variation on the base rate, -15.0 .. +15.0
    source_url,                 -- Revenue page (or immutable archive snapshot) parsed
    retrieved_date
FROM read_csv('data/_meta/lpt_local_adjustment_factors.csv', header = true, AUTO_DETECT = true)
ORDER BY year, local_authority;
