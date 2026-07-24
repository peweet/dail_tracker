-- v_ipas_la_profile — international-protection applicants by LOCAL AUTHORITY, with
-- population and a real per-1,000 rate. The council-map contract.
--
-- Source: IPAS weekly accommodation & arrivals statistics (gov.ie), snapshot 2024-12-29.
-- The 31 LA values sum EXACTLY to the report's own Grand Total (32,702) — validated at
-- extraction. Population is CSO Census 2022 (CC-BY); `ip_per_1000_population` is the metric
-- the C&AG's own Figure 10.2 choropleth uses, which the C&AG published only as BANDS.
--
-- GRAIN: a point-in-time HEADCOUNT per LA, not a flow. value_safe_to_sum is FALSE — the
-- LA values do sum to the national total, but the column must never be summed with money
-- or unioned with any other fact. `cag_band` reproduces the C&AG's published banding so the
-- map can be read against the auditor's own.
--
-- UNKNOWNS ARE PRESERVED: where population could not be mapped, ip_per_1000_population is
-- NULL and population_unknown_reason says why. Never impute it.
--
-- `map_key` reconciles this fact's LA naming ("Carlow County Council", "Dun Laoghaire")
-- to the canonical SVG-outline keys ("Carlow", "Dun Laoghaire-Rathdown") the national
-- choropleth is drawn against (data/_meta/local_authority_outlines.json, the same keys
-- v_la_chief_executives uses). The crosswalk is an explicit 31-row VALUES list — no fuzzy
-- match, so every authority resolves to exactly one outline polygon or the map is caught
-- lying by test_ipas_la_map_key_covers_all_outlines. Kept inline (not a JOIN to the
-- constituency-domain view) so this housing view carries no cross-glob registration order.
CREATE OR REPLACE VIEW v_ipas_la_profile AS
WITH map_crosswalk(la_name, map_key) AS (
    VALUES
        ('Carlow County Council', 'Carlow'),
        ('Cavan County', 'Cavan'),
        ('Clare County', 'Clare'),
        ('Cork City', 'Cork City'),
        ('Cork County', 'Cork County'),
        ('Donegal County', 'Donegal'),
        ('Dublin City', 'Dublin City'),
        ('Dun Laoghaire', 'Dun Laoghaire-Rathdown'),
        ('Fingal County', 'Fingal'),
        ('Galway City Council', 'Galway City'),
        ('Galway County Council', 'Galway County'),
        ('Kerry County', 'Kerry'),
        ('Kildare County', 'Kildare'),
        ('Kilkenny County', 'Kilkenny'),
        ('Laois County', 'Laois'),
        ('Leitrim County', 'Leitrim'),
        ('Limerick City & County', 'Limerick'),
        ('Longford County', 'Longford'),
        ('Louth County', 'Louth'),
        ('Mayo County', 'Mayo'),
        ('Meath County', 'Meath'),
        ('Monaghan County', 'Monaghan'),
        ('Offaly County', 'Offaly'),
        ('Roscommon County', 'Roscommon'),
        ('Sligo County', 'Sligo'),
        ('South Dublin County', 'South Dublin'),
        ('Tipperary County', 'Tipperary'),
        ('Waterford City and County', 'Waterford'),
        ('Westmeath County', 'Westmeath'),
        ('Wexford County', 'Wexford'),
        ('Wicklow County', 'Wicklow')
)
SELECT
    p.local_authority,
    x.map_key,
    p.ip_applicants,
    p.population_2022,
    p.ip_per_1000_population,
    p.cag_band,
    p.snapshot_date,
    p.population_census_year,
    p.population_unknown_reason,
    p.source_url_ip_applicants,
    p.source_url_population,
    p.provenance_footer,
    p.value_safe_to_sum
FROM read_parquet('data/gold/parquet/ipas_la_profile.parquet') p
LEFT JOIN map_crosswalk x ON x.la_name = p.local_authority
ORDER BY p.ip_applicants DESC;
