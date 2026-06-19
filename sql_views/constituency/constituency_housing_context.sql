-- v_constituency_housing_context — for each constituency, the housing situation in
-- the local authority area(s) serving it: residential vacancy and median house price.
-- One row per (constituency, serving council). CONTEXT, council-area grain — NOT a
-- per-constituency figure and never apportioned (same framing as the spending cards).
--
-- Sources (already in gold, CSO PxStat — extractors/cso_pxstat_extract.py):
--   * Vacancy : cso_vac14.parquet — "Residential vacancy from metered electricity",
--     by Local Authority, latest 2024Q4 (Number of Vacant Dwellings + Vacancy Rate %).
--   * Price   : cso_hpm03.parquet — RPPI "Median Price" (€), by RPPI region (which is
--     LA-level), latest month, on the standard headline cut (all dwelling statuses, all
--     buyer types, stamp-duty Executions). Limerick/Waterford have no single combined
--     region in HPM03, so the County region is used as the area proxy; Tipperary has a
--     single region. A handful of RPPI aggregates (Midland, "All", …) are not LA-level
--     and are intentionally unmapped.
--
-- LA-name mapping is EXPLICIT (a verified 31-row table): the CSO vocabularies are
-- irregular ("Cork City Council" vs "Cork County Council" vs bare "Carlow", the fada in
-- "Dún Laoghaire", "Limerick City & County Council"), so a string-strip would silently
-- mis-join. local_authority matches v_constituency_la_crosswalk exactly.
CREATE OR REPLACE VIEW v_constituency_housing_context AS
WITH la_map(local_authority, vac14_la, hpm03_region) AS (
    VALUES
    ('Carlow', 'Carlow County Council', 'Carlow'),
    ('Cavan', 'Cavan County Council', 'Cavan'),
    ('Clare', 'Clare County Council', 'Clare'),
    ('Cork City', 'Cork City Council', 'Cork City'),
    ('Cork County', 'Cork County Council', 'Cork County'),
    ('Donegal', 'Donegal County Council', 'Donegal'),
    ('Dublin City', 'Dublin City Council', 'Dublin City'),
    ('Dun Laoghaire-Rathdown', 'Dún Laoghaire Rathdown County Council', 'Dún Laoghaire-Rathdown'),
    ('Fingal', 'Fingal County Council', 'Fingal'),
    ('Galway City', 'Galway City Council', 'Galway City'),
    ('Galway County', 'Galway County Council', 'Galway County'),
    ('Kerry', 'Kerry County Council', 'Kerry'),
    ('Kildare', 'Kildare County Council', 'Kildare'),
    ('Kilkenny', 'Kilkenny County Council', 'Kilkenny'),
    ('Laois', 'Laois County Council', 'Laois'),
    ('Leitrim', 'Leitrim County Council', 'Leitrim'),
    ('Limerick', 'Limerick City & County Council', 'Limerick County'),
    ('Longford', 'Longford County Council', 'Longford'),
    ('Louth', 'Louth County Council', 'Louth'),
    ('Mayo', 'Mayo County Council', 'Mayo'),
    ('Meath', 'Meath County Council', 'Meath'),
    ('Monaghan', 'Monaghan County Council', 'Monaghan'),
    ('Offaly', 'Offaly County Council', 'Offaly'),
    ('Roscommon', 'Roscommon County Council', 'Roscommon'),
    ('Sligo', 'Sligo County Council', 'Sligo'),
    ('South Dublin', 'South Dublin County Council', 'South Dublin'),
    ('Tipperary', 'Tipperary County Council', 'Tipperary'),
    ('Waterford', 'Waterford City & County Council', 'Waterford County'),
    ('Westmeath', 'Westmeath County Council', 'Westmeath'),
    ('Wexford', 'Wexford County Council', 'Wexford'),
    ('Wicklow', 'Wicklow County Council', 'Wicklow')
),
vac AS (
    SELECT
        "Local Authority" AS vac14_la,
        MAX(Quarter) AS vac_period,
        MAX(CAST(VALUE AS DOUBLE)) FILTER (WHERE "Statistic Label" = 'Number of Vacant Dwellings') AS vacant_dwellings,
        MAX(CAST(VALUE AS DOUBLE)) FILTER (WHERE "Statistic Label" = 'Vacancy Rate') AS vacancy_rate
    FROM read_parquet('data/gold/parquet/cso_vac14.parquet')
    WHERE Quarter = (SELECT MAX(Quarter) FROM read_parquet('data/gold/parquet/cso_vac14.parquet'))
    GROUP BY "Local Authority"
),
med_ranked AS (
    SELECT
        "RPPI Region" AS hpm03_region,
        Month AS med_period,
        CAST(VALUE AS DOUBLE) AS median_price_eur,
        ROW_NUMBER() OVER (PARTITION BY "RPPI Region" ORDER BY "TLIST(M1)" DESC) AS rn
    FROM read_parquet('data/gold/parquet/cso_hpm03.parquet')
    WHERE "Statistic Label" = 'Median Price'
      AND "Dwelling Status" = 'All Dwelling Statuses'
      AND "Type of Buyer" = 'All Buyer Types'
      AND "Stamp Duty Event" = 'Executions'
),
med AS (
    SELECT hpm03_region, med_period, median_price_eur FROM med_ranked WHERE rn = 1
)
SELECT
    x.constituency_name,
    x.local_authority,
    x.link_type,
    v.vacant_dwellings,
    v.vacancy_rate,
    v.vac_period,
    m.median_price_eur,
    m.med_period
FROM v_constituency_la_crosswalk x
JOIN la_map  lm ON lm.local_authority = x.local_authority
LEFT JOIN vac v  ON v.vac14_la = lm.vac14_la
LEFT JOIN med m  ON m.hpm03_region = lm.hpm03_region
ORDER BY x.constituency_name, (x.link_type = 'primary') DESC, x.local_authority;
