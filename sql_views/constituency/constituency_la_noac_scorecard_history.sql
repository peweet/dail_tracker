-- v_la_noac_scorecard_history — the scorecard metrics across the available NOAC report
-- years (2022-2024), one row per council x year, for the trend sparklines on the dossier.
-- Headline values still come from v_la_noac_scorecard (latest year); this view ONLY feeds
-- the small spark trend beside each metric. Coverage is honest — a (metric, year) cell is
-- NULL where that year's report phrased the header too differently to locate (e.g. 2023
-- revenue balance); the sparkline simply skips missing points.
--
-- Source: data/gold/parquet/noac_scorecard_history.parquet, extracted by the layout-robust
-- header-driven extractors/noac_scorecard_history_extract.py. la mapped to the
-- local_authority key by the same explicit 31-row table as v_la_noac_scorecard.
CREATE OR REPLACE VIEW v_la_noac_scorecard_history AS
WITH la_map(local_authority, noac_la) AS (
    VALUES
    ('Carlow', 'Carlow County'), ('Cavan', 'Cavan County'), ('Clare', 'Clare County'),
    ('Cork City', 'Cork City'), ('Cork County', 'Cork County'), ('Donegal', 'Donegal County'),
    ('Dublin City', 'Dublin City'), ('Dun Laoghaire-Rathdown', 'Dun Laoghaire-Rathdown'),
    ('Fingal', 'Fingal County'), ('Galway City', 'Galway City'), ('Galway County', 'Galway County'),
    ('Kerry', 'Kerry County'), ('Kildare', 'Kildare County'), ('Kilkenny', 'Kilkenny County'),
    ('Laois', 'Laois County'), ('Leitrim', 'Leitrim County'), ('Limerick', 'Limerick City and County'),
    ('Longford', 'Longford County'), ('Louth', 'Louth County'), ('Mayo', 'Mayo County'),
    ('Meath', 'Meath County'), ('Monaghan', 'Monaghan County'), ('Offaly', 'Offaly County'),
    ('Roscommon', 'Roscommon County'), ('Sligo', 'Sligo County'), ('South Dublin', 'South Dublin County'),
    ('Tipperary', 'Tipperary County'), ('Waterford', 'Waterford City and County'),
    ('Westmeath', 'Westmeath County'), ('Wexford', 'Wexford County'), ('Wicklow', 'Wicklow County')
)
SELECT
    lm.local_authority,
    h.year,
    CAST(h.revenue_balance_pct        AS DOUBLE) AS revenue_balance_pct,
    CAST(h.m4_central_mgmt_charge_pct  AS DOUBLE) AS mgmt_overhead_pct,
    CAST(h.m3_claims_per_capita_eur    AS DOUBLE) AS insurance_claims_per_capita_eur,
    CAST(h.sickness_certified_pct      AS DOUBLE) AS sickness_absence_pct,
    CAST(h.roads_poor_pct              AS DOUBLE) AS roads_poor_pct,
    CAST(h.fire_within_10min_pct       AS DOUBLE) AS fire_within_10min_pct,
    CAST(h.litter_problem_pct          AS DOUBLE) AS litter_problem_pct
FROM read_parquet('data/gold/parquet/noac_scorecard_history.parquet') h
JOIN la_map lm ON lm.noac_la = h.la
ORDER BY lm.local_authority, h.year;
