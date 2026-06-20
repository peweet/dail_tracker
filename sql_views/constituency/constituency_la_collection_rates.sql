-- v_la_collection_rates — per-council revenue COLLECTION rates (NOAC M2, 2024):
-- the "is the council collecting what it's owed?" signal. Three rates — commercial
-- rates, rent & annuities, housing loans — each beside the national median across
-- the 31 councils. Collecting levied income is an EXECUTIVE function, so a low rate
-- is an administration (Chief Executive) signal, not an elected-member one.
--
-- Source: data/gold/parquet/noac_m2_collection_wide.parquet — FULL per-LA grid
-- (31 LAs x 2020-2024), extracted from NOAC's PDF tables via Camelot by
-- pipeline_sandbox/housing/noac_collection_wide_extract_experimental.py. (The same
-- gold table powers v_constituency_council_housing_performance's rent figure.)
--
-- Grain: one row per council (2024). la is mapped from NOAC's naming ("Carlow
-- County", "Limerick City and County") to the local_authority join key by the
-- explicit 31-row table (a string-strip would mis-map the City-and-County ones) —
-- the SAME map used by v_constituency_council_housing_performance. National medians
-- are window aggregates so the page can render value-vs-benchmark with no derivation.
-- ⚠️ housing-loan rate can exceed 100% (prior-year arrears collected alongside the year's due).
CREATE OR REPLACE VIEW v_la_collection_rates AS
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
),
m AS (
    SELECT
        CAST(commercial_rates_collection_pct AS DOUBLE) AS commercial_rates_pct,
        CAST(rent_annuities_collection_pct   AS DOUBLE) AS rent_annuities_pct,
        CAST(housing_loans_collection_pct    AS DOUBLE) AS housing_loans_pct,
        la
    FROM read_parquet('data/gold/parquet/noac_m2_collection_wide.parquet')
    WHERE year = 2024
)
SELECT
    lm.local_authority,
    2024                                                      AS year,
    m.commercial_rates_pct,
    m.rent_annuities_pct,
    m.housing_loans_pct,
    ROUND(MEDIAN(m.commercial_rates_pct) OVER (), 1)          AS nat_commercial_rates_pct,
    ROUND(MEDIAN(m.rent_annuities_pct)   OVER (), 1)          AS nat_rent_annuities_pct,
    ROUND(MEDIAN(m.housing_loans_pct)    OVER (), 1)          AS nat_housing_loans_pct
FROM la_map lm
JOIN m ON m.la = lm.noac_la
ORDER BY lm.local_authority;
