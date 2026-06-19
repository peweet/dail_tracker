-- v_constituency_council_housing_performance — for each constituency, how the
-- local authority(ies) serving it PERFORM on managing their social housing stock,
-- each metric shown beside the national median across all 31 LAs as a benchmark.
-- One row per (constituency, serving council). CONTEXT, council-area grain — NOT a
-- per-constituency figure, never apportioned. This is the council-OPERATIONS layer,
-- distinct from need (v_constituency_ssha_waiting_list) and supply
-- (v_constituency_housing_context).
--
-- Source (gold, NOAC Local Authority Performance Indicator Report 2024 —
-- pipeline_sandbox/housing/noac_housing_wide_extract_experimental.py --gold):
--   H2 noac_h2_vacancies_wide   — % of LA-owned dwellings vacant (31/12/2024)
--   H3 noac_h3_reletting_wide   — avg re-letting time (weeks) + cost (€)
--   H4 noac_h4_maintenance_wide — maintenance spend € per dwelling
--   H6 noac_h6_homeless_wide    — long-term homeless as % of homeless adults
--   H7 noac_h7_retrofit_wide    — houses retrofitted in 2024 (count)
--   H1 noac_h1_stock_wide       — LA-owned stock at 31/12/2024 (retrofit denominator)
-- Retrofit is normalised to % of stock (count favours large councils); the national
-- median is computed across the 31 LA values for every metric.
--
-- NO direction-of-good is encoded here (low vacancy is good, high retrofit is good —
-- it varies); the view presents value + median only, the page renders them factually.
--
-- LA-name mapping is EXPLICIT (verified 31-row table): NOAC uses "Carlow County",
-- "Limerick City and County", "Dun Laoghaire-Rathdown" etc.; the crosswalk uses
-- "Carlow", "Limerick", "Dun Laoghaire-Rathdown". A string-strip would mis-join
-- ("Limerick City and County"), so every LA is mapped by hand.
CREATE OR REPLACE VIEW v_constituency_council_housing_performance AS
WITH la_map(local_authority, noac_la) AS (
    VALUES
    ('Carlow', 'Carlow County'),
    ('Cavan', 'Cavan County'),
    ('Clare', 'Clare County'),
    ('Cork City', 'Cork City'),
    ('Cork County', 'Cork County'),
    ('Donegal', 'Donegal County'),
    ('Dublin City', 'Dublin City'),
    ('Dun Laoghaire-Rathdown', 'Dun Laoghaire-Rathdown'),
    ('Fingal', 'Fingal County'),
    ('Galway City', 'Galway City'),
    ('Galway County', 'Galway County'),
    ('Kerry', 'Kerry County'),
    ('Kildare', 'Kildare County'),
    ('Kilkenny', 'Kilkenny County'),
    ('Laois', 'Laois County'),
    ('Leitrim', 'Leitrim County'),
    ('Limerick', 'Limerick City and County'),
    ('Longford', 'Longford County'),
    ('Louth', 'Louth County'),
    ('Mayo', 'Mayo County'),
    ('Meath', 'Meath County'),
    ('Monaghan', 'Monaghan County'),
    ('Offaly', 'Offaly County'),
    ('Roscommon', 'Roscommon County'),
    ('Sligo', 'Sligo County'),
    ('South Dublin', 'South Dublin County'),
    ('Tipperary', 'Tipperary County'),
    ('Waterford', 'Waterford City and County'),
    ('Westmeath', 'Westmeath County'),
    ('Wexford', 'Wexford County'),
    ('Wicklow', 'Wicklow County')
),
h2 AS (
    SELECT la, CAST("percentage_la_owned_dwellings_vacant_on_pct" AS DOUBLE) AS vacancy_pct
    FROM read_parquet('data/gold/parquet/noac_h2_vacancies_wide.parquet')
),
h3 AS (
    SELECT la,
        CAST("average_re_letting_time_vacation_re_weeks" AS DOUBLE) AS reletting_weeks,
        CAST("average_cost_getting_re_tenanted_dwellings_eur" AS DOUBLE) AS reletting_cost_eur
    FROM read_parquet('data/gold/parquet/noac_h3_reletting_wide.parquet')
),
h4 AS (
    SELECT la, CAST("maintenance_expenditure_2024_per_dwelling_h1e_eur" AS DOUBLE) AS maintenance_eur_per_dwelling
    FROM read_parquet('data/gold/parquet/noac_h4_maintenance_wide.parquet')
),
h6 AS (
    SELECT la, CAST("long_term_homeless_adults_as_pct_pct" AS DOUBLE) AS longterm_homeless_pct
    FROM read_parquet('data/gold/parquet/noac_h6_homeless_wide.parquet')
),
h7 AS (
    SELECT la, CAST("houses_retrofitted_01_01_2024_31_count" AS DOUBLE) AS retrofit_count
    FROM read_parquet('data/gold/parquet/noac_h7_retrofit_wide.parquet')
),
h1 AS (
    SELECT la, CAST("dwellings_la_ownership_31_12_2024_count" AS DOUBLE) AS stock_end
    FROM read_parquet('data/gold/parquet/noac_h1_stock_wide.parquet')
),
m AS (
    SELECT
        lm.noac_la,
        h2.vacancy_pct,
        h3.reletting_weeks,
        h3.reletting_cost_eur,
        h4.maintenance_eur_per_dwelling,
        h6.longterm_homeless_pct,
        CASE WHEN h1.stock_end > 0
             THEN ROUND(100.0 * h7.retrofit_count / h1.stock_end, 2) END AS retrofit_pct_of_stock
    FROM la_map lm
    LEFT JOIN h2 ON h2.la = lm.noac_la
    LEFT JOIN h3 ON h3.la = lm.noac_la
    LEFT JOIN h4 ON h4.la = lm.noac_la
    LEFT JOIN h6 ON h6.la = lm.noac_la
    LEFT JOIN h7 ON h7.la = lm.noac_la
    LEFT JOIN h1 ON h1.la = lm.noac_la
),
nat AS (
    SELECT
        ROUND(MEDIAN(vacancy_pct), 2)                  AS nat_vacancy_pct,
        ROUND(MEDIAN(reletting_weeks), 1)              AS nat_reletting_weeks,
        ROUND(MEDIAN(reletting_cost_eur), 0)           AS nat_reletting_cost_eur,
        ROUND(MEDIAN(maintenance_eur_per_dwelling), 0) AS nat_maintenance_eur_per_dwelling,
        ROUND(MEDIAN(longterm_homeless_pct), 1)        AS nat_longterm_homeless_pct,
        ROUND(MEDIAN(retrofit_pct_of_stock), 2)        AS nat_retrofit_pct_of_stock
    FROM m
)
SELECT
    x.constituency_name,
    x.local_authority,
    x.link_type,
    m.vacancy_pct,
    m.reletting_weeks,
    m.reletting_cost_eur,
    m.maintenance_eur_per_dwelling,
    m.longterm_homeless_pct,
    m.retrofit_pct_of_stock,
    nat.nat_vacancy_pct,
    nat.nat_reletting_weeks,
    nat.nat_reletting_cost_eur,
    nat.nat_maintenance_eur_per_dwelling,
    nat.nat_longterm_homeless_pct,
    nat.nat_retrofit_pct_of_stock,
    2024 AS noac_period
FROM v_constituency_la_crosswalk x
JOIN la_map lm ON lm.local_authority = x.local_authority
LEFT JOIN m ON m.noac_la = lm.noac_la
CROSS JOIN nat
ORDER BY x.constituency_name, (x.link_type = 'primary') DESC, x.local_authority;
