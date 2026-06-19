-- v_ssha_waiting_list_totals — headline waiting-list figures per area, with
-- per-capita. Drives the national Housing screen's county league table and hero
-- numbers. One row per (grain, area); 2025 latest with 2024 for year-on-year.
--
--   grain            : 'national' | 'county' (26) | 'la' (31)
--   area             : 'Ireland' | county | local authority
--   waiting_total    : qualified households on the list (SSHA A1.8 total, 2025)
--   waiting_total_2024 / waiting_yoy_pct
--   over_4yr_pct / over_7yr_pct : share waiting >4 / >7 years
--   population       : CSO PEA08 'All ages', 'Both sexes', latest year (persons) —
--                      county/national ONLY (PEA08 is county-grain; LA has no honest
--                      denominator, so per-capita is left NULL at LA grain)
--   waiters_per_1000 : waiting_total / population * 1000
--
-- Per-capita forces the county rollup: SSHA is 31 LAs, PEA08 is 26 counties + Ireland,
-- so Dublin's 4 LAs / Cork's & Galway's 2 are summed to match. PEA08 'All ages' row is
-- used directly (summing the 18 age bands would double-count via that same total row).
CREATE OR REPLACE VIEW v_ssha_waiting_list_totals AS
WITH tol AS (
    SELECT la, year,
        CAST(total AS BIGINT) AS waiting_total,
        CAST("4_5_years" AS BIGINT) + CAST("5_7_years" AS BIGINT)
            + CAST("more_than_7_years" AS BIGINT) AS over_4yr,
        CAST("more_than_7_years" AS BIGINT) AS over_7yr
    FROM read_parquet('data/gold/parquet/ssha_a1_8_time_on_list_wide.parquet')
),
la_county(la, county) AS (
    VALUES
    ('Carlow County','Carlow'), ('Cavan County','Cavan'), ('Clare County','Clare'),
    ('Cork City','Cork'), ('Cork County','Cork'), ('Donegal County','Donegal'),
    ('Dublin City','Dublin'), ('Dun Laoghaire Rathdown County','Dublin'), ('Fingal County','Dublin'),
    ('South Dublin County','Dublin'), ('Galway City','Galway'), ('Galway County','Galway'),
    ('Kerry County','Kerry'), ('Kildare County','Kildare'), ('Kilkenny County','Kilkenny'),
    ('Laois County','Laois'), ('Leitrim County','Leitrim'), ('Limerick City and County','Limerick'),
    ('Longford County','Longford'), ('Louth County','Louth'), ('Mayo County','Mayo'),
    ('Meath County','Meath'), ('Monaghan County','Monaghan'), ('Offaly County','Offaly'),
    ('Roscommon County','Roscommon'), ('Sligo County','Sligo'), ('Tipperary County','Tipperary'),
    ('Waterford City and County','Waterford'), ('Westmeath County','Westmeath'),
    ('Wexford County','Wexford'), ('Wicklow County','Wicklow')
),
la_t AS (
    SELECT 'la' AS grain, la AS area, year, waiting_total, over_4yr, over_7yr FROM tol
),
county_t AS (
    SELECT 'county' AS grain, c.county AS area, t.year,
           SUM(t.waiting_total) AS waiting_total, SUM(t.over_4yr) AS over_4yr, SUM(t.over_7yr) AS over_7yr
    FROM tol t JOIN la_county c ON c.la = t.la
    GROUP BY c.county, t.year
),
nat_t AS (
    SELECT 'national' AS grain, 'Ireland' AS area, year,
           SUM(waiting_total) AS waiting_total, SUM(over_4yr) AS over_4yr, SUM(over_7yr) AS over_7yr
    FROM tol GROUP BY year
),
allt AS (
    SELECT * FROM la_t UNION ALL SELECT * FROM county_t UNION ALL SELECT * FROM nat_t
),
w AS (
    SELECT grain, area,
        MAX(waiting_total) FILTER (WHERE year = 2025) AS waiting_total,
        MAX(waiting_total) FILTER (WHERE year = 2024) AS waiting_total_2024,
        MAX(over_4yr)      FILTER (WHERE year = 2025) AS over_4yr_2025,
        MAX(over_7yr)      FILTER (WHERE year = 2025) AS over_7yr_2025
    FROM allt GROUP BY grain, area
),
pop AS (
    SELECT "County" AS pea_county, CAST(VALUE AS DOUBLE) * 1000 AS population
    FROM read_parquet('data/gold/parquet/cso_pea08.parquet')
    WHERE "Age Group" = 'All ages' AND "Sex" = 'Both sexes'
      AND "Year" = (SELECT MAX("Year") FROM read_parquet('data/gold/parquet/cso_pea08.parquet'))
)
SELECT
    w.grain, w.area, 2025 AS year,
    w.waiting_total,
    w.waiting_total_2024,
    CASE WHEN w.waiting_total_2024 > 0
         THEN ROUND(100.0 * (w.waiting_total - w.waiting_total_2024) / w.waiting_total_2024, 1) END AS waiting_yoy_pct,
    CASE WHEN w.waiting_total > 0 THEN ROUND(100.0 * w.over_4yr_2025 / w.waiting_total, 1) END AS over_4yr_pct,
    CASE WHEN w.waiting_total > 0 THEN ROUND(100.0 * w.over_7yr_2025 / w.waiting_total, 1) END AS over_7yr_pct,
    p.population,
    CASE WHEN p.population > 0 THEN ROUND(1000.0 * w.waiting_total / p.population, 1) END AS waiters_per_1000
FROM w
LEFT JOIN pop p
    ON p.pea_county = CASE WHEN w.grain = 'national' THEN 'Ireland'
                           WHEN w.grain = 'county'   THEN 'Co. ' || w.area END
ORDER BY w.grain, w.waiting_total DESC;
