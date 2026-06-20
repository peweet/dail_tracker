-- v_housing_supply_national — national supply & affordability headline figures, the
-- counterpart to the demand-side waiting list on the Housing screen. Single row.
-- Each metric carries its own period (the sources have different vintages — stated
-- explicitly, never blended). DISPLAY presents them factually side by side; no
-- causal claim is made between vacancy and the waiting list.
--
-- Sources (gold, CSO PxStat — extractors/cso_pxstat_extract.py):
--   Vacancy  : cso_vac14 — vacant dwellings + ESB-connection stock, latest quarter,
--              summed across the 31 local authorities (no national row in source).
--   Rent     : cso_f2023b — Census average weekly rent, private-landlord tenancies,
--              the 'State' row, latest census (2022).
--   HAP      : cso_hap01 — households in the Housing Assistance Payment scheme,
--              All Family Types, latest year, summed across LAs.
CREATE OR REPLACE VIEW v_housing_supply_national AS
WITH vac AS (
    SELECT
        SUM(CAST(VALUE AS DOUBLE)) FILTER (WHERE "Statistic Label" = 'Number of Vacant Dwellings') AS vacant_dwellings,
        SUM(CAST(VALUE AS DOUBLE)) FILTER (WHERE "Statistic Label" = 'Dwelling Stock (ESB residential connections)') AS dwelling_stock,
        MAX(Quarter) AS vacancy_period
    FROM read_parquet('data/gold/parquet/cso_vac14.parquet')
    WHERE Quarter = (SELECT MAX(Quarter) FROM read_parquet('data/gold/parquet/cso_vac14.parquet'))
),
rent AS (
    SELECT CAST(VALUE AS DOUBLE) AS avg_weekly_private_rent, "Census Year" AS rent_period
    FROM read_parquet('data/gold/parquet/cso_f2023b.parquet')
    WHERE "Statistic Label" = 'Average weekly rent'
      AND "Nature of Occupancy" = 'Rented from private landlord'
      AND "County and City" = 'State'
      AND "Census Year" = (SELECT MAX("Census Year") FROM read_parquet('data/gold/parquet/cso_f2023b.parquet'))
),
hap AS (
    SELECT SUM(CAST(VALUE AS DOUBLE)) AS hap_households, MAX(Year) AS hap_period
    FROM read_parquet('data/gold/parquet/cso_hap01.parquet')
    WHERE "Statistic Label" = 'Number of Households in HAP'
      AND "Family Type" = 'All Family Types'
      AND Year = (SELECT MAX(Year) FROM read_parquet('data/gold/parquet/cso_hap01.parquet'))
)
SELECT
    v.vacant_dwellings,
    v.dwelling_stock,
    ROUND(100.0 * v.vacant_dwellings / NULLIF(v.dwelling_stock, 0), 1) AS vacancy_rate,
    v.vacancy_period,
    r.avg_weekly_private_rent,
    r.rent_period,
    h.hap_households,
    h.hap_period
FROM vac v CROSS JOIN rent r CROSS JOIN hap h;
