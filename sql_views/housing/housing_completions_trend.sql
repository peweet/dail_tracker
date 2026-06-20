-- v_housing_completions_trend — new dwelling completions per year, nationally.
-- The supply trend behind the Housing screen's headline ("are we building enough?").
-- One row per complete calendar year.
--
-- Source: cso_ndq09 (CSO New Dwelling Completions, quarterly by Local Electoral Area).
-- Uses the source's own 'State' total row (not a sum of LEAs) and keeps only years
-- with all four quarters present, so a part-reported latest year never shows as a
-- false "drop".
CREATE OR REPLACE VIEW v_housing_completions_trend AS
SELECT
    CAST(substr(Quarter, 1, 4) AS INTEGER) AS year,
    SUM(CAST(VALUE AS DOUBLE)) AS completions
FROM read_parquet('data/gold/parquet/cso_ndq09.parquet')
WHERE "Statistic Label" = 'New Dwelling Completions'
  AND "Local Electoral Area" = 'State'
GROUP BY 1
HAVING COUNT(*) = 4
ORDER BY year;
