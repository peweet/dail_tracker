-- v_procurement_cpv_year_summary — per-(CPV category, year) version of
-- v_procurement_cpv_summary, for the Procurement page's year-pill filter.
-- Same universe and value-safety as the all-time view; adds a `year` dimension.
-- Undated rows are dropped so a year filter is exact.
CREATE OR REPLACE VIEW v_procurement_cpv_year_summary AS
SELECT
    "Main Cpv Code"                                             AS cpv_code,
    EXTRACT(year FROM TRY_STRPTIME("Notice Published Date/Contract Created Date", '%d/%m/%Y'))::INT AS year,
    mode("Main Cpv Code Description")                           AS cpv_description,
    COUNT(*)                                                    AS n_awards,
    COUNT(DISTINCT supplier_norm)                              AS n_suppliers,
    COALESCE(SUM(value_eur) FILTER (WHERE value_safe_to_sum), 0) AS awarded_value_safe_eur
FROM read_parquet('data/gold/parquet/procurement_awards.parquet')
WHERE "Main Cpv Code" IS NOT NULL
  AND "Main Cpv Code" <> ''
  AND TRY_STRPTIME("Notice Published Date/Contract Created Date", '%d/%m/%Y') IS NOT NULL
GROUP BY cpv_code, year
ORDER BY n_awards DESC;
