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
    COALESCE(SUM(value_eur) FILTER (WHERE value_safe_to_sum), 0) AS awarded_value_safe_eur,
    -- Factual award-value benchmark (mirrors v_procurement_cpv_summary). Per-year
    -- n_awards_valued is small for many categories, so the UI must guard on it
    -- before showing a "typical value".
    COUNT(*) FILTER (WHERE value_safe_to_sum AND value_eur > 0) AS n_awards_valued,
    median(value_eur) FILTER (WHERE value_safe_to_sum AND value_eur > 0)              AS median_award_eur,
    quantile_cont(value_eur, 0.25) FILTER (WHERE value_safe_to_sum AND value_eur > 0) AS p25_award_eur,
    quantile_cont(value_eur, 0.75) FILTER (WHERE value_safe_to_sum AND value_eur > 0) AS p75_award_eur
FROM read_parquet('data/gold/parquet/procurement_awards.parquet')
-- Same 'NULL'-string data-quality exclusion as the all-time view.
WHERE "Main Cpv Code" IS NOT NULL
  AND "Main Cpv Code" NOT IN ('', 'NULL')
  AND TRY_STRPTIME("Notice Published Date/Contract Created Date", '%d/%m/%Y') IS NOT NULL
GROUP BY cpv_code, year
ORDER BY n_awards DESC;
