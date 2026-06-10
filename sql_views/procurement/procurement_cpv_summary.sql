-- v_procurement_cpv_summary — per CPV category (what is being bought).
-- Source: data/gold/parquet/procurement_awards.parquet. CPV = EU Common
-- Procurement Vocabulary; description is the human label for the code.
-- n_awards is the trustworthy metric; value sums value_safe_to_sum only.
--
-- Factual award-value benchmark (added 2026-06-11, doc/PROCUREMENT_SURFACING_PLAN.md
-- recommendation #1): the typical AWARDED value for this category — median plus the
-- p25–p75 interquartile range — computed ONLY over sum-safe, positive-value awards
-- (n_awards_valued of them). This is a real-figure benchmark ("what do contracts in
-- this category cost?"), NOT spend and NOT inference; framework/DPS ceilings and
-- shared/zero values are excluded by the value_safe_to_sum gate. median/IQR resist
-- the long tail far better than a mean.
CREATE OR REPLACE VIEW v_procurement_cpv_summary AS
SELECT
    "Main Cpv Code"                                             AS cpv_code,
    mode("Main Cpv Code Description")                           AS cpv_description,
    COUNT(*)                                                    AS n_awards,
    COUNT(DISTINCT supplier_norm)                              AS n_suppliers,
    COALESCE(SUM(value_eur) FILTER (WHERE value_safe_to_sum), 0) AS awarded_value_safe_eur,
    COUNT(*) FILTER (WHERE value_safe_to_sum AND value_eur > 0) AS n_awards_valued,
    median(value_eur) FILTER (WHERE value_safe_to_sum AND value_eur > 0)            AS median_award_eur,
    quantile_cont(value_eur, 0.25) FILTER (WHERE value_safe_to_sum AND value_eur > 0) AS p25_award_eur,
    quantile_cont(value_eur, 0.75) FILTER (WHERE value_safe_to_sum AND value_eur > 0) AS p75_award_eur
FROM read_parquet('data/gold/parquet/procurement_awards.parquet')
-- The eTenders extractor writes the literal STRING 'NULL' (not a SQL null) for a
-- missing CPV on ~71% of awards, so plain IS NOT NULL / <> '' let it through and the
-- view used to surface a bogus "NULL" category. Exclude it here (display-correctness)
-- and fix upstream: extractors/<etenders>.py should emit a real null. The benchmark
-- therefore covers only awards that carry a real CPV — an honest coverage gap to show,
-- not hide.
WHERE "Main Cpv Code" IS NOT NULL
  AND "Main Cpv Code" NOT IN ('', 'NULL')
GROUP BY cpv_code
ORDER BY n_awards DESC;
