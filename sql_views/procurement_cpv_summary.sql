-- v_procurement_cpv_summary — per CPV category (what is being bought).
-- Source: data/gold/parquet/procurement_awards.parquet. CPV = EU Common
-- Procurement Vocabulary; description is the human label for the code.
-- n_awards is the trustworthy metric; value sums value_safe_to_sum only.
CREATE OR REPLACE VIEW v_procurement_cpv_summary AS
SELECT
    "Main Cpv Code"                                             AS cpv_code,
    mode("Main Cpv Code Description")                           AS cpv_description,
    COUNT(*)                                                    AS n_awards,
    COUNT(DISTINCT supplier_norm)                              AS n_suppliers,
    COALESCE(SUM(value_eur) FILTER (WHERE value_safe_to_sum), 0) AS awarded_value_safe_eur
FROM read_parquet('data/gold/parquet/procurement_awards.parquet')
WHERE "Main Cpv Code" IS NOT NULL
  AND "Main Cpv Code" <> ''
GROUP BY cpv_code
ORDER BY n_awards DESC;
