-- v_procurement_authority_summary — per contracting authority (the public bodies
-- doing the buying). Source: data/gold/parquet/procurement_awards.parquet.
-- n_awards is the trustworthy metric; awarded_value_safe_eur sums value_safe_to_sum
-- only (never framework ceilings).
CREATE OR REPLACE VIEW v_procurement_authority_summary AS
SELECT
    "Contracting Authority"                                      AS contracting_authority,
    COUNT(*)                                                     AS n_awards,
    COUNT(DISTINCT supplier_norm)                               AS n_suppliers,
    COALESCE(SUM(value_eur) FILTER (WHERE value_safe_to_sum), 0) AS awarded_value_safe_eur
FROM read_parquet('data/gold/parquet/procurement_awards.parquet')
WHERE "Contracting Authority" IS NOT NULL
  AND "Contracting Authority" <> ''
GROUP BY contracting_authority
ORDER BY n_awards DESC;
