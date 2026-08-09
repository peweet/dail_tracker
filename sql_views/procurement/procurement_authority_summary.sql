-- v_procurement_authority_summary — per buying authority (the public bodies doing
-- the buying). Source: data/gold/parquet/procurement_awards.parquet.
-- n_awards is the trustworthy metric; awarded_value_safe_eur sums value_safe_to_sum
-- only (never framework ceilings).
--
-- ATTRIBUTION CHANGE (2026-08-09): where the source names a CLIENT contracting
-- authority (a central body such as OGP/LGOPC ran the competition on another body's
-- behalf — ~5.2k award rows on the 2026-08 export), the award is attributed to the
-- client, the body actually buying. The output column keeps the name
-- `contracting_authority` because it is the page/query contract
-- (dail_tracker_core/queries/procurement/awards.py::authority_summary); rankings
-- move relative to pre-2026-08 outputs. Per-notice provenance (both raw columns)
-- stays visible in v_procurement_awards.
CREATE OR REPLACE VIEW v_procurement_authority_summary AS
SELECT
    -- Client side gets the same dirty-value guard as the WHERE below: a literal 'NULL'/''
    -- client must fall back to the contracting authority, never become an authority name.
    COALESCE(NULLIF(NULLIF(TRIM("Name of Client Contracting Authority"), ''), 'NULL'), "Contracting Authority")
                                                                 AS contracting_authority,
    COUNT(*)                                                     AS n_awards,
    COUNT(DISTINCT supplier_norm)                               AS n_suppliers,
    COALESCE(SUM(value_eur) FILTER (WHERE value_safe_to_sum), 0) AS awarded_value_safe_eur
FROM read_parquet('data/gold/parquet/procurement_awards.parquet')
-- Exclude the literal 'NULL' string the eTenders source writes for missing values
-- (root-fixed in the extractor 2026-06-11; this guard covers data cut before that run).
WHERE "Contracting Authority" IS NOT NULL
  AND "Contracting Authority" NOT IN ('', 'NULL')
GROUP BY 1
ORDER BY n_awards DESC;
