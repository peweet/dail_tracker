-- v_procurement_authority_year_summary — per-(authority, year) version of
-- v_procurement_authority_summary, for the Procurement page's year-pill filter.
-- Same universe as the all-time view (all award classes — these are public bodies,
-- not private suppliers, so no class gate) and same value-safety; adds a `year`
-- dimension. Undated rows are dropped so a year filter is exact.
-- Buyer attribution (2026-08-09): client authority where named, mirroring the
-- all-time view — the two MUST agree or the year pill contradicts the ranking.
CREATE OR REPLACE VIEW v_procurement_authority_year_summary AS
SELECT
    -- Same dirty-value guard on the client side as the all-time view (the two MUST agree).
    COALESCE(NULLIF(NULLIF(TRIM("Name of Client Contracting Authority"), ''), 'NULL'), "Contracting Authority")
                                                                 AS contracting_authority,
    EXTRACT(year FROM TRY_STRPTIME("Notice Published Date/Contract Created Date", '%d/%m/%Y'))::INT AS year,
    COUNT(*)                                                     AS n_awards,
    COUNT(DISTINCT supplier_norm)                               AS n_suppliers,
    COALESCE(SUM(value_eur) FILTER (WHERE value_safe_to_sum), 0) AS awarded_value_safe_eur
FROM read_parquet('data/gold/parquet/procurement_awards.parquet')
WHERE "Contracting Authority" IS NOT NULL
  -- same belt-and-braces guard as v_procurement_authority_summary (gold is coerced
  -- upstream; this only matters if a literal-NULL string ever regresses)
  AND "Contracting Authority" NOT IN ('', 'NULL')
  AND TRY_STRPTIME("Notice Published Date/Contract Created Date", '%d/%m/%Y') IS NOT NULL
GROUP BY contracting_authority, year
ORDER BY n_awards DESC;
