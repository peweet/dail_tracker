-- v_procurement_afs_vs_po_coverage — the TRACEABILITY denominator for a local-authority dossier:
-- a council's audited revenue-account spend (AFS) set against the slice of that spend that is
-- traceable to NAMED suppliers through its published purchase-orders/payments register.
--
-- One row per (council, year). AFS side = Σ revenue gross expenditure (the audited operating
-- spend by service). PO side = the council's own published PO/payment lines that are sum-safe,
-- carried for BOTH lifecycle tiers separately (SPENT = paid, COMMITTED = ordered) because the
-- two are never added. The page picks the tier the council actually publishes (most LAs publish
-- purchase-orders-over-€20k → COMMITTED) and reads the matching pct_* column.
--
-- ⚠️ GRAINS DIFFER — this view sets two facts SIDE BY SIDE, it does NOT sum them:
--   * AFS gross = full audited operating spend (all of it, every euro, accrual actuals).
--   * PO total  = only purchases over the €20k publication threshold, to NAMED suppliers, and
--                 (for most councils) ORDERS not payments.
-- So pct_* is an INDICATIVE traceability ratio ("how much of accounts-spend can be tied to a
-- named >€20k supplier line"), NOT a reconciliation — different thresholds, different stages,
-- different grain. The dossier states this caveat. Join key = publisher_name = council (both
-- carry the plain council name). LEFT JOIN from AFS: a council with accounts but no published PO
-- register keeps its AFS figure with NULL PO (traceability simply unavailable that year).
CREATE OR REPLACE VIEW v_procurement_afs_vs_po_coverage AS
WITH afs AS (
    SELECT council, year,
           SUM(gross_expenditure) AS afs_gross_eur,
           SUM(net_expenditure)   AS afs_net_eur
    FROM read_parquet('data/silver/parquet/la_afs_divisions.parquet')
    GROUP BY council, year
),
po AS (
    SELECT publisher_name, year,
           SUM(amount_eur) FILTER (WHERE value_safe_to_sum AND realisation_tier = 'SPENT')     AS po_spent_safe_eur,
           SUM(amount_eur) FILTER (WHERE value_safe_to_sum AND realisation_tier = 'COMMITTED') AS po_committed_safe_eur,
           COUNT(*)        FILTER (WHERE realisation_tier = 'SPENT')                            AS n_spent_lines,
           COUNT(*)        FILTER (WHERE realisation_tier = 'COMMITTED')                        AS n_committed_lines,
           COUNT(DISTINCT supplier_normalised)                                                  AS n_named_suppliers
    FROM read_parquet('data/gold/parquet/procurement_payments_fact.parquet')
    WHERE extraction_status = 'extracted'
      AND supplier_normalised IS NOT NULL
      AND length(supplier_normalised) >= 2
      AND year IS NOT NULL
    GROUP BY publisher_name, year
)
SELECT
    afs.council,
    afs.year,
    afs.afs_gross_eur,
    afs.afs_net_eur,
    po.po_spent_safe_eur,
    po.po_committed_safe_eur,
    po.n_spent_lines,
    po.n_committed_lines,
    po.n_named_suppliers,
    ROUND(100.0 * po.po_spent_safe_eur     / NULLIF(afs.afs_gross_eur, 0), 1) AS pct_spent_of_gross,
    ROUND(100.0 * po.po_committed_safe_eur / NULLIF(afs.afs_gross_eur, 0), 1) AS pct_committed_of_gross
FROM afs
LEFT JOIN po ON po.publisher_name = afs.council AND po.year = afs.year;
