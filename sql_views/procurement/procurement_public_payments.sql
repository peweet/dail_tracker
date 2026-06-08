-- v_public_payments / _publisher_summary / _supplier_summary — the PUBLIC-FACING display layer
-- over data/gold/parquet/procurement_payments_fact.parquet (the consolidated public-body
-- payment/PO feed built by extractors/procurement_payments_consolidate.py: a 28-publisher base
-- plus HSE/Tusla, NTA, NPHDB, SEAI and the LA over-€20k fact, all conformed to one taxonomy).
-- Powers the "Public-Body Payments" page (utility/pages_code/public_payments.py) through
-- dail_tracker_core/queries/public_payments.py.
--
-- PRIVACY GATE — this is the public surface, so v_public_payments is filtered to
-- public_display = TRUE. The consolidate step sets public_display = FALSE on rows quarantined
-- as personal/individual; they never reach this view (the page reports them as "withheld").
-- This is what distinguishes it from v_procurement_payments
-- (sql_views/procurement/procurement_payments.sql), the analyst feed that omits the gate.
--
-- ⚠️ VALUE TAXONOMY. amount_semantics splits payment_actual ("paid €X") from po_committed
-- ("ordered €X"); only value_safe_to_sum rows contribute to total_safe_eur, and these payments
-- are NEVER summed with eTenders/TED award ceilings. The two summaries differ deliberately,
-- matching the page copy:
--   • publisher_summary GROUPs BY (publisher × amount_semantics) so each row is single-semantics
--     and the per-body figure is "never blended" (a labelled paid/ordered pill).
--   • supplier_summary GROUPs BY supplier only, intentionally blending paid+ordered across
--     bodies into one indicative sum-safe FLOOR (the page labels it neutrally, never "paid").
CREATE OR REPLACE VIEW v_public_payments AS
SELECT
    publisher_id,
    publisher_name,
    sector,
    supplier_raw            AS supplier,
    supplier_normalised,
    supplier_class,
    amount_eur,
    amount_semantics,
    value_safe_to_sum,
    description,
    year,
    quarter,
    period,
    source_file_url,
    extraction_confidence
FROM read_parquet('data/gold/parquet/procurement_payments_fact.parquet')
WHERE public_display = TRUE
  AND extraction_status = 'extracted'
  AND supplier_normalised IS NOT NULL
  AND length(supplier_normalised) >= 2;

CREATE OR REPLACE VIEW v_public_payments_publisher_summary AS
SELECT
    publisher_id,
    mode(publisher_name)                                         AS publisher_name,
    mode(sector)                                                 AS sector,
    amount_semantics,
    COUNT(*)                                                     AS n_lines,
    COUNT(DISTINCT supplier_normalised)                          AS n_suppliers,
    MIN(year)::INT                                               AS first_year,
    MAX(year)::INT                                               AS last_year,
    COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum), 0) AS total_safe_eur
FROM v_public_payments
GROUP BY publisher_id, amount_semantics
ORDER BY total_safe_eur DESC;

CREATE OR REPLACE VIEW v_public_payments_supplier_summary AS
SELECT
    mode(supplier)                                              AS supplier,
    supplier_normalised,
    mode(supplier_class)                                        AS supplier_class,
    COUNT(*)                                                    AS n_lines,
    COUNT(DISTINCT publisher_id)                                AS n_publishers,
    MIN(year)::INT                                              AS first_year,
    MAX(year)::INT                                              AS last_year,
    COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum), 0) AS total_safe_eur
FROM v_public_payments
GROUP BY supplier_normalised
ORDER BY total_safe_eur DESC;
