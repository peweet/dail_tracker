-- v_procurement_payments — the consolidated public-body PAYMENT feed (the SPENT/COMMITTED
-- tiers), display layer over data/gold/parquet/procurement_payments_fact.parquet
-- (extractors/procurement_payments_consolidate.py).
--
-- ⚠️ DIFFERENT GRAIN FROM AWARDS. These are payments/purchase-orders a public body actually
-- made — NEVER summed with eTenders/TED award ceilings. Two tiers live here and are never
-- summed together: realisation_tier='SPENT' (value_kind=payment_actual, "paid €X") and
-- 'COMMITTED' (po_committed, "ordered €X"). Only value_safe_to_sum rows sum, and NEVER across
-- differing vat_status (HSE/Tusla are VAT-inclusive; others unconfirmed → 'unknown').
--
-- PRIVACY: suppliers are named, incl. sole traders/individuals, because the source is the
-- public body's own published PO/payments-over-€20k list (Circular 07/2012 / FOI). Only the
-- published name+amount+description is shown; the fact carries no address/PII.
CREATE OR REPLACE VIEW v_procurement_payments AS
SELECT
    publisher_name,
    publisher_type,
    sector,
    period,
    year,
    supplier_raw                              AS supplier,
    supplier_normalised,
    supplier_class,
    amount_eur,
    value_kind,
    realisation_tier,
    vat_status,
    value_safe_to_sum,
    description,
    po_number,
    cro_company_num,
    cro_company_status,
    source_file_url
FROM read_parquet('data/gold/parquet/procurement_payments_fact.parquet')
WHERE supplier_normalised IS NOT NULL
  AND length(supplier_normalised) >= 2
  AND extraction_status = 'extracted';
