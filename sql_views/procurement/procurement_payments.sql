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
    -- THE sum-safe money columns. Every aggregate over this feed must SUM one of
    -- these (never raw amount_eur): NULL unless the row is value_safe_to_sum AND
    -- in that tier, so SUM() implements the never-sum rule by construction. The
    -- same FILTER fragment used to be hand-copied at 5 call sites in
    -- queries/procurement.py — one drifted copy would misreport € publicly.
    CASE WHEN value_safe_to_sum AND realisation_tier = 'SPENT' THEN amount_eur END
                                              AS amount_spent_safe_eur,
    CASE WHEN value_safe_to_sum AND realisation_tier = 'COMMITTED' THEN amount_eur END
                                              AS amount_committed_safe_eur,
    description,
    po_number,
    -- Per-line PAYMENT STATUS where the body published one (some councils/depts carry a
    -- "Paid? Y/N", "Part Paid" or "Not Paid" column alongside the PO line). The raw paid_flag
    -- is a free-text column that suffered column-misalignment leakage in several source parsers
    -- (description text / dates bled in), so it is NEVER rendered raw — only a STRICT allowlist of
    -- recognised status tokens is canonicalised here, in the view's display layer; every other
    -- value (incl. leaked junk and ambiguous 'P'/'1') collapses to NULL = "no status published".
    -- Display-only context, not summed; absence of a status is the norm (most bodies don't publish one).
    CASE lower(trim(paid_flag))
        WHEN 'paid' THEN 'Paid' WHEN 'y' THEN 'Paid' WHEN 'yes' THEN 'Paid'
        WHEN 'part paid' THEN 'Part paid'
        WHEN 'not paid' THEN 'Not paid' WHEN 'n' THEN 'Not paid' WHEN 'no' THEN 'Not paid'
    END                                       AS paid_status,
    cro_company_num,
    cro_company_status,
    source_file_url
FROM read_parquet('data/gold/parquet/procurement_payments_fact.parquet')
WHERE supplier_normalised IS NOT NULL
  AND length(supplier_normalised) >= 2
  AND extraction_status = 'extracted';
