-- v_procurement_payments_supplier_summary — one row per (supplier × lifecycle tier): who the
-- public bodies actually PAID (SPENT) or ORDERED FROM (COMMITTED) the most. Suppliers are
-- named, incl. sole traders/individuals (published-source decision, see the feed view header).
--
-- ⚠️ total_safe_eur sums a supplier's payments ACROSS publishers, which may mix vat_status
-- (HSE incl-VAT vs others unknown). vat_mixed flags that so the UI can present the figure as
-- an indicative FLOOR ("at least €X paid"), never an audited total. Still one tier per row.
CREATE OR REPLACE VIEW v_procurement_payments_supplier_summary AS
SELECT
    mode(supplier)                                             AS supplier,
    supplier_normalised,
    realisation_tier,
    supplier_class,
    COUNT(*)                                                   AS n_payments,
    COUNT(DISTINCT publisher_name)                             AS n_publishers,
    MIN(year)::INT                                             AS min_year,
    MAX(year)::INT                                             AS max_year,
    COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum), 0) AS total_safe_eur,
    (COUNT(DISTINCT vat_status) > 1)                           AS vat_mixed,
    mode(cro_company_num)                                      AS cro_company_num,
    mode(cro_company_status)                                  AS cro_company_status
FROM v_procurement_payments
GROUP BY supplier_normalised, realisation_tier, supplier_class
ORDER BY total_safe_eur DESC;
