-- v_procurement_payments_publisher_summary — one row per (public body × lifecycle tier).
-- The primary "Money actually paid" ranking: which public bodies paid/ordered the most, and
-- to how many suppliers. Grouping BY realisation_tier keeps each row single-tier (SPENT vs
-- COMMITTED are never blended), and each publisher carries a single vat_status so the per-row
-- total is internally consistent (we never sum across publishers of differing vat_status).
-- Only value_safe_to_sum rows contribute to the euro total.
CREATE OR REPLACE VIEW v_procurement_payments_publisher_summary AS
SELECT
    publisher_name,
    publisher_type,
    sector,
    realisation_tier,
    mode(vat_status)                                            AS vat_status,
    COUNT(*)                                                    AS n_payments,
    COUNT(DISTINCT supplier_normalised)                         AS n_suppliers,
    MIN(year)::INT                                              AS min_year,
    MAX(year)::INT                                              AS max_year,
    COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum), 0) AS total_safe_eur
FROM v_procurement_payments
GROUP BY publisher_name, publisher_type, sector, realisation_tier
ORDER BY total_safe_eur DESC;
