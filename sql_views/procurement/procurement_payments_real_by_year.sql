-- v_procurement_payments_real_by_year — annual PUBLIC-SPEND totals, nominal vs real (government-
-- consumption deflator, base 2024). This is the headline use case for the agency-standard index:
-- "what did public bodies actually spend, in real terms?". EXPERIMENTAL; gate behind DAIL_EXPERIMENTAL.
--
-- Grouped by year × realisation_tier × vat_status so totals are NEVER summed across the
-- SPENT/COMMITTED tiers (the 3-money-grain rule) or across differing VAT bases (HSE/Tusla are
-- VAT-inclusive; others not). value_safe_to_sum only. n_real_excluded = safe lines whose year is
-- outside the deflator (2025+) — they remain in the nominal total only, so total_real_eur is a
-- floor for those years. Real-terms is purchasing power, not a cost today.
CREATE OR REPLACE VIEW v_procurement_payments_real_by_year AS
SELECT
    year,
    realisation_tier,
    vat_status,
    COUNT(*)             FILTER (WHERE value_safe_to_sum)                              AS n_lines,
    SUM(amount_eur)      FILTER (WHERE value_safe_to_sum)                              AS total_nominal_eur,
    SUM(amount_eur_real) FILTER (WHERE value_safe_to_sum)                              AS total_real_eur,
    COUNT(*)             FILTER (WHERE value_safe_to_sum AND amount_eur_real IS NULL)  AS n_real_excluded,
    any_value(real_base_year)                                                          AS real_base_year,
    'CSO_GOV_CONSUMPTION'                                                              AS deflator_index
FROM v_procurement_payments_real
GROUP BY year, realisation_tier, vat_status
ORDER BY year, realisation_tier, vat_status;
