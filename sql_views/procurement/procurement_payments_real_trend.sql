-- v_procurement_payments_real_trend — EXPERIMENTAL. Per (year × realisation_tier) public-spend
-- totals, nominal vs real (government-consumption deflator), for the real-terms TREND chart.
-- Rolls the vat-separated v_procurement_payments_real_by_year grain up to a year-level
-- INDICATIVE FLOOR (VAT bases combined) — the same "at least €X" basis the payments corpus
-- headline already uses, NOT an audited total. SPENT and COMMITTED stay separate (never one blend).
-- Gate consumption behind DAIL_EXPERIMENTAL.
--
-- real_uplift_pct is the pure inflation uplift over the ADJUSTABLE rows only (real vs nominal on
-- the same rows), so it is not distorted by years the deflator can't reach. total_real_eur is a
-- FLOOR for years with n_unadjustable_lines > 0 (the government-consumption deflator currently
-- ends 2024, so 2025+ spend has no real figure and stays nominal — surfaced, never hidden).
CREATE OR REPLACE VIEW v_procurement_payments_real_trend AS
SELECT
    year,
    realisation_tier,
    SUM(amount_eur)      FILTER (WHERE value_safe_to_sum)                                AS total_nominal_eur,
    SUM(amount_eur)      FILTER (WHERE value_safe_to_sum AND amount_eur_real IS NOT NULL) AS total_nominal_adjustable_eur,
    SUM(amount_eur_real) FILTER (WHERE value_safe_to_sum)                                AS total_real_eur,
    ROUND(
        100.0 * (
            SUM(amount_eur_real) FILTER (WHERE value_safe_to_sum)
            - SUM(amount_eur) FILTER (WHERE value_safe_to_sum AND amount_eur_real IS NOT NULL)
        ) / NULLIF(SUM(amount_eur) FILTER (WHERE value_safe_to_sum AND amount_eur_real IS NOT NULL), 0),
    1)                                                                                   AS real_uplift_pct,
    COUNT(*)             FILTER (WHERE value_safe_to_sum AND amount_eur_real IS NULL)     AS n_unadjustable_lines,
    any_value(real_base_year)                                                            AS real_base_year,
    'CSO_GOV_CONSUMPTION'                                                                AS deflator_index
FROM v_procurement_payments_real
WHERE year IS NOT NULL
GROUP BY year, realisation_tier
ORDER BY year, realisation_tier;
