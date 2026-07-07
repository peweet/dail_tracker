-- v_procurement_cpv_buyer_real — EXPERIMENTAL. Per (buyer × CPV) inflation-adjusted award
-- band. This is the surface where deflation earns its keep: a buyer whose awards skew to older
-- years, compared against a category whose median skews recent, is mis-compared in nominal
-- terms — deflating both to 2025 prices de-biases the comparison. Sum-safe contract awards
-- ONLY. Reads v_procurement_awards_real. Gate consumption behind DAIL_EXPERIMENTAL.
--
-- ⚠️ A CPV is a category, not a unit of work: a buyer-vs-category gap is driven mostly by
-- contract SIZE/scope, with inflation timing the smaller component. Do NOT present the gap as
-- an inflation effect. Real-terms = general CPI, not a current cost or a bid price.
-- Design: doc/PROCUREMENT_INFLATION_BENCHMARKING_DESIGN.md.
CREATE OR REPLACE VIEW v_procurement_cpv_buyer_real AS
SELECT
    contracting_authority,
    cpv_code,
    COUNT(*)                            FILTER (WHERE value_safe_to_sum AND value_eur_real > 0) AS n_awards_real,
    median(value_eur_real)              FILTER (WHERE value_safe_to_sum AND value_eur_real > 0) AS buyer_median_real_eur,
    quantile_cont(value_eur_real, 0.25) FILTER (WHERE value_safe_to_sum AND value_eur_real > 0) AS buyer_p25_real_eur,
    quantile_cont(value_eur_real, 0.75) FILTER (WHERE value_safe_to_sum AND value_eur_real > 0) AS buyer_p75_real_eur,
    any_value(real_base_year)                                                                   AS real_base_year,
    any_value(deflator_index)                                                                   AS deflator_index
FROM v_procurement_awards_real
WHERE cpv_code IS NOT NULL AND cpv_code NOT IN ('', 'NULL')
  AND contracting_authority IS NOT NULL
GROUP BY contracting_authority, cpv_code;
