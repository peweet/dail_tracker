-- v_procurement_bid_signal_real — EXPERIMENTAL. The real-terms (2025-prices) award band per
-- 4-digit CPV trade — the inflation-adjusted companion to v_procurement_bid_signal's nominal
-- band. Deflated per-award first, then quantiled; sum-safe contract awards ONLY (ceilings are
-- excluded upstream by value_eur_real being NULL on framework rows). Reads
-- v_procurement_awards_real. Gate consumption behind DAIL_EXPERIMENTAL.
--
-- NOTE (validated by the sandbox probe): within a trade the band shift is modest (~0–14% on the
-- median) because award samples skew recent — deflation matters most for cross-year totals and
-- buyer-vs-category comparison, not the headline band. Real-terms = general CPI; it is NOT a
-- current cost or a recommended bid price. doc/PROCUREMENT_INFLATION_BENCHMARKING_DESIGN.md.
CREATE OR REPLACE VIEW v_procurement_bid_signal_real AS
SELECT
    trade_code,
    COUNT(*)                            FILTER (WHERE value_safe_to_sum AND value_eur_real > 0) AS n_contract_awards_real,
    quantile_cont(value_eur_real, 0.25) FILTER (WHERE value_safe_to_sum AND value_eur_real > 0) AS award_p25_real_eur,
    median(value_eur_real)              FILTER (WHERE value_safe_to_sum AND value_eur_real > 0) AS award_median_real_eur,
    quantile_cont(value_eur_real, 0.75) FILTER (WHERE value_safe_to_sum AND value_eur_real > 0) AS award_p75_real_eur,
    any_value(real_base_year)                                                                   AS real_base_year,
    any_value(deflator_index)                                                                   AS deflator_index
FROM v_procurement_awards_real
WHERE trade_code IS NOT NULL AND trade_code NOT IN ('', 'NULL')
GROUP BY trade_code;
