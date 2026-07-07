-- v_procurement_cpv_summary_real — EXPERIMENTAL. Per-CPV award benchmark with the nominal
-- band beside an inflation-adjusted (2025-prices) band. The real band is computed by deflating
-- each award FIRST, then taking quantiles — this removes the recency/year-mix bias from the
-- "typical award value" signal (mixing 2017 and 2024 awards bakes pure inflation into a naive
-- median). Sum-safe contract awards ONLY (value_safe_to_sum); ceilings excluded upstream.
-- Reads v_procurement_awards_real (alphabetical loader registers awards_real first). Gate
-- consumption behind DAIL_EXPERIMENTAL. Design: doc/PROCUREMENT_INFLATION_BENCHMARKING_DESIGN.md.
--
-- ⚠️ Real-terms = GENERAL CPI, not construction/materials/labour/tender-price inflation, and
-- it is NOT a current cost or a recommended bid price — it re-expresses past disclosed awards
-- in today's money. n_real_excluded is the honest count of sum-safe awards we could NOT adjust
-- (year outside the index / implausible) — they stay in the nominal band only.
CREATE OR REPLACE VIEW v_procurement_cpv_summary_real AS
SELECT
    cpv_code,
    mode(cpv_description)                                                                    AS cpv_description,
    -- nominal band (same definition as v_procurement_cpv_summary, for parity)
    COUNT(*)                            FILTER (WHERE value_safe_to_sum AND value_eur > 0)      AS n_awards_valued,
    median(value_eur)                   FILTER (WHERE value_safe_to_sum AND value_eur > 0)      AS median_award_eur,
    quantile_cont(value_eur, 0.25)      FILTER (WHERE value_safe_to_sum AND value_eur > 0)      AS p25_award_eur,
    quantile_cont(value_eur, 0.75)      FILTER (WHERE value_safe_to_sum AND value_eur > 0)      AS p75_award_eur,
    -- real-terms band (only rows that actually got adjusted: value_eur_real not null)
    COUNT(*)                            FILTER (WHERE value_safe_to_sum AND value_eur_real > 0) AS n_awards_valued_real,
    median(value_eur_real)              FILTER (WHERE value_safe_to_sum AND value_eur_real > 0) AS median_award_real_eur,
    quantile_cont(value_eur_real, 0.25) FILTER (WHERE value_safe_to_sum AND value_eur_real > 0) AS p25_award_real_eur,
    quantile_cont(value_eur_real, 0.75) FILTER (WHERE value_safe_to_sum AND value_eur_real > 0) AS p75_award_real_eur,
    min(value_eur_real)                 FILTER (WHERE value_safe_to_sum AND value_eur_real > 0) AS min_award_real_eur,
    max(value_eur_real)                 FILTER (WHERE value_safe_to_sum AND value_eur_real > 0) AS max_award_real_eur,
    -- honest "couldn't adjust" count (sum-safe awards whose year is outside the index / implausible)
    COUNT(*)                            FILTER (WHERE value_safe_to_sum AND value_eur > 0 AND value_eur_real IS NULL) AS n_real_excluded,
    any_value(real_base_year)                                                                   AS real_base_year,
    any_value(deflator_index)                                                                   AS deflator_index
FROM v_procurement_awards_real
WHERE cpv_code IS NOT NULL AND cpv_code NOT IN ('', 'NULL')
GROUP BY cpv_code;
