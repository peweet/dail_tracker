-- v_procurement_ted_supplier_summary — per-winner ranking of TED (EU-journal) award
-- notices, for the Procurement page's "EU-level awards (TED)" tab. One row per company-
-- class winner. n_awards (count) is the trustworthy lead metric, exactly as for eTenders.
--
-- Two value columns so the UI can honour the pan-EU toggle WITHOUT a second view:
--   ted_value_safe_eur          — sum-safe value EXCLUDING pan-EU outliers (the default)
--   ted_value_safe_incl_eu_eur  — sum-safe value INCLUDING pan-EU outliers (toggle on)
-- has_pan_eu flags winners whose total moves when the toggle flips. Individuals/sole
-- traders are quarantined (privacy), same gate as the eTenders supplier ranking.
CREATE OR REPLACE VIEW v_procurement_ted_supplier_summary AS
SELECT
    mode(winner_name)                                  AS winner_name,
    winner_join_norm,
    COUNT(*)                                           AS n_awards,
    COUNT(*) FILTER (WHERE is_pan_eu_outlier)          AS n_pan_eu_awards,
    COUNT(DISTINCT buyer_name)                         AS n_buyers,
    COALESCE(SUM(award_value_eur)
        FILTER (WHERE value_safe_to_sum AND NOT is_pan_eu_outlier), 0) AS ted_value_safe_eur,
    COALESCE(SUM(award_value_eur)
        FILTER (WHERE value_safe_to_sum), 0)           AS ted_value_safe_incl_eu_eur,
    BOOL_OR(is_pan_eu_outlier)                         AS has_pan_eu,
    mode(cro_company_num)                              AS cro_company_num,
    mode(cro_company_status)                           AS cro_company_status
FROM v_procurement_ted_awards
WHERE supplier_class = 'company'
  AND length(winner_join_norm) >= 4
GROUP BY winner_join_norm
ORDER BY n_awards DESC;
