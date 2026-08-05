-- v_procurement_ted_supplier_summary — per-winner ranking of TED (EU-journal) award
-- notices, for the Procurement page's "EU-level awards (TED)" tab. One row per company-
-- class winner. The source view is notice x winner, so all notice counts are DISTINCT.
-- TED monetary values are never aggregated; individual notice values remain available
-- in the notice drill-down only. Individuals/sole
-- traders are quarantined (privacy), same gate as the eTenders supplier ranking.
CREATE OR REPLACE VIEW v_procurement_ted_supplier_summary AS
SELECT
    mode(winner_name)                                  AS winner_name,
    winner_join_norm,
    COUNT(DISTINCT publication_number)                 AS n_awards,
    COUNT(DISTINCT publication_number)
        FILTER (WHERE is_pan_eu_outlier)               AS n_pan_eu_awards,
    COUNT(DISTINCT buyer_name)                         AS n_buyers,
    BOOL_OR(is_pan_eu_outlier)                         AS has_pan_eu,
    mode(cro_company_num)                              AS cro_company_num,
    mode(cro_company_status)                           AS cro_company_status
FROM v_procurement_ted_winner_history
WHERE supplier_class = 'company'
  AND length(winner_join_norm) >= 4
GROUP BY winner_join_norm
ORDER BY n_awards DESC;
