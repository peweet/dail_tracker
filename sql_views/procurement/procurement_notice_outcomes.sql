-- v_procurement_notice_outcomes — the award-reporting gap, per (year × threshold band):
-- how many notices were published, how many ever got an award recorded, how many were
-- cancelled, and — within the awarded set — how many carry a value. Source:
-- data/gold/parquet/procurement_notices.parquet (2026-08-09).
--
-- WHY: award totals systematically read "too low". Two measured causes (2026-08 export):
-- ~half of notices never get an award recorded at all, and the National (below-EU) band
-- publishes an awarded VALUE on only ~27% of its award rows (vs ~89% OJEU) despite
-- Circular 05/2023 requiring award publication over €25k — and the split is temporal:
-- pre-2023 National award rows are almost value-dark (2019: 6% valued), 2024+ is 100%.
-- This view is the denominator that makes both gaps citable instead of a feeling.
-- Recency caveat: recent-year n_no_award_published includes competitions still in
-- train, not only reporting gaps — read old years as gaps, young years as pipeline.
-- COUNTS ONLY — no euro column is aggregated here (estimates are never summed).
CREATE OR REPLACE VIEW v_procurement_notice_outcomes AS
SELECT
    EXTRACT(year FROM published_date)::INT AS year,
    COALESCE(threshold_level, 'Unknown')   AS threshold_level,
    COUNT(*)                               AS n_notices,
    COUNT(*) FILTER (WHERE award_status = 'awarded')                     AS n_awarded,
    COUNT(*) FILTER (WHERE award_status = 'cancelled')                   AS n_cancelled,
    COUNT(*) FILTER (WHERE award_status = 'award_published_no_supplier') AS n_award_published_no_supplier,
    COUNT(*) FILTER (WHERE award_status = 'no_award_published')          AS n_no_award_published,
    ROUND(100.0 * COUNT(*) FILTER (WHERE award_status = 'awarded') / COUNT(*), 1) AS pct_awarded,
    COUNT(*) FILTER (WHERE award_status = 'awarded' AND awarded_value_present)    AS n_awarded_with_value,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE award_status = 'awarded' AND awarded_value_present)
        / NULLIF(COUNT(*) FILTER (WHERE award_status = 'awarded'), 0), 1
    ) AS pct_awarded_with_value
FROM read_parquet('data/gold/parquet/procurement_notices.parquet')
WHERE published_date IS NOT NULL
GROUP BY 1, 2
ORDER BY year DESC, n_notices DESC;
