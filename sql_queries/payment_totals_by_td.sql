-- Grain: one row per (full_name, taa_label, position)
-- Source: aggregated_payment_tables (silver Parquet — auto-registered)
-- Used by: payments.py TD summary table — Streamlit filters by name only
--
-- Key concepts:
--   GROUP BY multiple columns  → https://duckdb.org/docs/sql/query_syntax/groupby
--   ROUND / CAST               → https://duckdb.org/docs/sql/functions/numeric
--
-- Run this to discover exact column names:
--   SELECT * FROM aggregated_payment_tables LIMIT 1
--
-- Expected columns (verify): full_name, taa_label, Position, Amount_num, Date_Paid

SELECT
    full_name,
    -- TODO: verify the allowance-type column name (taa_label? category?)
    ???                                                                               AS allowance_type,
    -- TODO: verify the position column name
    ???                                                                               AS position,
    SUM(???)                                                                          AS total_paid,
    COUNT(*)                                                                          AS payment_count,
    -- TODO: average payment per entry for this TD × allowance combination
    ROUND(AVG(???), 2)                                                                AS avg_payment
FROM   aggregated_payment_tables
GROUP  BY full_name, ???, ???
ORDER  BY total_paid DESC
