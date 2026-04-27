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
    Full_Name AS full_name,
    TAA_Band AS allowance_type,
    Position AS position,
    SUM(Amount) AS total_paid,
    COUNT(*) AS payment_count,
    ROUND(AVG(Amount), 2) AS avg_payment
FROM aggregated_payment_tables
GROUP BY Full_Name, TAA_Band, Position
ORDER BY total_paid DESC
