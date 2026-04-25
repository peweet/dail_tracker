-- Grain: one row per calendar month
-- Source: aggregated_payment_tables (silver Parquet — auto-registered)
-- Used by: payments.py timeline chart — Streamlit plots this directly
--
-- Key concepts:
--   DATE_TRUNC for month bucketing  → https://duckdb.org/docs/sql/functions/date#date_truncdate-part-date
--   STRFTIME for display labels     → https://duckdb.org/docs/sql/functions/date#strftimeformat-timestamp
--
-- Run this to discover the date column name:
--   SELECT * FROM aggregated_payment_tables LIMIT 1

SELECT
    -- TODO: truncate the payment date column to month
    -- Hint: DATE_TRUNC('month', ???)
    DATE_TRUNC('month', ???)                                                          AS month,
    -- TODO: readable label for the chart x-axis e.g. 'Jan 2023'
    -- Hint: STRFTIME(DATE_TRUNC('month', ???), '%b %Y')
    STRFTIME(DATE_TRUNC('month', ???), '%b %Y')                                       AS month_label,
    SUM(???)                                                                          AS total_paid,
    COUNT(DISTINCT full_name)                                                         AS tds_paid
FROM   aggregated_payment_tables
WHERE  ??? IS NOT NULL
GROUP  BY month, month_label
ORDER  BY month
