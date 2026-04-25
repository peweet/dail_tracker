-- Grain: one row per TD (lifetime totals)
-- Source: aggregated_td_tables (silver Parquet — auto-registered)
-- Used by: attendance.py rankings, top/bottom 20, overview stats
--
-- Key concepts:
--   SUM aggregation  → https://duckdb.org/docs/sql/aggregates#general-aggregate-functions
--   RANK window fn   → https://duckdb.org/docs/sql/window_functions (for percentile ranking)
--
-- Run this to discover column names:
--   SELECT * FROM aggregated_td_tables LIMIT 1

SELECT
    full_name,
    -- TODO: sum of sitting days across all years
    SUM(???)                                                                          AS total_sitting_days,
    -- TODO: sum of other days across all years
    SUM(???)                                                                          AS total_other_days,
    -- TODO: derive total attendance days
    SUM(???) + SUM(???)                                                               AS total_days,
    COUNT(DISTINCT ???)                                                               AS years_on_record,
    -- TODO: attendance rate as a percentage of total possible days
    -- Hint: total_sitting_days / NULLIF(total_days, 0) * 100.0
    ROUND(SUM(???) * 100.0 / NULLIF(SUM(???) + SUM(???), 0), 1)                     AS attendance_rate_pct
FROM   aggregated_td_tables
GROUP  BY full_name
ORDER  BY total_sitting_days DESC
