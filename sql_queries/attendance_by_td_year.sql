-- Grain: one row per (full_name, year)
-- Source: aggregated_td_tables (silver Parquet — auto-registered)
-- Used by: attendance.py yearly breakdown and timeline chart
--
-- Key concepts:
--   Basic GROUP BY + SUM  → https://duckdb.org/docs/sql/query_syntax/groupby
--   Computed columns      → https://duckdb.org/docs/sql/expressions/overview
--
-- Run this to discover column names:
--   SELECT * FROM aggregated_td_tables LIMIT 1
--
-- Expected columns (verify): full_name, year, sitting_days_count, other_days_count

SELECT
    full_name,
    -- TODO: which column holds the year? verify from the table
    ???                                                                               AS year,
    -- TODO: sitting days column name
    ???                                                                               AS sitting_days,
    -- TODO: other days column name
    ???                                                                               AS other_days,
    -- TODO: derive a total_days column from the two above
    ???  +  ???                                                                       AS total_days
FROM   aggregated_td_tables
WHERE  -- TODO: filter out nulls on year if needed
    ??? IS NOT NULL
GROUP  BY full_name, year
ORDER  BY full_name, year
