-- Grain: one row per (full_name, year)
-- Source: aggregated_td_tables (silver Parquet — auto-registered)
-- Used by: attendance.py yearly breakdown and timeline chart
--
-- Key concepts:
--   Basic GROUP BY + SUM  → https://duckdb.org/docs/sql/query_syntax/groupby
--   Computed columns      → https://duckdb.org/docs/sql/expressions/overview
--
-- Note: no full_name column in source — constructed from first_name + last_name.
--       sitting_total_days is pre-computed in the silver layer; kept here for
--       cross-check. The table is already one row per (identifier, year) so
--       GROUP BY is not needed, but is retained for safety in case of dupes.

SELECT
    CONCAT(first_name, ' ', last_name)                                                 AS full_name,
    year,
    SUM(sitting_days_count)                                                            AS sitting_days,
    SUM(other_days_count)                                                              AS other_days,
    SUM(sitting_days_count) + SUM(other_days_count)                                    AS total_days
FROM   aggregated_td_tables
WHERE  year IS NOT NULL
GROUP  BY full_name, year
ORDER  BY full_name, year
