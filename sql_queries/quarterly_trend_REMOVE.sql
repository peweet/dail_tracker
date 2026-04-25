-- Grain: one row per calendar quarter
-- Source: returns (one row per lobby return, already deduped and nil-filtered)
SELECT
    YEAR(lobbying_period_start_date)                                         AS year,
    QUARTER(lobbying_period_start_date)                                      AS quarter,
    YEAR(lobbying_period_start_date)::VARCHAR
        || '-Q' ||
    QUARTER(lobbying_period_start_date)::VARCHAR                             AS year_quarter,
    COUNT(*)                                                                 AS return_count,
    COUNT(DISTINCT lobbyist_name)                                            AS distinct_lobbyists
FROM   returns
WHERE  lobbying_period_start_date IS NOT NULL
GROUP  BY year, quarter, year_quarter
ORDER  BY year_quarter
