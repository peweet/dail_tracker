-- Grain: one row per lobbyist
-- Signals: filing history, active span in days, count of distinct quarters filed
-- Source: returns (one row per lobby return)
-- Orgs that file every single period vs one-off appearances carry different weight.
SELECT
    lobbyist_name,
    MIN(lobbying_period_start_date)                                          AS first_return_date,
    MAX(lobbying_period_start_date)                                          AS last_return_date,
    COUNT(DISTINCT primary_key)                                              AS total_returns,
    COUNT(DISTINCT
        YEAR(lobbying_period_start_date)::VARCHAR
        || '-Q' ||
        QUARTER(lobbying_period_start_date)::VARCHAR
    )                                                                        AS distinct_periods_filed,
    DATEDIFF('day',
        MIN(lobbying_period_start_date),
        MAX(lobbying_period_start_date)
    )                                                                        AS active_span_days
FROM   returns
WHERE  lobbying_period_start_date IS NOT NULL
GROUP  BY lobbyist_name
ORDER  BY total_returns DESC, distinct_periods_filed DESC
