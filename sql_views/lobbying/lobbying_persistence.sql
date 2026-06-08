-- v_lobbying_persistence — lobbying organisation filing history
-- Source: data/gold/parquet/lobbyist_persistence.parquet
-- One row per lobbying organisation.
-- Use to show: how long an org has been active, total returns, distinct periods filed.

CREATE OR REPLACE VIEW v_lobbying_persistence AS
SELECT
    lobbyist_name,
    first_return_date::DATE     AS first_return_date,
    last_return_date::DATE      AS last_return_date,
    total_returns,
    distinct_periods_filed,
    active_span_days
FROM read_parquet('data/gold/parquet/lobbyist_persistence.parquet')
ORDER BY total_returns DESC;
