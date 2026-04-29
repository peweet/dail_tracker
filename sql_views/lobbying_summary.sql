-- v_lobbying_summary — single hero-banner row with dataset-level counts
-- Derived from gold parquet files — no enrichment run required.

CREATE OR REPLACE VIEW v_lobbying_summary AS
SELECT
    (SELECT SUM(return_count)          FROM read_parquet('data/gold/parquet/policy_area_breakdown.parquet'))         AS total_returns,
    (SELECT COUNT(DISTINCT lobbyist_name) FROM read_parquet('data/gold/parquet/top_lobbyist_organisations.parquet')) AS total_orgs,
    (SELECT COUNT(DISTINCT full_name)  FROM read_parquet('data/gold/parquet/most_lobbied_politicians.parquet'))      AS total_politicians,
    (SELECT COUNT(*)                   FROM read_parquet('data/gold/parquet/policy_area_breakdown.parquet'))         AS total_policy_areas,
    NULL::VARCHAR                       AS first_period,
    NULL::VARCHAR                       AS last_period,
    'data/gold/parquet (lobbying.ie)'   AS source_summary,
    current_timestamp::VARCHAR          AS latest_fetch_timestamp_utc;
