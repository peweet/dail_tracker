-- v_lobbying_summary — single hero-banner row with dataset-level counts
-- Derived from gold parquet files — no enrichment run required.
--
-- first_period / last_period: MIN/MAX of relationship_start / relationship_last_seen
-- on bilateral_relationships.parquet (the only gold file carrying period dates).
-- Formatted as 'YYYY-MM' so the hero badge reads "Data: 2014-12 → 2025-09" — a
-- coarse quarterly range matching how lobbying returns are filed.
-- (Audit fix 2026-05-26: previously NULL → hero badge rendered "Data: None → None".)

CREATE OR REPLACE VIEW v_lobbying_summary AS
SELECT
    (SELECT SUM(return_count)          FROM read_parquet('data/gold/parquet/policy_area_breakdown.parquet'))         AS total_returns,
    (SELECT COUNT(DISTINCT lobbyist_name) FROM read_parquet('data/gold/parquet/top_lobbyist_organisations.parquet')) AS total_orgs,
    (SELECT COUNT(DISTINCT full_name)  FROM read_parquet('data/gold/parquet/most_lobbied_politicians.parquet'))      AS total_politicians,
    (SELECT COUNT(*)                   FROM read_parquet('data/gold/parquet/policy_area_breakdown.parquet'))         AS total_policy_areas,
    (SELECT strftime(MIN(CAST(relationship_start     AS DATE)), '%Y-%m')
        FROM read_parquet('data/gold/parquet/bilateral_relationships.parquet')
        WHERE relationship_start IS NOT NULL)                                                                        AS first_period,
    (SELECT strftime(MAX(CAST(relationship_last_seen AS DATE)), '%Y-%m')
        FROM read_parquet('data/gold/parquet/bilateral_relationships.parquet')
        WHERE relationship_last_seen IS NOT NULL)                                                                    AS last_period,
    'data/gold/parquet (lobbying.ie)'   AS source_summary,
    current_timestamp::VARCHAR          AS latest_fetch_timestamp_utc;
