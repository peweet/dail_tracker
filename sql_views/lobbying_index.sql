-- v_lobbying_index — ranked list of most-lobbied politicians/senators
-- Source: data/gold/parquet/most_lobbied_politicians.parquet
-- Gold file has one row per (politician, role/chamber); deduplicate to one row
-- per politician keeping the primary role (highest lobby_returns_targeting).

CREATE OR REPLACE VIEW v_lobbying_index AS
WITH deduped AS (
    SELECT
        full_name,
        chamber,
        lobby_returns_targeting,
        distinct_orgs,
        total_returns,
        ROW_NUMBER() OVER (
            PARTITION BY full_name
            ORDER BY lobby_returns_targeting DESC
        ) AS rn
    FROM read_parquet('data/gold/parquet/most_lobbied_politicians.parquet')
)
SELECT
    full_name                   AS member_name,
    COALESCE(chamber, '')       AS chamber,
    ''                          AS position,
    total_returns               AS return_count,
    distinct_orgs,
    0                           AS distinct_policy_areas,
    NULL::VARCHAR               AS first_period,
    NULL::VARCHAR               AS last_period
FROM deduped
WHERE rn = 1
ORDER BY return_count DESC;
