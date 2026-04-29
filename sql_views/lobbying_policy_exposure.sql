-- v_lobbying_policy_exposure — politician-level policy area exposure
-- Source: data/gold/parquet/politician_policy_exposure.parquet
-- One row per (politician, policy area).
-- Use to show: which policy areas target a politician most (filter by full_name)
--              which politicians are most exposed to a given area (filter by public_policy_area)

CREATE OR REPLACE VIEW v_lobbying_policy_exposure AS
SELECT
    full_name           AS member_name,
    COALESCE(chamber, '') AS chamber,
    public_policy_area,
    returns_targeting,
    distinct_lobbyists
FROM read_parquet('data/gold/parquet/politician_policy_exposure.parquet')
WHERE public_policy_area IS NOT NULL
ORDER BY returns_targeting DESC;
