-- v_lobbying_policy_exposure — politician-level policy area exposure
-- Source: data/gold/parquet/politician_policy_exposure.parquet
-- One row per (politician, policy area).
-- Use to show: which policy areas target a politician most (filter by full_name)
--              which politicians are most exposed to a given area (filter by public_policy_area)
--
-- unique_member_code is brought in via LEFT JOIN against most_lobbied_politicians.parquet
-- (the only source carrying the ID today). Deduped to one (full_name, code) pair.

CREATE OR REPLACE VIEW v_lobbying_policy_exposure AS
WITH member_codes AS (
    SELECT full_name, unique_member_code
    FROM (
        SELECT
            full_name,
            unique_member_code,
            ROW_NUMBER() OVER (PARTITION BY full_name ORDER BY unique_member_code DESC) AS rn
        FROM read_parquet('data/gold/parquet/most_lobbied_politicians.parquet')
        WHERE unique_member_code IS NOT NULL AND unique_member_code <> ''
    )
    WHERE rn = 1
)
SELECT
    src.full_name                               AS member_name,
    COALESCE(mc.unique_member_code, '')         AS unique_member_code,
    COALESCE(src.chamber, '')                   AS chamber,
    src.public_policy_area,
    src.returns_targeting,
    src.distinct_lobbyists
FROM read_parquet('data/gold/parquet/politician_policy_exposure.parquet') src
LEFT JOIN member_codes mc ON src.full_name = mc.full_name
WHERE src.public_policy_area IS NOT NULL
ORDER BY src.returns_targeting DESC;
