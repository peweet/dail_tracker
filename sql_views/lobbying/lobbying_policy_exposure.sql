-- v_lobbying_policy_exposure — politician-level policy area exposure
-- Source: data/gold/parquet/politician_policy_exposure.parquet
-- One row per (politician, policy area).
-- Use to show: which policy areas target a politician most (filter by full_name)
--              which politicians are most exposed to a given area (filter by public_policy_area)
--
-- unique_member_code is brought in via LEFT JOIN against the silver member registry
-- (flattened_members.parquet — the canonical Dáil/Seanad list). Names are normalised
-- with LOWER(strip_accents(TRIM())) on both sides. Non-Dáil/Seanad DPOs surface with
-- empty unique_member_code so the UI degrades to a non-clickable name.

CREATE OR REPLACE VIEW v_lobbying_policy_exposure AS
SELECT
    src.full_name                               AS member_name,
    COALESCE(mc.unique_member_code, '')         AS unique_member_code,
    COALESCE(src.chamber, '')                   AS chamber,
    src.public_policy_area,
    src.returns_targeting,
    src.distinct_lobbyists
FROM read_parquet('data/gold/parquet/politician_policy_exposure.parquet') src
LEFT JOIN v_lobbying_base_member_codes mc
    ON LOWER(strip_accents(TRIM(src.full_name))) = mc.norm_name
WHERE src.public_policy_area IS NOT NULL
ORDER BY src.returns_targeting DESC;
