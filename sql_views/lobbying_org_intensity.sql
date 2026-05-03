-- v_lobbying_org_intensity — org-politician relationship intensity
-- Source: data/gold/parquet/bilateral_relationships.parquet
-- One row per (lobbyist_name, member_name) pair.
-- Use to show: which orgs lobby a politician most intensely (filter by member_name)
--              which politicians an org targets most intensely (filter by lobbyist_name)
--
-- unique_member_code is brought in via LEFT JOIN against most_lobbied_politicians.parquet
-- (the only source carrying the ID today). Deduped to one (full_name, code) pair so the
-- join cannot multiply intensity rows. Missing IDs surface as '' so the UI degrades to
-- a plain (unlinked) name.

CREATE OR REPLACE VIEW v_lobbying_org_intensity AS
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
    src.lobbyist_name,
    src.full_name                               AS member_name,
    COALESCE(mc.unique_member_code, '')         AS unique_member_code,
    COALESCE(src.chamber, '')                   AS chamber,
    src.returns_in_relationship,
    src.distinct_policy_areas,
    src.distinct_periods,
    src.relationship_start::DATE                AS first_contact,
    src.relationship_last_seen::DATE            AS last_contact
FROM read_parquet('data/gold/parquet/bilateral_relationships.parquet') src
LEFT JOIN member_codes mc ON src.full_name = mc.full_name
ORDER BY src.returns_in_relationship DESC;
