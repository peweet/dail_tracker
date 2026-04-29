-- v_lobbying_org_intensity — org-politician relationship intensity
-- Source: data/gold/parquet/bilateral_relationships.parquet
-- One row per (lobbyist_name, member_name) pair.
-- Use to show: which orgs lobby a politician most intensely (filter by member_name)
--              which politicians an org targets most intensely (filter by lobbyist_name)

CREATE OR REPLACE VIEW v_lobbying_org_intensity AS
SELECT
    lobbyist_name,
    full_name                               AS member_name,
    COALESCE(chamber, '')                   AS chamber,
    returns_in_relationship,
    distinct_policy_areas,
    distinct_periods,
    relationship_start::DATE                AS first_contact,
    relationship_last_seen::DATE            AS last_contact
FROM read_parquet('data/gold/parquet/bilateral_relationships.parquet')
ORDER BY returns_in_relationship DESC;
