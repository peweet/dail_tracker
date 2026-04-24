-- Replaces: experimental_compute_distinct_orgs_per_politician
-- Grain: one row per (politician, chamber, position)
-- Dedup on (primary_key, full_name, lobbyist_name) first so
-- activity-explosion does not inflate org counts.
-- A TD targeted 40x by 3 orgs is a different signal from 40x by 30 orgs.

WITH deduped AS (
    SELECT DISTINCT
        primary_key,
        full_name,
        chamber,
        position,
        lobbyist_name,
        public_policy_area
    FROM activities
)
SELECT
    full_name,
    chamber,
    position,
    COUNT(DISTINCT lobbyist_name)      AS distinct_orgs,
    COUNT(DISTINCT primary_key)        AS distinct_returns,
    COUNT(DISTINCT public_policy_area) AS distinct_policy_areas
FROM   deduped
GROUP  BY full_name, chamber, position
ORDER  BY distinct_orgs DESC, distinct_returns DESC
