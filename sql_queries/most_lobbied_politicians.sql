-- Replaces: experimental_compute_most_lobbied_politicians
-- Grain: one row per (politician, chamber)
-- Dedup on (primary_key, full_name) first so multiple activities within
-- the same return count as one contact, not N.

WITH deduped AS (
    SELECT DISTINCT
        primary_key,
        full_name,
        chamber,
        lobbyist_name
    FROM activities
),
segmented AS (
    SELECT
        full_name,
        chamber,
        COUNT(DISTINCT primary_key)   AS lobby_returns_targeting,
        COUNT(DISTINCT lobbyist_name) AS distinct_orgs
    FROM   deduped
    GROUP BY full_name, chamber
),
totals AS (
    SELECT
        full_name,
        COUNT(DISTINCT primary_key) AS total_returns
    FROM   deduped
    GROUP BY full_name
)
SELECT
    s.full_name,
    s.chamber,
    s.lobby_returns_targeting,
    s.distinct_orgs,
    t.total_returns
FROM   segmented s
JOIN   totals    t USING (full_name)
ORDER  BY t.total_returns DESC
