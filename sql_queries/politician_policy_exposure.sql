WITH deduped AS (
    SELECT DISTINCT
        primary_key,
        full_name,
        chamber,
        public_policy_area,
        lobbyist_name
    FROM activities
)
SELECT
    full_name,
    chamber,
    public_policy_area,
    COUNT(DISTINCT primary_key)   AS returns_targeting,
    COUNT(DISTINCT lobbyist_name) AS distinct_lobbyists
FROM   deduped
GROUP  BY full_name, chamber, public_policy_area
ORDER  BY full_name ASC, returns_targeting DESC
