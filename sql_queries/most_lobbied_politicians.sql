-- unique_member_code joined from flattened_members so downstream SQL views
-- can read it directly (was previously patched in by enrich.py post-hoc).
-- Only Dáil/Seanad members match; councillors, civil servants, MEPs etc.
-- get '' so the UI can degrade to a non-clickable name.
WITH deduped AS (
    SELECT DISTINCT
        primary_key,
        full_name,
        chamber,
        lobbyist_name
    FROM activities
),
member_codes AS (
    SELECT norm_name, unique_member_code FROM (
        SELECT
            LOWER(strip_accents(TRIM(full_name))) AS norm_name,
            unique_member_code,
            ROW_NUMBER() OVER (
                PARTITION BY LOWER(strip_accents(TRIM(full_name)))
                ORDER BY unique_member_code DESC
            ) AS rn
        FROM flattened_members
        WHERE full_name IS NOT NULL AND unique_member_code IS NOT NULL
    )
    WHERE rn = 1
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
    COALESCE(mc.unique_member_code, '') AS unique_member_code,
    s.full_name,
    s.chamber,
    s.lobby_returns_targeting,
    s.distinct_orgs,
    t.total_returns
FROM   segmented s
JOIN   totals    t USING (full_name)
LEFT JOIN member_codes mc
    ON LOWER(strip_accents(TRIM(s.full_name))) = mc.norm_name
ORDER  BY t.total_returns DESC
