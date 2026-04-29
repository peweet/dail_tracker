
SELECT
    lobbyist_name,
    COUNT(DISTINCT primary_key)            AS returns_filed,
    COUNT(DISTINCT full_name)              AS distinct_politicians_targeted,
    COUNT(DISTINCT public_policy_area)     AS distinct_policy_areas
FROM   lobby_count_details
WHERE  lobbyist_name IS NOT NULL
  AND  lobbyist_name <> ''
GROUP  BY lobbyist_name
ORDER  BY returns_filed DESC
