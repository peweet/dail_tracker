SELECT
    client_name,
    COUNT(DISTINCT primary_key)        AS return_count,
    COUNT(DISTINCT lobbyist_name)      AS distinct_lobbyist_firms,
    COUNT(DISTINCT full_name)          AS distinct_politicians_targeted,
    COUNT(DISTINCT public_policy_area) AS distinct_policy_areas,
    COUNT(DISTINCT chamber)            AS distinct_chambers
FROM   activities
WHERE  client_name IS NOT NULL
  AND  client_name <> ''
GROUP  BY client_name
ORDER  BY return_count DESC, distinct_politicians_targeted DESC
