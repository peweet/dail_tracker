-- Replaces: experimental_compute_top_client_companies
-- Grain: one row per client company (the ultimate principal behind a lobbying firm)
-- Source: activities (one row per return x politician)
-- NULL and empty-string client_name rows excluded — those are own-account lobbyists.

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
