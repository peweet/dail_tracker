SELECT
    public_policy_area,
    COUNT(*)                      AS return_count,
    COUNT(DISTINCT lobbyist_name) AS distinct_lobbyists
FROM   returns
GROUP  BY public_policy_area
ORDER  BY return_count DESC
