-- Grain: one row per lobbyist organisation
-- Source: politician_returns_detail (silver Parquet — auto-registered)
--   columns: full_name, chamber, position, primary_key, lobby_url,
--            lobbyist_name, public_policy_area, lobbying_period_start_date
-- (lobby_count_details is org-level only and does not carry full_name / public_policy_area)

SELECT
    lobbyist_name,
    COUNT(DISTINCT primary_key)        AS returns_filed,
    COUNT(DISTINCT full_name)          AS distinct_politicians_targeted,
    COUNT(DISTINCT public_policy_area) AS distinct_policy_areas
FROM   politician_returns_detail
WHERE  lobbyist_name IS NOT NULL
  AND  lobbyist_name <> ''
GROUP  BY lobbyist_name
ORDER  BY returns_filed DESC
