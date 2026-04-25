-- Grain: one row per lobbying organisation (the entity filing the return)
-- Source: activities (one row per return × politician contact)
-- Replaces: experimental_compute_most_prolific_lobbyist_organisations in lobby_processing.py
-- Used by: legislation.py "Most prolific lobbying organisations" leaderboard
--
-- DISTINCT from top_client_companies.sql:
--   top_client_companies → groups by client_name (who hired the firm, the ultimate principal)
--   this file          → groups by lobbyist_name (the firm actually filing returns)
--
-- Key concepts:
--   COUNT DISTINCT for deduplication  → https://duckdb.org/docs/sql/aggregates#general-aggregate-functions
--   NULL / empty-string filter        → https://duckdb.org/docs/sql/expressions/comparison_operators

SELECT
    lobbyist_name,
    COUNT(DISTINCT primary_key)            AS returns_filed,
    COUNT(DISTINCT full_name)              AS distinct_politicians_targeted,
    COUNT(DISTINCT public_policy_area)     AS distinct_policy_areas
FROM   activities
WHERE  lobbyist_name IS NOT NULL
  AND  lobbyist_name <> ''
GROUP  BY lobbyist_name
ORDER  BY returns_filed DESC
