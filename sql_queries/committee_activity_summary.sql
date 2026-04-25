-- Grain: one row per TD
-- Source: committee_assignments (gold CSV — auto-registered)
-- Used by: committees.py TD overview table and leaderboard
--
-- Key concepts:
--   COUNT DISTINCT          → https://duckdb.org/docs/sql/aggregates#general-aggregate-functions
--   CASE WHEN in aggregates → https://duckdb.org/docs/sql/expressions/case
--   BOOL_OR / MAX for flags → https://duckdb.org/docs/sql/aggregates#general-aggregate-functions
--
-- Run this to see available columns:
--   SELECT * FROM committee_assignments LIMIT 1
--
-- Expected columns (verify): name, committee, status, is_chair, party

SELECT
    -- TODO: which column holds the TD name? (name? full_name?)
    ???                                                                               AS full_name,
    COUNT(DISTINCT ???)                                                               AS total_committees,
    -- TODO: count only committees where status is 'Active' (or equivalent)
    SUM(CASE WHEN ??? = '???' THEN 1 ELSE 0 END)                                     AS active_committees,
    -- TODO: count rows where is_chair is true
    SUM(CASE WHEN ??? THEN 1 ELSE 0 END)                                             AS chairs_held,
    -- TODO: which party column? (party? political_party?)
    MAX(???)                                                                          AS party
FROM   committee_assignments
GROUP  BY ???
ORDER  BY total_committees DESC
