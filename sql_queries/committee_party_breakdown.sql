-- Grain: one row per party (active memberships only)
-- Source: committee_assignments (gold CSV — auto-registered)
-- Replaces: committees.py party_seats groupby (lines 302-307)
-- Used by: committees.py "Committee seats by party" overview section
--
-- Key concepts:
--   COUNT vs COUNT DISTINCT  → https://duckdb.org/docs/sql/aggregates#general-aggregate-functions
--   CASE WHEN in aggregates  → https://duckdb.org/docs/sql/expressions/case
--   WHERE clause filtering   → https://duckdb.org/docs/sql/query_syntax/where
--
-- Run this to see available columns:
--   SELECT * FROM committee_assignments LIMIT 1
--
-- Expected columns (verify): name, party, committee, status, is_chair

SELECT
    -- TODO: verify party column name
    ???                                                                AS party,
    COUNT(*)                                                           AS seats,
    COUNT(DISTINCT ???)                                                AS distinct_members,
    SUM(CASE WHEN ??? THEN 1 ELSE 0 END)                               AS chairs_held
FROM   committee_assignments
-- TODO: verify status column and Active value (case-sensitive?)
WHERE  ??? = 'Active'
GROUP  BY ???
ORDER  BY seats DESC
