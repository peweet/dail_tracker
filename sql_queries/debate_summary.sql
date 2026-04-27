-- Grain: one row per debate_title
-- Source: current_dail_vote_history (one row per TD × division)
-- Used by: votes.py debate explorer — Streamlit filters this, no groupby in-page
--
-- Key concepts:
--   CASE WHEN aggregation  → https://duckdb.org/docs/sql/expressions/case
--   COUNT DISTINCT         → https://duckdb.org/docs/sql/aggregates#general-aggregate-functions
--   HAVING                 → https://duckdb.org/docs/sql/query_syntax/having
--
-- Available columns (run: SELECT * FROM current_dail_vote_history LIMIT 1):
--   debate_title, vote_id, date, vote_outcome, vote_type, subject, full_name, party

SELECT
    debate_title,
    COUNT(DISTINCT vote_id) AS divisions,
    MIN(date) AS first_date,
    MAX(date) AS last_date,
    SUM(CASE WHEN vote_outcome = 'Carried' THEN 1 ELSE 0 END) AS carried,
    SUM(CASE WHEN vote_outcome = 'Lost' THEN 1 ELSE 0 END) AS lost,
    SUM(CASE WHEN vote_type = 'Voted Yes' THEN 1 ELSE 0 END) AS total_yes,
    SUM(CASE WHEN vote_type = 'Voted No' THEN 1 ELSE 0 END) AS total_no
FROM current_dail_vote_history
GROUP BY debate_title
ORDER BY divisions DESC
