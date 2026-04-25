-- Grain: one row per (party, vote_type)
-- Source: current_dail_vote_history
-- Used by: votes.py party chart — bar chart of yes/no/abstain split per party
--
-- Key concepts:
--   Window functions / percentage of total  → https://duckdb.org/docs/sql/window_functions
--   NULLIF to avoid divide-by-zero          → https://duckdb.org/docs/sql/functions/numeric
--
-- Available columns: party, vote_type, vote_id, full_name, debate_title, date
-- Distinct vote_type values: 'Voted Yes', 'Voted No', 'Abstained'

SELECT
    party,
    vote_type,
    COUNT(*)                                                                            AS vote_count,
    ROUND(
        100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (PARTITION BY party), 0),
        1
    )                                                                                   AS pct_of_party_votes
FROM   current_dail_vote_history
WHERE  party IS NOT NULL
  AND  party <> ''
GROUP  BY party, vote_type
ORDER  BY party, vote_count DESC
