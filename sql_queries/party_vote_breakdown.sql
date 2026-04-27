
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
