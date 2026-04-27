SELECT
    full_name,
    party,
    constituency_name,
    COUNT(DISTINCT vote_id)                                                           AS divisions,
    SUM(CASE WHEN vote_type = 'Voted Yes'  THEN 1 ELSE 0 END)                        AS yes_votes,
    SUM(CASE WHEN vote_type = 'Voted No'   THEN 1 ELSE 0 END)                        AS no_votes,
    SUM(CASE WHEN vote_type = 'Abstained'  THEN 1 ELSE 0 END)                        AS abstentions,
    ROUND(
        100.0 * SUM(CASE WHEN vote_type = 'Voted Yes' THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN vote_type IN ('Voted Yes', 'Voted No') THEN 1 ELSE 0 END), 0),
        1
    )                                                                                 AS yes_rate_pct
FROM   current_dail_vote_history
GROUP  BY full_name, party, constituency_name
ORDER  BY divisions DESC
