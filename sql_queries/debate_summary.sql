
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
