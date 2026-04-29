CREATE OR REPLACE VIEW v_vote_index AS
SELECT
    vote_id,
    MAX(CAST(date AS DATE))                                                          AS vote_date,
    MAX(debate_title)                                                                AS debate_title,
    MAX(vote_outcome)                                                                AS vote_outcome,
    COUNT(CASE WHEN vote_type = 'Voted Yes' THEN 1 END)                             AS yes_count,
    COUNT(CASE WHEN vote_type = 'Voted No'  THEN 1 END)                             AS no_count,
    COUNT(CASE WHEN vote_type = 'Abstained' THEN 1 END)                             AS abstained_count,
    ABS(
        COUNT(CASE WHEN vote_type = 'Voted Yes' THEN 1 END) -
        COUNT(CASE WHEN vote_type = 'Voted No'  THEN 1 END)
    )                                                                                AS margin,
    MAX(subject)                                                                     AS subject
FROM read_parquet('{PARQUET_PATH}')
WHERE full_name IS NOT NULL
GROUP BY vote_id;
