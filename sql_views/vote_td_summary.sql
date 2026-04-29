CREATE OR REPLACE VIEW td_vote_summary AS
SELECT
    unique_member_code                                              AS member_id,
    full_name                                                       AS member_name,
    party                                                           AS party_name,
    MAX(constituency_name)                                          AS constituency,
    COUNT(CASE WHEN vote_type = 'Voted Yes' THEN 1 END)             AS yes_count,
    COUNT(CASE WHEN vote_type = 'Voted No'  THEN 1 END)             AS no_count,
    COUNT(CASE WHEN vote_type = 'Abstained' THEN 1 END)             AS abstained_count,
    COUNT(DISTINCT vote_id)                                         AS division_count,
    ROUND(
        100.0 * COUNT(CASE WHEN vote_type = 'Voted Yes' THEN 1 END) /
        NULLIF(COUNT(CASE WHEN vote_type IN ('Voted Yes','Voted No','Abstained') THEN 1 END), 0),
        1
    )                                                               AS yes_rate_pct
FROM read_parquet('{PARQUET_PATH}')
WHERE full_name IS NOT NULL
  AND unique_member_code IS NOT NULL
GROUP BY unique_member_code, full_name, party;
