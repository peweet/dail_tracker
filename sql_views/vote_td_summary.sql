CREATE OR REPLACE VIEW td_vote_summary AS
SELECT
    unique_member_code                                              AS member_id,
    full_name                                                       AS member_name,
    party                                                           AS party_name,
    MAX(constituency_name)                                          AS constituency,
    COUNT(CASE WHEN vote_type = 'Voted Yes' THEN 1 END)             AS yes_count,
    COUNT(CASE WHEN vote_type = 'Voted No'  THEN 1 END)             AS no_count,
    COUNT(CASE WHEN vote_type = 'Abstained' THEN 1 END)             AS abstained_count,
    COUNT(DISTINCT vote_id)                                         AS division_count
FROM read_parquet('{PARQUET_PATH}')
GROUP BY unique_member_code, full_name, party;
