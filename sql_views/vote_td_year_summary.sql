CREATE OR REPLACE VIEW td_vote_year_summary AS
SELECT
    unique_member_code                                              AS member_id,
    full_name                                                       AS member_name,
    EXTRACT(YEAR FROM CAST(date AS DATE))::INTEGER                  AS year,
    COUNT(CASE WHEN vote_type = 'Voted Yes' THEN 1 END)             AS yes_count,
    COUNT(CASE WHEN vote_type = 'Voted No'  THEN 1 END)             AS no_count,
    COUNT(CASE WHEN vote_type = 'Abstained' THEN 1 END)             AS abstained_count,
    party                                                           AS party_name
FROM read_parquet('{PARQUET_PATH}')
GROUP BY unique_member_code, full_name, EXTRACT(YEAR FROM CAST(date AS DATE))::INTEGER, party;
