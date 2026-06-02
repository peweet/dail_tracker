CREATE OR REPLACE VIEW td_vote_year_summary AS
WITH base AS (
    SELECT
        unique_member_code,
        full_name,
        EXTRACT(YEAR FROM CAST(date AS DATE))::INTEGER AS year,
        vote_type,
        party,
        house
    FROM v_vote_base
)
SELECT
    unique_member_code                                              AS member_id,
    full_name                                                       AS member_name,
    year,
    house,
    COUNT(CASE WHEN vote_type = 'Voted Yes' THEN 1 END)             AS yes_count,
    COUNT(CASE WHEN vote_type = 'Voted No'  THEN 1 END)             AS no_count,
    COUNT(CASE WHEN vote_type = 'Abstained' THEN 1 END)             AS abstained_count,
    party                                                           AS party_name
FROM base
GROUP BY unique_member_code, full_name, year, party, house;
