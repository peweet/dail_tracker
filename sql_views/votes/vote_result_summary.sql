-- One summary row per house so the votes page hero can scope its corpus
-- counts to the selected chamber (Dáil / Seanad).
CREATE OR REPLACE VIEW v_vote_result_summary AS
SELECT
    house,
    COUNT(DISTINCT vote_id)             AS division_count,
    COUNT(DISTINCT unique_member_code)  AS member_count,
    MIN(CAST(date AS DATE))             AS first_vote_date,
    MAX(CAST(date AS DATE))             AS last_vote_date
FROM v_vote_base
GROUP BY house;
