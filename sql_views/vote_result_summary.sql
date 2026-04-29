CREATE OR REPLACE VIEW v_vote_result_summary AS
SELECT
    COUNT(DISTINCT vote_id)             AS division_count,
    COUNT(DISTINCT unique_member_code)  AS member_count,
    MIN(CAST(date AS DATE))             AS first_vote_date,
    MAX(CAST(date AS DATE))             AS last_vote_date
FROM read_parquet('{PARQUET_PATH}');
