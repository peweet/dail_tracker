CREATE OR REPLACE VIEW v_vote_member_detail AS
SELECT
    vote_id,
    unique_member_code          AS member_id,
    full_name                   AS member_name,
    vote_type,
    party                       AS party_name,
    constituency_name           AS constituency,
    CAST(date AS DATE)          AS vote_date,
    debate_title,
    vote_outcome,
    vote_url                    AS oireachtas_url
FROM read_parquet('{PARQUET_PATH}');
