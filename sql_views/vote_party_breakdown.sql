CREATE OR REPLACE VIEW party_vote_breakdown AS
SELECT
    vote_id,
    party                       AS party_name,
    vote_type,
    COUNT(*)                    AS member_count,
    ROUND(
        COUNT(*) * 100.0
        / SUM(COUNT(*)) OVER (PARTITION BY vote_id, party),
        1
    )                           AS vote_pct
FROM read_parquet('{PARQUET_PATH}')
GROUP BY vote_id, party, vote_type;
