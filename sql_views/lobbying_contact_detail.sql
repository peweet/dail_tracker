-- v_lobbying_contact_detail — one row per (politician, return, policy area)
-- Source: data/silver/lobbying/parquet/politician_returns_detail.parquet
-- Produced by lobby_processing.build_politician_returns_fact_table (already deduped).
-- Supports all Stage 2 filters:
--   politician profile  → WHERE member_name = ?
--   org profile         → WHERE lobbyist_name = ?
--   area profile        → WHERE public_policy_area = ?

CREATE OR REPLACE VIEW v_lobbying_contact_detail AS
SELECT
    primary_key                     AS return_id,
    full_name                       AS member_name,
    COALESCE(chamber,   '')         AS chamber,
    COALESCE(position,  '')         AS position,
    lobbyist_name,
    public_policy_area,
    lobbying_period_start_date::DATE AS period_start_date,
    lobby_url                       AS source_url
FROM read_parquet('data/silver/lobbying/parquet/politician_returns_detail.parquet')
ORDER BY period_start_date DESC NULLS LAST;
