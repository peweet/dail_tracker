-- v_lobbying_recent_returns — 20 most recently filed lobbying returns
-- TODO_PIPELINE_VIEW_REQUIRED: member_name — returns_master.parquet is return-level,
--   not politician-level, so member_name is NULL. Wire in once enrichment joins
--   returns to politician detail.
-- Source: data/silver/lobbying/parquet/returns_master.parquet (already deduped, sorted desc)

CREATE OR REPLACE VIEW v_lobbying_recent_returns AS
SELECT
    lobbying_period_start_date      AS period_start_date,
    strftime(lobbying_period_start_date, '%Y-%m') AS period_month,
    lobbyist_name,
    NULL::VARCHAR                   AS member_name,
    public_policy_area,
    COALESCE(relevant_matter, '')   AS relevant_matter,
    lobby_url                       AS source_url
FROM read_parquet('data/silver/lobbying/parquet/returns_master.parquet')
ORDER BY period_start_date DESC NULLS LAST
LIMIT 20;
