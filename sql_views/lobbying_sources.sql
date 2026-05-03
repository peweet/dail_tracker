-- v_lobbying_sources — official source links for Stage 2 profile views
-- Source: data/silver/lobbying/parquet/politician_returns_detail.parquet
-- lobby_url is a real lobbying.ie return URL (https://www.lobbying.ie/return/...)
-- Used via render_source_links() from utility/ui/source_links.py.

CREATE OR REPLACE VIEW v_lobbying_sources AS
SELECT
    primary_key         AS return_id,
    full_name           AS member_name,
    lobbyist_name,
    public_policy_area,
    lobby_url           AS source_url,
    NULL::VARCHAR       AS official_pdf_url,
    NULL::VARCHAR       AS oireachtas_url
FROM read_parquet('data/silver/lobbying/parquet/politician_returns_detail.parquet')
WHERE lobby_url IS NOT NULL;
