-- v_lobbying_topic_search — per-return view exposing free-text fields for
-- keyword scans driven from the UI "Topics" rail.
--
-- This is NOT a register taxonomy. lobbying.ie's official `public_policy_area`
-- is fixed (32 categories) and does not include things like immigration or
-- climate as standalone areas. The Topics rail surfaces returns whose
-- description text mentions a curated keyword set, regardless of which
-- official area the filer chose.
--
-- Source: data/silver/lobbying/parquet/returns_master.parquet (one row per
-- lobbying return; carries `relevant_matter`, `specific_details`,
-- `intended_results`).

CREATE OR REPLACE VIEW v_lobbying_topic_search AS
SELECT
    primary_key                          AS return_id,
    lobbyist_name,
    public_policy_area,
    relevant_matter,
    specific_details,
    intended_results,
    lobbying_period_start_date::DATE     AS period_start_date,
    lobby_url                            AS source_url,
    LOWER(
        COALESCE(specific_details,  '') || ' ' ||
        COALESCE(intended_results,  '') || ' ' ||
        COALESCE(relevant_matter,   '')
    )                                    AS searchable_text
FROM read_parquet('data/silver/lobbying/parquet/returns_master.parquet');
