-- v_lobbying_org_index — ranked list of most active lobbying organisations
-- Source: data/gold/parquet/top_lobbyist_organisations.parquet

CREATE OR REPLACE VIEW v_lobbying_org_index AS
SELECT
    lobbyist_name,
    ''                              AS sector,
    returns_filed                   AS return_count,
    distinct_politicians_targeted   AS politicians_targeted,
    distinct_policy_areas,
    ''                              AS website,
    ''                              AS profile_url,
    NULL::VARCHAR                   AS first_period,
    NULL::VARCHAR                   AS last_period
FROM read_parquet('data/gold/parquet/top_lobbyist_organisations.parquet')
ORDER BY return_count DESC;
