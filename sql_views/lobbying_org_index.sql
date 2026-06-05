-- v_lobbying_org_index — ranked list of most active lobbying organisations
-- Source: data/gold/parquet/top_lobbyist_organisations.parquet
--
-- first_period / last_period derived from lobbyist_persistence.parquet
--   (first_return_date / last_return_date already computed by pipeline).
--
-- website / profile_url come from the lobbying.ie organisation register, carried
--   through gold by sql_queries/top_lobbyist_organisations.sql (joined from
--   split_lobbyists). `sector` has no clean source — the register only supplies a
--   free-text `main_activities_of_organisation`, surfaced here as `main_activities`
--   rather than mislabelled as a sector taxonomy. The richer charity/CRO-derived
--   sector_label lives in v_experimental_lobbying_org_index_enriched.

CREATE OR REPLACE VIEW v_lobbying_org_index AS
WITH persistence AS (
    SELECT
        lobbyist_name,
        CAST(first_return_date AS DATE) AS first_return_date,
        CAST(last_return_date  AS DATE) AS last_return_date
    FROM read_parquet('data/gold/parquet/lobbyist_persistence.parquet')
)
SELECT
    o.lobbyist_name,
    ''                                          AS sector,
    o.returns_filed                             AS return_count,
    o.distinct_politicians_targeted             AS politicians_targeted,
    o.distinct_policy_areas,
    COALESCE(o.website, '')                      AS website,
    COALESCE(o.lobby_org_link, '')               AS profile_url,
    COALESCE(o.main_activities_of_organisation, '') AS main_activities,
    COALESCE(o.company_registration_number, '')  AS company_registration_number,
    COALESCE(o.company_registered_name, '')      AS company_registered_name,
    CAST(p.first_return_date AS VARCHAR)        AS first_period,
    CAST(p.last_return_date  AS VARCHAR)        AS last_period
FROM read_parquet('data/gold/parquet/top_lobbyist_organisations.parquet') o
LEFT JOIN persistence p ON o.lobbyist_name = p.lobbyist_name
ORDER BY return_count DESC;
