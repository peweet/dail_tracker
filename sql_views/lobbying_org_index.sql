-- v_lobbying_org_index — ranked list of most active lobbying organisations
-- Source: data/gold/parquet/top_lobbyist_organisations.parquet
--
-- first_period / last_period derived from lobbyist_persistence.parquet
--   (first_return_date / last_return_date already computed by pipeline).
--
-- TODO_PIPELINE_REQUIRED: sector, website, profile_url — not in current gold files.
--   The org_registry enrichment in lobbying_enrichment.py has scaffolding for these
--   but does not yet populate them. Add to enrichment step before wiring here.

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
    ''                                          AS website,
    ''                                          AS profile_url,
    CAST(p.first_return_date AS VARCHAR)        AS first_period,
    CAST(p.last_return_date  AS VARCHAR)        AS last_period
FROM read_parquet('data/gold/parquet/top_lobbyist_organisations.parquet') o
LEFT JOIN persistence p ON o.lobbyist_name = p.lobbyist_name
ORDER BY return_count DESC;
