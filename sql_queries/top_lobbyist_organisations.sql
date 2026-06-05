-- Grain: one row per lobbyist organisation
-- Source: politician_returns_detail (silver Parquet — auto-registered)
--   columns: full_name, chamber, position, primary_key, lobby_url,
--            lobbyist_name, public_policy_area, lobbying_period_start_date
-- (lobby_count_details is org-level only and does not carry full_name / public_policy_area)
--
-- Org-register enrichment — website, company registration number, registered
-- company name, main activities and the lobbying.ie organisation page — is
-- joined from split_lobbyists (silver Parquet — auto-registered), which carries
-- the lobbying.ie organisation-register fields per lobbyist. The register can
-- list one organisation under several issue URIs, so split_lobbyists may hold
-- >1 row per lobbyist_name; it is deduped to a single row (richest website, then
-- richest activities text) before the LEFT JOIN so this table stays strictly
-- one-row-per-organisation. LEFT JOIN keeps organisations with no register match
-- (their enrichment columns are NULL).
WITH org_register AS (
    SELECT
        lobbyist_name,
        main_activities_of_organisation,
        website,
        company_registration_number,
        company_registered_name,
        lobby_org_link
    FROM split_lobbyists
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY lobbyist_name
        ORDER BY LENGTH(COALESCE(website, '')) DESC,
                 LENGTH(COALESCE(main_activities_of_organisation, '')) DESC
    ) = 1
)
SELECT
    p.lobbyist_name,
    COUNT(DISTINCT p.primary_key)            AS returns_filed,
    COUNT(DISTINCT p.full_name)              AS distinct_politicians_targeted,
    COUNT(DISTINCT p.public_policy_area)     AS distinct_policy_areas,
    -- org_register is one row per lobbyist_name, so these are constant within the
    -- group; ANY_VALUE picks that single value without an extra GROUP BY column.
    ANY_VALUE(r.main_activities_of_organisation) AS main_activities_of_organisation,
    ANY_VALUE(r.website)                         AS website,
    ANY_VALUE(r.company_registration_number)     AS company_registration_number,
    ANY_VALUE(r.company_registered_name)         AS company_registered_name,
    ANY_VALUE(r.lobby_org_link)                  AS lobby_org_link
FROM   politician_returns_detail p
LEFT JOIN org_register r ON r.lobbyist_name = p.lobbyist_name
WHERE  p.lobbyist_name IS NOT NULL
  AND  p.lobbyist_name <> ''
GROUP  BY p.lobbyist_name
ORDER  BY returns_filed DESC
