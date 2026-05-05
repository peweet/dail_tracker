-- v_lobbying_index — ranked list of most-lobbied politicians/senators
-- Source: data/gold/parquet/most_lobbied_politicians.parquet
-- Gold file has one row per (politician, role/chamber); deduplicate to one row
-- per politician keeping the primary role (highest lobby_returns_targeting).
--
-- Enrichments computed here from other gold/silver parquet files:
--   unique_member_code     ← flattened_members.parquet (LOWER(strip_accents()) join)
--                            Only ~5% of lobbying DPOs are Dáil/Seanad members; the
--                            rest are councillors, civil servants, ministerial staff,
--                            MEPs, etc. These get an empty unique_member_code and
--                            the page renders them as non-clickable cards.
--   distinct_policy_areas  ← politician_policy_exposure.parquet (COUNT DISTINCT per name)
--   first_period/last_period ← bilateral_relationships.parquet (MIN/MAX relationship dates)
--   position               ← derived from chamber string (Dáil → "TD", Seanad → "Senator")
--
-- TODO_PIPELINE_REQUIRED: position for ministerial-office holders (Taoiseach, ministers)
--   cannot be inferred from chamber alone; requires join with flattened_members.csv
--   ministerial_office / office_1_name columns — add in enrichment step, not here.

CREATE OR REPLACE VIEW v_lobbying_index AS
WITH deduped AS (
    SELECT
        full_name,
        chamber,
        lobby_returns_targeting,
        distinct_orgs,
        total_returns,
        ROW_NUMBER() OVER (
            PARTITION BY full_name
            ORDER BY lobby_returns_targeting DESC
        ) AS rn
    FROM read_parquet('data/gold/parquet/most_lobbied_politicians.parquet')
),
member_lookup AS (
    SELECT
        full_name AS member_full_name,
        unique_member_code,
        ROW_NUMBER() OVER (
            PARTITION BY LOWER(strip_accents(TRIM(full_name)))
            ORDER BY unique_member_code DESC
        ) AS rn
    FROM read_parquet('data/silver/parquet/flattened_members.parquet')
    WHERE full_name IS NOT NULL AND unique_member_code IS NOT NULL
),
policy_areas AS (
    SELECT
        full_name,
        COUNT(DISTINCT public_policy_area) AS distinct_policy_areas
    FROM read_parquet('data/gold/parquet/politician_policy_exposure.parquet')
    WHERE public_policy_area IS NOT NULL
    GROUP BY full_name
),
periods AS (
    SELECT
        full_name,
        CAST(MIN(relationship_start) AS DATE) AS first_contact_date,
        CAST(MAX(relationship_last_seen) AS DATE) AS last_contact_date
    FROM read_parquet('data/gold/parquet/bilateral_relationships.parquet')
    WHERE relationship_start IS NOT NULL
    GROUP BY full_name
)
SELECT
    ROW_NUMBER() OVER (ORDER BY d.total_returns DESC)                   AS rank,
    d.full_name                                                         AS member_name,
    COALESCE(m.unique_member_code, '')                                  AS unique_member_code,
    COALESCE(d.chamber, '')                                             AS chamber,
    CASE
        WHEN d.chamber ILIKE '%dáil%' OR d.chamber ILIKE '%dail%' THEN 'TD'
        WHEN d.chamber ILIKE '%seanad%'                             THEN 'Senator'
        WHEN d.chamber IS NOT NULL AND d.chamber <> ''              THEN d.chamber
        ELSE ''
    END                                                                 AS position,
    d.total_returns                                                     AS return_count,
    d.distinct_orgs,
    COALESCE(pa.distinct_policy_areas, 0)                               AS distinct_policy_areas,
    CAST(p.first_contact_date AS VARCHAR)                               AS first_period,
    CAST(p.last_contact_date  AS VARCHAR)                               AS last_period
FROM deduped d
LEFT JOIN member_lookup m
    ON LOWER(strip_accents(TRIM(d.full_name))) = LOWER(strip_accents(TRIM(m.member_full_name)))
    AND m.rn = 1
LEFT JOIN policy_areas pa ON d.full_name = pa.full_name
LEFT JOIN periods      p  ON d.full_name = p.full_name
WHERE d.rn = 1
ORDER BY return_count DESC;
