-- v_lobbying_policy_area_summary — return count per public policy area
-- Sources:
--   return_count / distinct_orgs → data/gold/parquet/policy_area_breakdown.parquet
--   distinct_politicians         → data/gold/parquet/politician_policy_exposure.parquet (aggregated)

CREATE OR REPLACE VIEW v_lobbying_policy_area_summary AS
WITH pol_counts AS (
    SELECT public_policy_area, COUNT(DISTINCT full_name) AS distinct_politicians
    FROM read_parquet('data/gold/parquet/politician_policy_exposure.parquet')
    WHERE public_policy_area IS NOT NULL
    GROUP BY public_policy_area
)
SELECT
    p.public_policy_area,
    p.return_count,
    COALESCE(p.distinct_lobbyists, 0)       AS distinct_orgs,
    COALESCE(pc.distinct_politicians, 0)    AS distinct_politicians
FROM read_parquet('data/gold/parquet/policy_area_breakdown.parquet') p
LEFT JOIN pol_counts pc ON p.public_policy_area = pc.public_policy_area
WHERE p.public_policy_area IS NOT NULL
ORDER BY p.return_count DESC;
