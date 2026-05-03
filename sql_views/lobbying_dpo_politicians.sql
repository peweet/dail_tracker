-- v_lobbying_dpo_politicians — one row per (DPO individual, politician targeted)
-- Drives the "Politicians targeted" section on the Stage 2 individual profile
-- and enables cross-link into the politician Stage 2 profile.
--
-- Sources:
--   data/silver/lobbying/parquet/revolving_door_returns_detail.parquet  (DPO -> primary_key)
--   data/silver/lobbying/parquet/politician_returns_detail.parquet      (politician -> primary_key)
--
-- Joined on primary_key. politician_returns_detail has multiple rows per
-- (politician, return) when a return spans multiple policy areas — the
-- DISTINCT in the CTE collapses that down to one row per (politician, return)
-- so the final COUNT(*) is honestly "distinct returns this DPO filed that
-- targeted this politician".

CREATE OR REPLACE VIEW v_lobbying_dpo_politicians AS
WITH dpo_returns AS (
    SELECT DISTINCT
        dpos_or_former_dpos_who_carried_out_lobbying_name AS individual_name,
        primary_key
    FROM read_parquet('data/silver/lobbying/parquet/revolving_door_returns_detail.parquet')
    WHERE dpos_or_former_dpos_who_carried_out_lobbying_name IS NOT NULL
),
politician_returns AS (
    SELECT DISTINCT
        primary_key,
        full_name              AS member_name,
        COALESCE(chamber, '')  AS chamber
    FROM read_parquet('data/silver/lobbying/parquet/politician_returns_detail.parquet')
    WHERE full_name IS NOT NULL
)
SELECT
    d.individual_name,
    p.member_name,
    p.chamber,
    COUNT(*) AS return_count
FROM dpo_returns d
JOIN politician_returns p USING (primary_key)
GROUP BY d.individual_name, p.member_name, p.chamber
ORDER BY return_count DESC;
