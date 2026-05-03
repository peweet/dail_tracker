-- v_lobbying_contact_detail — one row per (politician, return, policy area)
-- Source: data/silver/lobbying/parquet/politician_returns_detail.parquet
-- Produced by lobby_processing.build_politician_returns_fact_table (already deduped).
-- Supports all Stage 2 filters:
--   politician profile  → WHERE member_name = ?
--   org profile         → WHERE lobbyist_name = ?
--   area profile        → WHERE public_policy_area = ?
--
-- unique_member_code is brought in via LEFT JOIN against most_lobbied_politicians.parquet
-- (the only source carrying the ID today). Deduped to one (full_name, code) pair so the
-- join preserves the original row count.

CREATE OR REPLACE VIEW v_lobbying_contact_detail AS
WITH member_codes AS (
    SELECT full_name, unique_member_code
    FROM (
        SELECT
            full_name,
            unique_member_code,
            ROW_NUMBER() OVER (PARTITION BY full_name ORDER BY unique_member_code DESC) AS rn
        FROM read_parquet('data/gold/parquet/most_lobbied_politicians.parquet')
        WHERE unique_member_code IS NOT NULL AND unique_member_code <> ''
    )
    WHERE rn = 1
)
SELECT
    src.primary_key                     AS return_id,
    src.full_name                       AS member_name,
    COALESCE(mc.unique_member_code, '') AS unique_member_code,
    COALESCE(src.chamber,   '')         AS chamber,
    COALESCE(src.position,  '')         AS position,
    src.lobbyist_name,
    src.public_policy_area,
    src.lobbying_period_start_date::DATE AS period_start_date,
    src.lobby_url                       AS source_url
FROM read_parquet('data/silver/lobbying/parquet/politician_returns_detail.parquet') src
LEFT JOIN member_codes mc ON src.full_name = mc.full_name
ORDER BY period_start_date DESC NULLS LAST;
