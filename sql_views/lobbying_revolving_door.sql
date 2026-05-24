-- v_lobbying_revolving_door — former DPOs appearing on lobbying returns
-- Source: data/gold/parquet/revolving_door_dpos.parquet
-- Caveats must be surfaced in the UI via todo_callout().
--
-- unique_member_code is brought in via LEFT JOIN against the silver member registry
-- (flattened_members.parquet) using LOWER(strip_accents(TRIM())) on both sides.
-- Only current Dáil/Seanad members match; non-Oireachtas DPOs (councillors,
-- civil servants, members of previous Oireachtas) surface with NULL so the UI
-- can degrade gracefully.

CREATE OR REPLACE VIEW v_lobbying_revolving_door AS
WITH member_codes AS (
    SELECT norm_name, unique_member_code
    FROM (
        SELECT
            LOWER(strip_accents(TRIM(full_name))) AS norm_name,
            unique_member_code,
            ROW_NUMBER() OVER (
                PARTITION BY LOWER(strip_accents(TRIM(full_name)))
                ORDER BY unique_member_code DESC
            ) AS rn
        FROM read_parquet('data/silver/parquet/flattened_members.parquet')
        WHERE full_name IS NOT NULL AND unique_member_code IS NOT NULL
    )
    WHERE rn = 1
)
SELECT
    src.dpos_or_former_dpos_who_carried_out_lobbying_name AS individual_name,
    mc.unique_member_code                                 AS unique_member_code,
    COALESCE(src.current_or_former_dpos_position, '')     AS former_position,
    COALESCE(src.current_or_former_dpos_chamber,  '')     AS former_chamber,
    SPLIT_PART(COALESCE(src.current_or_former_dpos_chamber, ''), '::', 1) AS chamber_display,
    COALESCE(src.returns_involved_in, 0)::INT             AS return_count,
    COALESCE(src.distinct_lobbyist_firms, 0)::INT         AS distinct_firms,
    COALESCE(src.distinct_policy_areas, 0)::INT           AS distinct_policy_areas,
    COALESCE(src.distinct_politicians_targeted, 0)::INT   AS distinct_politicians_targeted,
    NULL::VARCHAR                                         AS source_url
FROM read_parquet('data/gold/parquet/revolving_door_dpos.parquet') src
LEFT JOIN member_codes mc
    ON LOWER(strip_accents(TRIM(src.dpos_or_former_dpos_who_carried_out_lobbying_name)))
     = mc.norm_name
WHERE src.dpos_or_former_dpos_who_carried_out_lobbying_name IS NOT NULL
ORDER BY return_count DESC;
