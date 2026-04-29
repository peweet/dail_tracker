-- v_lobbying_revolving_door — former DPOs appearing on lobbying returns
-- Source: data/gold/parquet/revolving_door_dpos.parquet
-- Caveats must be surfaced in the UI via todo_callout().

CREATE OR REPLACE VIEW v_lobbying_revolving_door AS
SELECT
    dpos_or_former_dpos_who_carried_out_lobbying_name   AS individual_name,
    COALESCE(current_or_former_dpos_position, '')       AS former_position,
    COALESCE(current_or_former_dpos_chamber,  '')       AS former_chamber,
    COALESCE(returns_involved_in, 0)::INT               AS return_count,
    COALESCE(distinct_lobbyist_firms, 0)::INT           AS distinct_firms,
    COALESCE(distinct_policy_areas, 0)::INT             AS distinct_policy_areas,
    COALESCE(distinct_politicians_targeted, 0)::INT     AS distinct_politicians_targeted,
    NULL::VARCHAR                                       AS source_url
FROM read_parquet('data/gold/parquet/revolving_door_dpos.parquet')
WHERE dpos_or_former_dpos_who_carried_out_lobbying_name IS NOT NULL
ORDER BY return_count DESC;
