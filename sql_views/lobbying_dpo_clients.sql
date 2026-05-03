-- v_lobbying_dpo_clients — one row per (DPO individual, client company)
-- Drives the "Clients represented" section on the Stage 2 individual profile.
-- Source: data/silver/lobbying/parquet/revolving_door_returns_detail.parquet

CREATE OR REPLACE VIEW v_lobbying_dpo_clients AS
SELECT
    dpos_or_former_dpos_who_carried_out_lobbying_name AS individual_name,
    display_client_name                                AS client_name,
    COUNT(*)                                           AS return_count,
    MIN(lobbying_period_start_date::DATE)              AS first_period,
    MAX(lobbying_period_start_date::DATE)              AS last_period
FROM read_parquet('data/silver/lobbying/parquet/revolving_door_returns_detail.parquet')
WHERE dpos_or_former_dpos_who_carried_out_lobbying_name IS NOT NULL
  AND display_client_name IS NOT NULL
  AND TRIM(display_client_name) <> ''
GROUP BY dpos_or_former_dpos_who_carried_out_lobbying_name, display_client_name
ORDER BY return_count DESC;
