-- v_lobbying_dpo_returns — individual return rows for every revolving-door DPO
-- Source: data/silver/lobbying/parquet/revolving_door_returns_detail.parquet
-- One row per (DPO, return). Includes firm, client, policy area, URL.

CREATE OR REPLACE VIEW v_lobbying_dpo_returns AS
SELECT
    dpos_or_former_dpos_who_carried_out_lobbying_name AS individual_name,
    primary_key::VARCHAR                              AS return_id,
    lobby_url                                         AS source_url,
    lobbyist_name,
    display_client_name                               AS client_name,
    public_policy_area,
    lobbying_period_start_date::DATE                  AS period_start_date
FROM read_parquet('data/silver/lobbying/parquet/revolving_door_returns_detail.parquet')
WHERE dpos_or_former_dpos_who_carried_out_lobbying_name IS NOT NULL;
