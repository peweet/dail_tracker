-- v_lobbying_clients — client companies lobbied on behalf of by a firm
-- Source: data/silver/lobbying/client_company_returns_detail.csv
-- One row per (client, lobbying_firm, return).
-- Use to show: clients of a given lobbying firm (filter by lobbying_firm)

CREATE OR REPLACE VIEW v_lobbying_clients AS
SELECT
    client_name,
    primary_key                         AS return_id,
    lobby_url                           AS source_url,
    lobbying_firm,
    policy_areas,
    politicians_targeted,
    politicians_count,
    lobbying_period_start_date::DATE    AS period_start_date
FROM read_csv_auto('data/silver/lobbying/client_company_returns_detail.csv')
WHERE client_name IS NOT NULL
ORDER BY lobbying_period_start_date DESC NULLS LAST;
