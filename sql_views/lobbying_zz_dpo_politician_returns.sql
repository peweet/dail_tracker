-- v_lobbying_dpo_politician_returns — one row per (DPO, politician, return,
-- policy area). Used by the lobbying page's Stage 3 DPO×politician sub-route
-- to list every return a former-DPO filed against a specific politician.
--
-- Cardinality: a return can target multiple politicians (one row per
-- politician×area in contact_detail) and have multiple DPOs (one row per
-- DPO in dpo_returns), so the inner join fans out 1:N:M as expected.
--
-- File name prefix 'lobbying_zz_' so it loads after the two upstream views.

CREATE OR REPLACE VIEW v_lobbying_dpo_politician_returns AS
SELECT
    d.individual_name,
    c.member_name,
    c.unique_member_code,
    c.chamber,
    c.return_id,
    c.lobbyist_name,
    d.client_name,
    c.public_policy_area,
    c.period_start_date,
    c.source_url
FROM v_lobbying_dpo_returns d
JOIN v_lobbying_contact_detail c USING (return_id)
WHERE d.individual_name IS NOT NULL
  AND c.member_name IS NOT NULL;
