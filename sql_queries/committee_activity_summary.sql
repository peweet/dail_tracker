-- Grain: one row per member (TD or Senator) who appears in committee_assignments
-- Source: committee_assignments (gold CSV — auto-registered, wide format with up to 12 committee slots per row)
--   slot columns: committee_N_name_en, committee_N_main_status, committee_N_role_title (slots 9-11 lack role_title)
-- Joins: flattened_members + flattened_seanad_members (silver Parquet, auto-registered) for party
-- Used by: committees.py TD overview table and leaderboard
--
-- Notes from data inspection:
--   committee_N_main_status values: 'Live' or NULL  (no 'Active' value exists)
--   committee_N_role_title values:  'Cathaoirleach' (chair), 'Leas-Chathaoirleach' (vice-chair), or NULL

WITH long AS (
    SELECT full_name, committee_1_name_en  AS committee, committee_1_main_status  AS main_status, committee_1_role_title  AS role_title FROM committee_assignments WHERE committee_1_name_en  IS NOT NULL
    UNION ALL SELECT full_name, committee_2_name_en,  committee_2_main_status,  committee_2_role_title  FROM committee_assignments WHERE committee_2_name_en  IS NOT NULL
    UNION ALL SELECT full_name, committee_3_name_en,  committee_3_main_status,  committee_3_role_title  FROM committee_assignments WHERE committee_3_name_en  IS NOT NULL
    UNION ALL SELECT full_name, committee_4_name_en,  committee_4_main_status,  committee_4_role_title  FROM committee_assignments WHERE committee_4_name_en  IS NOT NULL
    UNION ALL SELECT full_name, committee_5_name_en,  committee_5_main_status,  committee_5_role_title  FROM committee_assignments WHERE committee_5_name_en  IS NOT NULL
    UNION ALL SELECT full_name, committee_6_name_en,  committee_6_main_status,  committee_6_role_title  FROM committee_assignments WHERE committee_6_name_en  IS NOT NULL
    UNION ALL SELECT full_name, committee_7_name_en,  committee_7_main_status,  committee_7_role_title  FROM committee_assignments WHERE committee_7_name_en  IS NOT NULL
    UNION ALL SELECT full_name, committee_8_name_en,  committee_8_main_status,  committee_8_role_title  FROM committee_assignments WHERE committee_8_name_en  IS NOT NULL
    UNION ALL SELECT full_name, committee_9_name_en,  committee_9_main_status,  CAST(NULL AS VARCHAR)   FROM committee_assignments WHERE committee_9_name_en  IS NOT NULL
    UNION ALL SELECT full_name, committee_10_name_en, committee_10_main_status, CAST(NULL AS VARCHAR)   FROM committee_assignments WHERE committee_10_name_en IS NOT NULL
    UNION ALL SELECT full_name, committee_11_name_en, committee_11_main_status, CAST(NULL AS VARCHAR)   FROM committee_assignments WHERE committee_11_name_en IS NOT NULL
    UNION ALL SELECT full_name, committee_12_name_en, committee_12_main_status, committee_12_role_title FROM committee_assignments WHERE committee_12_name_en IS NOT NULL
),
party_lookup AS (
    SELECT full_name, party FROM flattened_members
    UNION ALL
    SELECT full_name, party FROM flattened_seanad_members
)
SELECT
    long.full_name                                                          AS full_name,
    COUNT(DISTINCT long.committee)                                          AS total_committees,
    SUM(CASE WHEN long.main_status = 'Live' THEN 1 ELSE 0 END)              AS active_committees,
    SUM(CASE WHEN long.role_title  = 'Cathaoirleach' THEN 1 ELSE 0 END)     AS chairs_held,
    MAX(p.party)                                                            AS party
FROM long
LEFT JOIN party_lookup p ON p.full_name = long.full_name
GROUP BY long.full_name
ORDER BY total_committees DESC
