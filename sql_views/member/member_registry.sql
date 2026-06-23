-- v_member_registry — canonical member list sourced from data.oireachtas.ie API.
-- Source: data/silver/parquet/flattened_members.parquet (Dáil 34) UNION
--         data/silver/parquet/flattened_seanad_members.parquet (Seanad 27).
--   Dáil 34 ships ~176 members (by-election TDs absorbed); Seanad 27 ships 60.
--   A `house` column ('Dáil' / 'Seanad') is added so the UI can scope its
--   picker and labels. Identity is (unique_member_code, house) — codes are NOT
--   globally unique across houses (one person, Seán Kyne, sits in both files).
--
-- This is the authoritative member reference for the UI layer.
-- PDF-derived data (attendance, payments) must supplement this; it must never
-- replace it as the primary member list. Attendance/payments may be absent for
-- any given member — that is a pipeline coverage gap, not a reason to exclude.

CREATE OR REPLACE VIEW v_member_registry AS
-- current_ministers — corrects the unreliable is_minister flag below.
-- The Oireachtas member feed's `ministerial_office` boolean returns false for many
-- cabinet members (the Taoiseach included), so it under-reports office holders.
-- The feed's OWN office slots are authoritative and current, though: a member
-- holds ministerial office if any office slot with no end_date is the Taoiseach,
-- a senior Minister, or a Minister of State. (This is the same source
-- member_salary.sql classifies for the office-allowance rate.) Chair roles —
-- Ceann Comhairle / Leas-Cheann Comhairle — are deliberately NOT counted: they
-- are not ministers and their attendance IS recorded, so they should not get the
-- ministerial framing. Self-contained: same {MEMBER_PARQUET_PATH} the dail CTE
-- reads, so no view/extra-file dependency and no Wikidata-tenure staleness (that
-- spine lagged the sitting Taoiseach). Ministers are TDs, so the Dáil feed's six
-- office slots cover every case; senators fall back to their API flag below.
WITH current_ministers AS (
    SELECT DISTINCT unique_member_code
    FROM (
        SELECT unique_member_code, CAST(office_1_name AS VARCHAR) AS office_name,
               CAST(office_1_end_date AS VARCHAR) AS end_date FROM read_parquet('{MEMBER_PARQUET_PATH}')
        UNION ALL SELECT unique_member_code, CAST(office_2_name AS VARCHAR),
               CAST(office_2_end_date AS VARCHAR) FROM read_parquet('{MEMBER_PARQUET_PATH}')
        UNION ALL SELECT unique_member_code, CAST(office_3_name AS VARCHAR),
               CAST(office_3_end_date AS VARCHAR) FROM read_parquet('{MEMBER_PARQUET_PATH}')
        UNION ALL SELECT unique_member_code, CAST(office_4_name AS VARCHAR),
               CAST(office_4_end_date AS VARCHAR) FROM read_parquet('{MEMBER_PARQUET_PATH}')
        UNION ALL SELECT unique_member_code, CAST(office_5_name AS VARCHAR),
               CAST(office_5_end_date AS VARCHAR) FROM read_parquet('{MEMBER_PARQUET_PATH}')
        UNION ALL SELECT unique_member_code, CAST(office_6_name AS VARCHAR),
               CAST(office_6_end_date AS VARCHAR) FROM read_parquet('{MEMBER_PARQUET_PATH}')
    )
    WHERE end_date IS NULL
      AND office_name IS NOT NULL
      AND (
            office_name = 'Taoiseach'
         OR office_name LIKE 'Minister for %'    -- senior ministers (Tánaiste holds one of these)
         OR office_name LIKE 'Minister of %'     -- 'Minister of State at ...' (junior ministers)
      )
),
dail AS (
    SELECT
        unique_member_code,
        full_name                                                      AS member_name,
        constituency_name                                              AS constituency,
        party                                                          AS party_name,
        CASE WHEN LOWER(CAST(ministerial_office AS VARCHAR)) = 'true'
             THEN 'true' ELSE 'false' END                             AS is_minister,
        year_elected,
        membership_start_date,
        membership_end_date,
        'Dáil'                                                         AS house
    FROM read_parquet('{MEMBER_PARQUET_PATH}')
    WHERE full_name IS NOT NULL
      AND unique_member_code IS NOT NULL
),
seanad AS (
    SELECT
        unique_member_code,
        full_name                                                      AS member_name,
        constituency_name                                              AS constituency,
        party                                                          AS party_name,
        CASE WHEN LOWER(CAST(ministerial_office AS VARCHAR)) = 'true'
             THEN 'true' ELSE 'false' END                             AS is_minister,
        year_elected,
        membership_start_date,
        membership_end_date,
        'Seanad'                                                       AS house
    FROM read_parquet('{SEANAD_MEMBER_PARQUET_PATH}')
    WHERE full_name IS NOT NULL
      AND unique_member_code IS NOT NULL
),
base AS (
    SELECT * FROM dail
    UNION ALL
    SELECT * FROM seanad
)
-- is_minister = the API flag OR a currently-held ministerial post. Column order
-- preserved exactly; stays a 'true'/'false' VARCHAR (callers do
-- str(is_minister).lower() == 'true'). The LEFT JOIN never fans out — current_ministers
-- is DISTINCT on the code.
SELECT
    base.unique_member_code,
    base.member_name,
    base.constituency,
    base.party_name,
    CASE WHEN base.is_minister = 'true' OR cm.unique_member_code IS NOT NULL
         THEN 'true' ELSE 'false' END                                 AS is_minister,
    base.year_elected,
    base.membership_start_date,
    base.membership_end_date,
    base.house
FROM base
LEFT JOIN current_ministers cm ON cm.unique_member_code = base.unique_member_code
ORDER BY base.member_name;
