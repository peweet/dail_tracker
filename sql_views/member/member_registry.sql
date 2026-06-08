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
WITH dail AS (
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
)
SELECT * FROM dail
UNION ALL
SELECT * FROM seanad
ORDER BY member_name;
