-- v_member_registry — canonical TD list sourced from data.oireachtas.ie API
-- Source: data/silver/parquet/flattened_members.parquet (Dáil 34, 174 members)
--
-- This is the authoritative member reference for the UI layer.
-- PDF-derived data (attendance, payments) must supplement this; it must never
-- replace it as the primary member list. Attendance/payments may be absent for
-- any given member — that is a pipeline coverage gap, not a reason to exclude.

CREATE OR REPLACE VIEW v_member_registry AS
SELECT
    unique_member_code,
    full_name                                                      AS member_name,
    constituency_name                                              AS constituency,
    party                                                          AS party_name,
    CASE WHEN LOWER(CAST(ministerial_office AS VARCHAR)) = 'true'
         THEN 'true' ELSE 'false' END                             AS is_minister,
    year_elected,
    membership_start_date,
    membership_end_date
FROM read_parquet('{MEMBER_PARQUET_PATH}')
WHERE full_name IS NOT NULL
  AND unique_member_code IS NOT NULL
ORDER BY full_name;
