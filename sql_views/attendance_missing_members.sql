-- v_attendance_missing_members — TDs on the current Dáil roster who have no row
-- in the attendance parquet. Used by the attendance page to make the gap between
-- roster size (~174) and attendance-roster size visible to users.
--
-- Two-way classification:
--   • 'office_holder' — flagged minister/minister-of-state OR currently holds a
--     ministerial post in data/silver/ministerial_tenure.parquet (end_date IS NULL).
--     The official Oireachtas TAA PDFs do not capture office-holder attendance, so
--     their absence is a source-document limitation, NOT an editorial decision.
--   • 'no_record_on_file' — everyone else. Includes the Taoiseach (the
--     ministerial_office flag in flattened_members.csv classifies departmental
--     ministries only, so the Taoiseach surfaces here) and any genuine roster /
--     name-match gaps in the ETL.

CREATE OR REPLACE VIEW v_attendance_missing_members AS
WITH roster AS (
    SELECT DISTINCT
        first_name || ' ' || last_name AS member_name,
        party                          AS party_name,
        constituency_name              AS constituency,
        ministerial_office,
        year_elected
    FROM read_csv_auto('data/silver/flattened_members.csv')
    WHERE first_name IS NOT NULL AND last_name IS NOT NULL
),
attendance_roster AS (
    SELECT DISTINCT full_name
    FROM read_parquet('data/gold/parquet/attendance_by_td_year.parquet')
    WHERE full_name IS NOT NULL
),
current_office AS (
    SELECT
        minister_name,
        STRING_AGG(DISTINCT department_label, ', ' ORDER BY department_label) AS departments_held
    FROM read_parquet('data/silver/ministerial_tenure.parquet')
    WHERE end_date IS NULL
    GROUP BY minister_name
)
SELECT
    r.member_name,
    COALESCE(r.party_name, '')      AS party_name,
    COALESCE(r.constituency, '')    AS constituency,
    r.ministerial_office,
    r.year_elected,
    COALESCE(co.departments_held, '') AS departments_held,
    CASE
        WHEN r.ministerial_office OR co.minister_name IS NOT NULL THEN 'office_holder'
        ELSE 'no_record_on_file'
    END                              AS missing_reason,
    'data/gold/parquet/attendance_by_td_year.parquet' AS source_summary,
    current_timestamp                AS latest_fetch_timestamp_utc
FROM roster r
LEFT JOIN attendance_roster a ON a.full_name = r.member_name
LEFT JOIN current_office co    ON co.minister_name = r.member_name
WHERE a.full_name IS NULL
ORDER BY missing_reason, r.member_name;
