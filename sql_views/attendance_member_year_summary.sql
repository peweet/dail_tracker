-- Source: data/gold/csv/attendance_by_td_year.csv
-- Join resolved in enrich.py — no join needed here.
-- Gold CSV is authoritative; update this view if it disagrees with gold.

CREATE OR REPLACE VIEW v_attendance_member_year_summary AS
SELECT
    full_name                           AS member_name,
    member_id,
    CAST(year AS INTEGER)               AS year,
    total_days                          AS attended_count,
    COALESCE(party_name,    '')         AS party_name,
    COALESCE(constituency,  '')         AS constituency,
    COALESCE(is_minister, 'false')      AS is_minister
FROM read_csv_auto('data/gold/csv/attendance_by_td_year.csv')
WHERE full_name IS NOT NULL
  AND year IS NOT NULL;
