-- v_attendance_chamber_sitting_days — per (house, year) count of distinct
-- plenary sitting dates the chamber actually sat. This is the data-derived
-- denominator the per-member attendance bar uses for SEANAD (the Dáil bar keeps
-- the curated config.SITTING_DAYS_BY_YEAR official figures). Seanad and Dáil
-- sit on different days, so a single Dáil denominator would mis-rate Senators.
--
-- File name starts with 'attendance_' so attendance_data.py registers it.

CREATE OR REPLACE VIEW v_attendance_chamber_sitting_days AS
WITH dates AS (
    SELECT 'Dáil' AS house, iso_sitting_days_attendance AS sitting_date
    FROM read_csv_auto('data/silver/aggregated_td_tables.csv')
    WHERE iso_sitting_days_attendance IS NOT NULL
    UNION ALL
    SELECT 'Seanad' AS house, iso_sitting_days_attendance
    FROM read_csv_auto('data/silver/seanad_aggregated_tables.csv')
    WHERE iso_sitting_days_attendance IS NOT NULL
)
SELECT
    house,
    CAST(year(sitting_date) AS INTEGER)         AS year,
    COUNT(DISTINCT sitting_date)                AS sitting_days
FROM dates
GROUP BY house, year(sitting_date)
ORDER BY house, year;
