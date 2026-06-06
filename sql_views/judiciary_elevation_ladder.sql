-- v_judiciary_elevation_ladder — real judicial promotions per court transition (aggregate).
-- Source: data/gold/parquet/judiciary_appointments.parquet.
--
-- Grain: one row per (from-court -> to-court) elevation with its count. Excludes the
-- name-collision artefacts flagged requires_manual_review (an "elevation" to a more
-- junior court is impossible). This is the pipeline-owned rollup behind the Appointments
-- tab's elevation ladder; the page renders it without counting in-app.
CREATE OR REPLACE VIEW v_judiciary_elevation_ladder AS
SELECT
    appointed_court,
    elevated_to,
    count(*) AS n
FROM read_parquet('data/gold/parquet/judiciary_appointments.parquet')
WHERE is_elevation AND NOT requires_manual_review
GROUP BY appointed_court, elevated_to
ORDER BY n DESC;
