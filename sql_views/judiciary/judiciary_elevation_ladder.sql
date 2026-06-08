-- v_judiciary_elevation_ladder — real judicial promotions per court transition (aggregate).
-- Source: data/gold/parquet/judiciary_appointments.parquet.
--
-- Grain: one row per (from-court -> to-court) elevation. Counts DISTINCT judges, not
-- notices: a judge can carry several appointment notices to the same court (re-notices /
-- corrections), and because is_elevation is derived from the current-roster court, every
-- one of those notices is flagged as the same elevation — counting rows would double-count
-- a single promotion (e.g. High Court -> Court of Appeal read 17 notices but 11 judges).
-- Excludes the name-collision artefacts flagged requires_manual_review (an "elevation" to a
-- more junior court is impossible). This is the pipeline-owned rollup behind the
-- Appointments tab's elevation ladder; the page renders it without counting in-app.
CREATE OR REPLACE VIEW v_judiciary_elevation_ladder AS
SELECT
    appointed_court,
    elevated_to,
    count(DISTINCT judge_key) AS n
FROM read_parquet('data/gold/parquet/judiciary_appointments.parquet')
WHERE is_elevation AND NOT requires_manual_review
GROUP BY appointed_court, elevated_to
ORDER BY n DESC;
