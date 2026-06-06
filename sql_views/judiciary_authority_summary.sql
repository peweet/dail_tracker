-- v_judiciary_authority_summary — how each judicial appointment notice recorded
-- the appointing authority (aggregate).
-- Source: data/gold/parquet/judiciary_appointments.parquet.
--
-- Grain: one row per appointing_authority with its appointment-notice count. This is
-- the pipeline-owned rollup behind the Appointments tab's authority cards (the page
-- does NOT count in-app). NOTE: constitutionally the Government decides judicial
-- appointments and the President formalises them; this is a count of how the NOTICE
-- recorded the authority, not a measure of who exercised the choice. The page frames
-- it as such.
CREATE OR REPLACE VIEW v_judiciary_authority_summary AS
SELECT
    appointing_authority,
    count(*) AS n
FROM read_parquet('data/gold/parquet/judiciary_appointments.parquet')
GROUP BY appointing_authority
ORDER BY n DESC;
