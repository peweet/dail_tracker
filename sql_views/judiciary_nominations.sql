-- v_judiciary_nominations — gov.ie judicial nomination announcements (vacancy context).
-- Source: data/gold/parquet/judiciary_nominations.parquet (nominee, target court, prior
--   career, the vacancy cause and named predecessor that created the seat).
--
-- Grain: one row per nominated person per gov.ie announcement. Drives the Appointments
-- tab's vacancy-lifecycle (cause + predecessor → nominee → now on bench). vacancy_cause
-- and predecessor are preserved verbatim from the release, not normalised or inferred.
-- Every row carries its gov.ie source_url.
CREATE OR REPLACE VIEW v_judiciary_nominations AS
SELECT
    announce_date,
    nominee,
    judge_key,
    target_court,
    prior_career,
    vacancy_cause,
    predecessor,
    source_name,
    source_url
FROM read_parquet('data/gold/parquet/judiciary_nominations.parquet')
ORDER BY announce_date DESC, target_court, nominee;
