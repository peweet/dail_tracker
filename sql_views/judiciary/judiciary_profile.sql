-- v_judiciary_profile — per-judge identity summary for the profile drill-down.
-- Source: data/gold/parquet/judiciary_bench.parquet LEFT JOINed to the gov.ie
--   nominations context for that judge (prior career + the vacancy they filled).
--
-- Grain: one row per sitting judge (judge_key) — same universe as v_judiciary_roster,
-- shaped for the ?judge= career-arc view: current office + rank, first appointment +
-- appointing authority, the elevation_path string, salary band, HC assignment, and the
-- honesty flags (has_spine false = pre-2016 record gap; requires_manual_review = a
-- low-confidence join to surface, not hide). The per-event timeline itself is read from
-- v_judiciary_appointments filtered to the same judge_key.
-- NO vacancy rows and NO conduct/performance data appear here by construction.
CREATE OR REPLACE VIEW v_judiciary_profile AS
SELECT
    b.judge_key,
    b.judge_name,
    b.court            AS current_court,
    b.court_rank,
    b.is_ex_officio_or_multi,
    b.salary_band_eur,
    b.salary_office,
    b.salary_source,
    b.assignment,
    b.assignment_term,
    b.has_spine,
    b.first_appointed_date,
    b.first_appointing_authority,
    b.appointed_court,
    b.is_elevation,
    b.elevation_path,
    b.requires_manual_review,
    b.appt_source_url,
    b.source_url,
    b.source_published_at,
    n.prior_career    AS govie_prior_career,
    n.vacancy_cause   AS govie_vacancy_cause,
    n.predecessor     AS govie_predecessor,
    n.source_url      AS govie_source_url
FROM read_parquet('data/gold/parquet/judiciary_bench.parquet') b
LEFT JOIN read_parquet('data/gold/parquet/judiciary_nominations.parquet') n
    ON b.judge_key = n.judge_key AND b.appointed_court = n.target_court
ORDER BY b.court_rank, b.judge_name;
