-- v_judiciary_appointments — judicial appointment events (event grain).
-- Source: data/gold/parquet/judiciary_appointments.parquet (one row per appointee per
--   Iris Oifigiúil judicial appointment notice, real courts only — the body=='Courts'
--   junk bucket is already dropped upstream) LEFT JOINed to the gov.ie nominations
--   context (prior career + vacancy cause + predecessor) by judge + court.
--
-- Grain: one row per (appointee, issue_date) appointment event. current_court / status
-- come from the validated spine→roster match; is_elevation marks a move to a more senior
-- court and elevated_to names it. requires_manual_review flags an "elevation" to a more
-- junior court (a name-collision artefact, not a real promotion). govie_* columns are the
-- nomination enrichment and are NULL for events with no matched gov.ie release.
-- Every event carries its Iris source_url; no fact is shown without provenance.
CREATE OR REPLACE VIEW v_judiciary_appointments AS
SELECT
    a.judge_key,
    a.appointee,
    a.issue_date,
    a.appointed_court,
    a.role,
    a.appointing_authority,
    a.current_court,
    a.status,
    a.is_elevation,
    a.elevated_to,
    a.requires_manual_review,
    a.notice_ref,
    a.source_url,
    n.prior_career      AS govie_prior_career,
    n.vacancy_cause     AS govie_vacancy_cause,
    n.predecessor       AS govie_predecessor,
    n.announce_date     AS govie_announce_date,
    n.source_url        AS govie_source_url
FROM read_parquet('data/gold/parquet/judiciary_appointments.parquet') a
LEFT JOIN read_parquet('data/gold/parquet/judiciary_nominations.parquet') n
    ON a.judge_key = n.judge_key AND a.appointed_court = n.target_court
ORDER BY a.issue_date DESC, a.appointed_court, a.appointee;
