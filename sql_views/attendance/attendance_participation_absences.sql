-- v_attendance_participation_absences
-- Sources: participation_absence_gaps.parquet (longest PHYSICAL-absence run),
--          participation_member_year.parquet (name / party / role context),
--          participation_absence_news.parquet (sourced explanation, if any).
--          All written by extractors/participation_extract.py.
--
-- "Notable absences": the longest unbroken run of consecutive PLENARY SITTING
-- DAYS a member was not recorded present at Leinster House at all (no sitting AND
-- no committee badge-in) — physical absence from the TAA attendance PDFs, NOT a
-- vote-gap. The old vote-gap conflated absence with present-but-not-voting
-- (pairing): a member could be in the building every day yet show a vote-gap.
-- Recess-proof (measured against actual sitting dates) and interior-only (always
-- real). The explanation columns DISPLAY a sourced fact (curated, verified) — never
-- an inferred reason. A NULL reason renders as "no public explanation found".
CREATE OR REPLACE VIEW v_attendance_participation_absences AS
WITH gaps AS (
    SELECT
        COALESCE(unique_member_code, '') AS unique_member_code,
        full_name AS member_name,
        house,
        CAST(year AS INTEGER) AS year,
        longest_run_sitting_days,
        run_calendar_days,
        run_start,
        run_end
    FROM read_parquet('data/gold/parquet/participation_absence_gaps.parquet')
),
ctx AS (
    SELECT unique_member_code, house, year, party, turnout_pct,
           is_minister, is_chair, is_leader, role, role_note
    FROM read_parquet('data/gold/parquet/participation_member_year.parquet')
),
news AS (
    SELECT unique_member_code, year, reason_label, source_title, source_url, outlet, is_curated
    FROM read_parquet('data/gold/parquet/participation_absence_news.parquet')
)
SELECT
    g.unique_member_code,
    g.member_name,
    COALESCE(c.party, '')      AS party_name,
    g.house,
    g.year,
    g.longest_run_sitting_days,
    g.run_calendar_days,
    g.run_start,
    g.run_end,
    c.turnout_pct,
    COALESCE(c.is_minister, FALSE) AS is_minister,
    COALESCE(c.is_chair, FALSE)    AS is_chair,
    COALESCE(c.is_leader, FALSE)   AS is_leader,
    COALESCE(c.role, '')           AS role,
    COALESCE(c.role_note, '')      AS role_note,
    n.reason_label,
    n.source_title,
    n.source_url,
    n.is_curated
FROM gaps g
LEFT JOIN ctx c
    ON c.unique_member_code = g.unique_member_code AND c.house = g.house AND c.year = g.year
LEFT JOIN news n
    ON n.unique_member_code = g.unique_member_code AND (n.year = g.year OR n.year IS NULL)
-- one explanation per (member, year): prefer a year-matched curated note, then any
-- curated note, then a live-feed headline.
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY g.unique_member_code, g.year
    ORDER BY (n.year = g.year) DESC NULLS LAST, n.is_curated DESC NULLS LAST
) = 1;
