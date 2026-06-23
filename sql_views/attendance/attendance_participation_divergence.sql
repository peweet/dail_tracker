-- v_attendance_participation_divergence
-- Source: data/gold/parquet/participation_presence_year.parquet
--         (written by extractors/participation_extract.py).
--
-- THE headline signal: a member can sign in at Leinster House to the 120-day TAA
-- threshold (looks "present") yet cast almost no votes. Blaney 2024 — 120 days
-- present, 20% of votes — is the archetype the old count hid. Surfaced only for
-- backbenchers (ministers are excluded from the TAA record so have no presence
-- figure to compare). Pure facts: days present vs votes cast — no inferred reason.
CREATE OR REPLACE VIEW v_attendance_participation_divergence AS
SELECT
    COALESCE(unique_member_code, '') AS unique_member_code,
    full_name        AS member_name,
    COALESCE(party_name, '')   AS party_name,
    COALESCE(constituency, '') AS constituency,
    house,
    CAST(year AS INTEGER) AS year,
    total_days        AS taa_days_present,
    COALESCE(voted_in, 0)        AS votes_cast,
    total_divisions,
    turnout_pct,
    divergence_present_low_vote
FROM read_parquet('data/gold/parquet/participation_presence_year.parquet')
WHERE divergence_present_low_vote = TRUE;
