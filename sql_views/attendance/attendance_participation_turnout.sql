-- v_attendance_participation_turnout
-- Source: data/gold/parquet/participation_member_year.parquet
--         (written by extractors/participation_extract.py).
--
-- The honest replacement for the censored TAA "sitting days" ranking: how many
-- of the term's recorded divisions each member actually voted in. A row exists
-- in the vote source per member-division, so a missed vote = genuine non-
-- participation (unfakeable, unlike the TAA badge-in count which censors at 120).
--
-- Office-holders are KEPT, not hidden: is_minister / is_chair / is_leader +
-- role_note let the UI context-flag a structurally-low voter (Ceann Comhairle
-- votes only to break ties; ministers are paired / on executive duty) rather
-- than shame them. CURRENT TERM ONLY (year >= 2025) — earlier years are
-- survivor-biased in the vote source and unsafe to rank.
CREATE OR REPLACE VIEW v_attendance_participation_turnout AS
SELECT
    COALESCE(unique_member_code, '') AS unique_member_code,
    full_name                        AS member_name,
    COALESCE(party, '')              AS party_name,
    COALESCE(constituency, '')       AS constituency,
    house,
    CAST(year AS INTEGER)            AS year,
    voted_in,
    missed,
    total_divisions,
    turnout_pct,
    COALESCE(is_minister, FALSE)     AS is_minister,
    COALESCE(is_chair, FALSE)        AS is_chair,
    COALESCE(is_leader, FALSE)       AS is_leader,
    COALESCE(role, '')               AS role,
    COALESCE(role_note, '')          AS role_note
FROM read_parquet('data/gold/parquet/participation_member_year.parquet');
