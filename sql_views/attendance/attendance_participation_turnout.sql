-- v_attendance_participation_turnout
-- Sources: data/gold/parquet/current_dail_vote_history.parquet
--          data/gold/parquet/current_seanad_vote_history.parquet   (division facts)
--          data/silver/parquet/member_terms.parquet                (service windows)
--          data/silver/ministerial_tenure.parquet                  (dated senior-minister spans)
--          data/silver/parquet/flattened_members.parquet           (dated office slots, incl. MoS)
--          data/silver/parquet/flattened_seanad_members.parquet
--          data/gold/parquet/participation_member_year.parquet     (display context: constituency,
--                                                                   curated leader flag, role note)
--
-- The honest replacement for the censored TAA "sitting days" ranking: how many
-- of the term's recorded divisions each member actually voted in. A row exists
-- in the vote source per member-division, so a missed vote = genuine non-
-- participation (unfakeable, unlike the TAA badge-in count which censors at 120).
--
-- REWORKED 2026-07-13 (MCP sweep defects 5/6/7) — three fixes, all data-anchored:
--   1. CROSS-HOUSE GUARD — a member's votes count ONLY toward the house the
--      DIVISION belongs to, taken from the division's own vote_url
--      (…/debates/vote/dail/… vs …/debates/vote/seanad/…), never from which
--      source file the row happened to sit in. A member who moved between
--      houses mid-term (e.g. Seán Kyne, Seanad → Dáil 2026-05-25) gets one row
--      per house, each scored only against that house's divisions.
--   2. SERVICE-WINDOW DENOMINATOR — total_divisions is the number of divisions
--      held in that house-year WITHIN the member's membership window(s)
--      (member_terms.parquet start/end dates), so a mid-year arrival (Daniel
--      Ennis, TD from 2026-05-25) is no longer ranked against divisions held
--      before they had a seat, and a term-ended senator is not ranked against
--      divisions held after they left. Falls back to the full house-year count
--      only when the member has no term record for that house; GREATEST(...)
--      guards the denominator at >= voted_in so turnout can never exceed 100%.
--   3. DATE-BOUNDED OFFICE FLAGS — is_minister is true only when a dated
--      ministerial span (Wikidata tenure spine ∪ Oireachtas member-feed office
--      slots, which carry start AND end dates and include Ministers of State)
--      overlaps that row's year — not the point-in-time "holds office today"
--      snapshot, which was retroactively wrong in both directions (Donohoe
--      2025 false after resigning 2025-11-18; pre-office years true). is_chair
--      is date-bounded the same way from the office slots. is_leader stays the
--      curated point-in-time flag (the special-roles CSV has no dates — known
--      residual).
--
-- Office-holders are KEPT, not hidden: is_minister / is_chair / is_leader +
-- role_note let the UI context-flag a structurally-low voter (Ceann Comhairle
-- votes only to break ties; ministers are paired / on executive duty) rather
-- than shame them. CURRENT TERM ONLY (year >= 2025) — earlier years are
-- survivor-biased in the vote source and unsafe to rank.
CREATE OR REPLACE VIEW v_attendance_participation_turnout AS
WITH raw_votes AS (
    SELECT unique_member_code, full_name, party, vote_id, vote_url, CAST(date AS DATE) AS d
    FROM read_parquet('data/gold/parquet/current_dail_vote_history.parquet')
    UNION ALL
    SELECT unique_member_code, full_name, party, vote_id, vote_url, CAST(date AS DATE) AS d
    FROM read_parquet('data/gold/parquet/current_seanad_vote_history.parquet')
),
votes AS (
    -- house = the DIVISION's house, from its own URL — never the source file.
    SELECT
        unique_member_code, full_name, party, vote_id, d,
        YEAR(d) AS year,
        CASE WHEN vote_url LIKE '%/debates/vote/dail/%'   THEN 'Dáil'
             WHEN vote_url LIKE '%/debates/vote/seanad/%' THEN 'Seanad' END AS house
    FROM raw_votes
    WHERE unique_member_code IS NOT NULL AND full_name IS NOT NULL AND d IS NOT NULL
      AND YEAR(d) >= 2025  -- current term only (34th Dáil / 27th Seanad)
),
divisions AS (
    SELECT house, vote_id, MIN(d) AS d, YEAR(MIN(d)) AS year
    FROM votes
    WHERE house IS NOT NULL
    GROUP BY house, vote_id
),
totals AS (
    SELECT house, year, COUNT(*) AS total_divisions
    FROM divisions
    GROUP BY house, year
),
per AS (
    -- same grain as the old extractor output: (house, year, member, name, party)
    SELECT house, year, unique_member_code, full_name, party,
           COUNT(DISTINCT vote_id) AS voted_in
    FROM votes
    WHERE house IS NOT NULL
    GROUP BY house, year, unique_member_code, full_name, party
),
terms AS (
    SELECT DISTINCT
        unique_member_code,
        CASE WHEN lower(house) = 'dail' THEN 'Dáil' ELSE 'Seanad' END AS house,
        CAST(membership_start_date AS DATE)                            AS t_start,
        COALESCE(CAST(membership_end_date AS DATE), DATE '9999-12-31') AS t_end
    FROM read_parquet('data/silver/parquet/member_terms.parquet')
    WHERE membership_start_date IS NOT NULL
),
win AS (
    -- divisions held while the member actually held a seat in that house
    SELECT t.unique_member_code, dv.house, dv.year,
           COUNT(DISTINCT dv.vote_id) AS divisions_in_window
    FROM terms t
    JOIN divisions dv
      ON dv.house = t.house AND dv.d BETWEEN t.t_start AND t.t_end
    GROUP BY 1, 2, 3
),
-- Dated ministerial spans: Wikidata tenure spine (senior ministers, code-keyed)
-- ∪ member-feed office slots (start/end dates; includes Ministers of State,
-- Taoiseach, Tánaiste). Both are published records — no inference.
office_slots AS (
    SELECT unique_member_code, office_1_name AS office_name,
           TRY_CAST(office_1_start_date AS DATE) AS s, TRY_CAST(office_1_end_date AS DATE) AS e
    FROM read_parquet('data/silver/parquet/flattened_members.parquet') WHERE office_1_name IS NOT NULL
    UNION ALL
    SELECT unique_member_code, office_2_name,
           TRY_CAST(office_2_start_date AS DATE), TRY_CAST(office_2_end_date AS DATE)
    FROM read_parquet('data/silver/parquet/flattened_members.parquet') WHERE office_2_name IS NOT NULL
    UNION ALL
    SELECT unique_member_code, office_3_name,
           TRY_CAST(office_3_start_date AS DATE), TRY_CAST(office_3_end_date AS DATE)
    FROM read_parquet('data/silver/parquet/flattened_members.parquet') WHERE office_3_name IS NOT NULL
    UNION ALL
    SELECT unique_member_code, office_4_name,
           TRY_CAST(office_4_start_date AS DATE), TRY_CAST(office_4_end_date AS DATE)
    FROM read_parquet('data/silver/parquet/flattened_members.parquet') WHERE office_4_name IS NOT NULL
    UNION ALL
    SELECT unique_member_code, office_5_name,
           TRY_CAST(office_5_start_date AS DATE), TRY_CAST(office_5_end_date AS DATE)
    FROM read_parquet('data/silver/parquet/flattened_members.parquet') WHERE office_5_name IS NOT NULL
    UNION ALL
    SELECT unique_member_code, office_6_name,
           TRY_CAST(office_6_start_date AS DATE), TRY_CAST(office_6_end_date AS DATE)
    FROM read_parquet('data/silver/parquet/flattened_members.parquet') WHERE office_6_name IS NOT NULL
    UNION ALL
    SELECT unique_member_code, office_1_name,
           TRY_CAST(office_1_start_date AS DATE), TRY_CAST(office_1_end_date AS DATE)
    FROM read_parquet('data/silver/parquet/flattened_seanad_members.parquet') WHERE office_1_name IS NOT NULL
),
minister_spans AS (
    SELECT member_code AS unique_member_code,
           department_label AS label,
           CAST(start_date AS DATE) AS s,
           COALESCE(CAST(end_date AS DATE), DATE '9999-12-31') AS e
    FROM read_parquet('data/silver/ministerial_tenure.parquet')
    WHERE member_code IS NOT NULL AND start_date IS NOT NULL
    UNION ALL
    SELECT unique_member_code, office_name,
           s, COALESCE(e, DATE '9999-12-31')
    FROM office_slots
    WHERE s IS NOT NULL
      AND (office_name LIKE 'Minister%' OR office_name IN ('Taoiseach', 'Tánaiste'))
),
chair_spans AS (
    SELECT unique_member_code, office_name, s, COALESCE(e, DATE '9999-12-31') AS e
    FROM office_slots
    WHERE s IS NOT NULL
      AND office_name IN ('Ceann Comhairle', 'Leas-Cheann Comhairle', 'Cathaoirleach', 'Leas-Chathaoirleach')
),
years AS (SELECT DISTINCT year FROM totals),
flag_minister AS (
    SELECT ms.unique_member_code, y.year, MIN(ms.label) AS minister_label
    FROM minister_spans ms
    CROSS JOIN years y
    WHERE ms.s <= make_date(y.year, 12, 31) AND ms.e >= make_date(y.year, 1, 1)
    GROUP BY 1, 2
),
flag_chair AS (
    SELECT cs.unique_member_code, y.year, MIN(cs.office_name) AS chair_label
    FROM chair_spans cs
    CROSS JOIN years y
    WHERE cs.s <= make_date(y.year, 12, 31) AND cs.e >= make_date(y.year, 1, 1)
    GROUP BY 1, 2
),
cov_minister AS (SELECT DISTINCT unique_member_code FROM minister_spans),
cov_chair    AS (SELECT DISTINCT unique_member_code FROM chair_spans),
-- old extractor output — kept ONLY for display context that is not in the vote
-- facts (constituency, the curated party-leader flag/note) and as the flag
-- fallback for members with no dated span record at all.
old AS (
    SELECT house, year, unique_member_code, full_name, party,
           constituency, is_minister, is_chair, is_leader, role_note
    FROM read_parquet('data/gold/parquet/participation_member_year.parquet')
)
SELECT
    COALESCE(p.unique_member_code, '') AS unique_member_code,
    p.full_name                        AS member_name,
    COALESCE(p.party, '')              AS party_name,
    COALESCE(o.constituency, '')       AS constituency,
    p.house,
    CAST(p.year AS INTEGER)            AS year,
    p.voted_in,
    GREATEST(p.voted_in, COALESCE(w.divisions_in_window, t.total_divisions)) - p.voted_in AS missed,
    GREATEST(p.voted_in, COALESCE(w.divisions_in_window, t.total_divisions))              AS total_divisions,
    ROUND(100.0 * p.voted_in
          / GREATEST(p.voted_in, COALESCE(w.divisions_in_window, t.total_divisions)), 1)  AS turnout_pct,
    (CASE WHEN fm.unique_member_code IS NOT NULL THEN TRUE
          WHEN cm.unique_member_code IS NOT NULL THEN FALSE
          ELSE COALESCE(o.is_minister, FALSE) END) AS is_minister,
    (CASE WHEN fc.unique_member_code IS NOT NULL THEN TRUE
          WHEN cc.unique_member_code IS NOT NULL THEN FALSE
          ELSE COALESCE(o.is_chair, FALSE) END)    AS is_chair,
    COALESCE(o.is_leader, FALSE)       AS is_leader,
    (CASE WHEN (CASE WHEN fc.unique_member_code IS NOT NULL THEN TRUE
                     WHEN cc.unique_member_code IS NOT NULL THEN FALSE
                     ELSE COALESCE(o.is_chair, FALSE) END) THEN 'chair'
          WHEN (CASE WHEN fm.unique_member_code IS NOT NULL THEN TRUE
                     WHEN cm.unique_member_code IS NOT NULL THEN FALSE
                     ELSE COALESCE(o.is_minister, FALSE) END) THEN 'minister'
          WHEN COALESCE(o.is_leader, FALSE) THEN 'party_leader'
          ELSE '' END)                 AS role,
    COALESCE(NULLIF(o.role_note, ''), fc.chair_label, fm.minister_label, '') AS role_note
FROM per p
LEFT JOIN totals t ON t.house = p.house AND t.year = p.year
LEFT JOIN win w
       ON w.unique_member_code = p.unique_member_code AND w.house = p.house AND w.year = p.year
LEFT JOIN old o
       ON o.house = p.house AND o.year = p.year AND o.unique_member_code = p.unique_member_code
      AND o.full_name = p.full_name AND o.party IS NOT DISTINCT FROM p.party
LEFT JOIN flag_minister fm ON fm.unique_member_code = p.unique_member_code AND fm.year = p.year
LEFT JOIN flag_chair    fc ON fc.unique_member_code = p.unique_member_code AND fc.year = p.year
LEFT JOIN cov_minister  cm ON cm.unique_member_code = p.unique_member_code
LEFT JOIN cov_chair     cc ON cc.unique_member_code = p.unique_member_code;
