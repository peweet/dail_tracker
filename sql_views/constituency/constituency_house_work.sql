-- v_constituency_house_work — one row per constituency: a compact summary of the
-- DÁIL WORK done by that constituency's CURRENT TDs since the 2024 general election.
--
-- Every metric is aggregated from the existing per-member activity views, joined to
-- the current Dáil roster (v_member_registry, house='Dáil') on the member code — so
-- the figures are tied to exactly the TDs the dossier's roster shows, and exclude any
-- prior-Dáil activity by a TD who also sat before. Date-scoped to the 34th Dáil
-- (polling day 29 Nov 2024). Attendance is intentionally NOT summarised here: its
-- per-member columns are not a clean day-attendance rate, so it stays on each TD's
-- member-overview profile rather than risk a misleading constituency figure.
--
-- HONESTY: this is "work by this constituency's TDs", an aggregate of individual
-- records — not a boundary-stable historical series. Counts are activity volume, not
-- a quality judgement (no inference — see feedback_no_inference_in_app). Interests use
-- ONLY the landlord/property flags (the reliable signals per the interests register);
-- they count CURRENT TDs who declared, at the latest declaration year.
CREATE OR REPLACE VIEW v_constituency_house_work AS
WITH tds AS (
    SELECT constituency, unique_member_code
    FROM v_member_registry
    WHERE house = 'Dáil' AND constituency IS NOT NULL
),
q AS (
    SELECT t.constituency, COUNT(*) AS n_questions
    FROM tds t JOIN v_member_questions x ON x.unique_member_code = t.unique_member_code
    WHERE TRY_CAST(x.question_date AS DATE) >= DATE '2024-11-29'
    GROUP BY t.constituency
),
sp AS (
    SELECT t.constituency, COUNT(*) AS n_speeches, COALESCE(SUM(x.word_count), 0) AS n_words
    FROM tds t JOIN v_member_speeches x ON x.unique_member_code = t.unique_member_code
    WHERE TRY_CAST(x.speech_date AS DATE) >= DATE '2024-11-29'
    GROUP BY t.constituency
),
v AS (
    SELECT t.constituency, COUNT(*) AS n_votes_cast
    FROM tds t JOIN v_vote_member_detail x ON x.member_id = t.unique_member_code
    WHERE TRY_CAST(x.vote_date AS DATE) >= DATE '2024-11-29'
    GROUP BY t.constituency
),
intr AS (
    SELECT t.constituency,
           COUNT(DISTINCT CASE WHEN x.landlord_flag THEN x.member_id END) AS n_landlords,
           COUNT(DISTINCT CASE WHEN x.property_flag THEN x.member_id END) AS n_property_owners
    FROM tds t JOIN v_member_interests_detail x ON x.member_id = t.unique_member_code
    WHERE x.declaration_year = (SELECT MAX(declaration_year) FROM v_member_interests_detail)
    GROUP BY t.constituency
)
SELECT
    c.constituency_name,
    COALESCE(q.n_questions, 0)        AS n_questions,
    COALESCE(sp.n_speeches, 0)        AS n_speeches,
    COALESCE(sp.n_words, 0)           AS n_words,
    COALESCE(v.n_votes_cast, 0)       AS n_votes_cast,
    COALESCE(intr.n_landlords, 0)     AS n_landlords,
    COALESCE(intr.n_property_owners, 0) AS n_property_owners
FROM (SELECT DISTINCT constituency AS constituency_name FROM tds) c
LEFT JOIN q    ON q.constituency = c.constituency_name
LEFT JOIN sp   ON sp.constituency = c.constituency_name
LEFT JOIN v    ON v.constituency = c.constituency_name
LEFT JOIN intr ON intr.constituency = c.constituency_name
ORDER BY c.constituency_name;
