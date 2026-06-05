-- v_member_ministerial_tenure — "who ran each department, and when".
-- Source: data/silver/ministerial_tenure.parquet (Wikidata-derived, ~2011→present;
--   built by wikidata/ministerial_tenure_build.py).
--
-- Grain: one row per (department, minister, tenure span). end_date NULL = the
-- minister is currently in post.
--
-- unique_member_code is the Oireachtas member code ONLY where the minister is a
-- CURRENT member of the registry (~52%); historical ministers predate the current
-- member spine and have no code. minister_name is ALWAYS present, so display never
-- depends on the join — the code is a bonus that lets the UI link a clickable
-- profile when available. wikidata_person / wikidata_position are stable QIDs for
-- external linkage and de-duplication.
--
-- This is the accountability spine: join member_code to votes/payments/questions,
-- or use it to answer "who held department X on date D" (see
-- dail_tracker_core/queries/ministerial.minister_on_date).

CREATE OR REPLACE VIEW v_member_ministerial_tenure AS
SELECT
    department_key,
    department_label,
    minister_name,
    member_code                                       AS unique_member_code,
    CAST(start_date AS DATE)                          AS start_date,
    CAST(end_date   AS DATE)                          AS end_date,
    (end_date IS NULL)                                AS is_current,
    -- Days in post; for a sitting minister this counts to today (CURRENT_DATE is
    -- evaluated at query time, so the figure stays live without a rebuild).
    DATE_DIFF('day', CAST(start_date AS DATE),
              COALESCE(CAST(end_date AS DATE), CURRENT_DATE)) AS tenure_days,
    wikidata_person,
    wikidata_position
FROM read_parquet('data/silver/ministerial_tenure.parquet')
WHERE start_date IS NOT NULL
-- Source dedup: Wikidata occasionally records the SAME post twice with restated
-- start dates (e.g. a mid-term re-appointment to a department already held, both
-- left open-ended). Collapse to one row per (minister, department, end_date),
-- keeping the EARLIEST start — the appointment date. This is cleaning a
-- duplicated record, not inferring a fact: the held-since date is the first one.
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY minister_name, department_label, end_date
    ORDER BY start_date
) = 1
ORDER BY department_label, start_date DESC;
