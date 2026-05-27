-- v_member_question_ministries — per-TD ministry list ordered by frequency.
-- Powers the ministry selectbox on /member-overview question feed.
-- One row per (unique_member_code, ministry).
--
-- Source: v_member_questions (already registered).
-- File name starts with 'member_' so member_overview_data.py picks it up.

CREATE OR REPLACE VIEW v_member_question_ministries AS
SELECT unique_member_code,
       ministry,
       COUNT(*) AS n
FROM v_member_questions
WHERE ministry IS NOT NULL
GROUP BY unique_member_code, ministry;
