-- v_member_question_top_topics — per-TD topic counts, ordered by frequency.
-- Powers the "top 3 topics" pill row on /member-overview profile.
-- One row per (unique_member_code, topic). The "top 3" limit is applied
-- in retrieval SQL with ORDER BY n DESC LIMIT 3.
--
-- Source: v_member_questions (already registered).
-- File name starts with 'member_' so member_overview_data.py picks it up.

CREATE OR REPLACE VIEW v_member_question_top_topics AS
SELECT unique_member_code,
       topic,
       COUNT(*) AS n
FROM v_member_questions
WHERE topic IS NOT NULL AND topic <> ''
GROUP BY unique_member_code, topic;
