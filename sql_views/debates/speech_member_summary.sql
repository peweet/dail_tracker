-- v_member_speech_summary — one row per member: the header-strip aggregates for
-- the Debates section (total contributions, words, as-Gaeilge count, breadth,
-- by-type and Commencement-Matters counts, active span). Pipeline-owned
-- aggregation (GROUP BY) so the UI does retrieval-only SELECT … WHERE.

CREATE OR REPLACE VIEW v_member_speech_summary AS
SELECT
    unique_member_code,
    any_value(member_name)                                          AS member_name,
    any_value(house)                                                AS house,
    COUNT(*)                                                        AS total_contributions,
    COALESCE(SUM(word_count), 0)                                    AS total_words,
    SUM(CASE WHEN is_irish THEN 1 ELSE 0 END)                       AS irish_count,
    COUNT(DISTINCT section_heading)                                 AS distinct_topics,
    COUNT(DISTINCT business)                                        AS distinct_business,
    SUM(CASE WHEN contribution_type = 'speech' THEN 1 ELSE 0 END)   AS speech_count,
    SUM(CASE WHEN contribution_type = 'question' THEN 1 ELSE 0 END) AS question_count,
    SUM(CASE WHEN contribution_type = 'answer' THEN 1 ELSE 0 END)   AS answer_count,
    SUM(CASE WHEN business ILIKE '%Commencement Matter%' THEN 1 ELSE 0 END) AS commencement_count,
    MIN(CAST("date" AS VARCHAR))                                    AS first_date,
    MAX(CAST("date" AS VARCHAR))                                    AS last_date
FROM v_speech_base
WHERE unique_member_code IS NOT NULL
GROUP BY unique_member_code;
