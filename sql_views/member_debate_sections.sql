-- v_member_debate_sections — debate sections in which a TD raised a question.
-- Source: data/silver/parquet/questions.parquet
--
-- Grain: one row per (unique_member_code, debate_section_id, debate_date,
-- topic). Several questions by the same TD in the same section on the same
-- topic collapse into one row, counted in question_count.
--
-- SCOPE: this records "the TD raised a parliamentary question in this debate
-- section", NOT "the TD spoke in this debate". True floor-speech attribution
-- needs the AKN-XML speech layer (debates Stage 2). For oral / Topical-Issue
-- questions the asking TD did speak; for written answers they did not.
--
-- oireachtas_url points to the public per-question page
--   https://www.oireachtas.ie/en/debates/question/<date>/<question_number>/
-- verified to resolve for BOTH oral and written questions. The earlier
-- /debates/debate/<chamber>/<date>/<n>/ form only worked for floor debate
-- sections — written-answer sections (the bulk of questions) returned an
-- empty shell page. The per-question route is correct for every row.
-- The card is one (member, section, topic, date) group; any_value() picks
-- a representative question for the link.

CREATE OR REPLACE VIEW v_member_debate_sections AS
SELECT
    unique_member_code,
    any_value(td_name)                                   AS td_name,
    CAST(SUBSTR(context_date, 1, 4) AS INTEGER)          AS debate_year,
    debate_section_id,
    context_date                                         AS debate_date,
    "question.house.houseCode"                           AS chamber,
    topic,
    COUNT(*)                                             AS question_count,
    'https://www.oireachtas.ie/en/debates/question/'
        || context_date || '/'
        || CAST(any_value(question_number) AS VARCHAR) || '/'  AS oireachtas_url
FROM read_parquet('data/silver/parquet/questions.parquet')
WHERE debate_section_id IS NOT NULL
  AND context_date IS NOT NULL
  AND question_number IS NOT NULL
  AND "question.house.houseCode" IS NOT NULL
GROUP BY
    unique_member_code,
    CAST(SUBSTR(context_date, 1, 4) AS INTEGER),
    debate_section_id,
    context_date,
    "question.house.houseCode",
    topic;
