-- v_member_questions — feed of every parliamentary question per TD.
--
-- Grain: one row per question (no aggregation). The member-overview page
-- filters by unique_member_code, optionally by year / type / ministry,
-- then paginates the feed.
--
-- Source: data/silver/parquet/questions.parquet (120k → 264k rows after the
-- May 2026 paginate-on-skip backfill that lifted the 1000-row per-TD API
-- cap; coverage now 2020-03-05 to current; complete history per TD).
--
-- oireachtas_url is the public per-question page. Same construction as
-- v_member_debate_sections — verified to resolve for BOTH oral and written
-- questions. The /question/ route works where /debate/ only worked for
-- floor sections.
--
-- question_ref (e.g. "28800/26") is the Oireachtas reference number,
-- extracted upstream by questions.py from a bracket in the question_text
-- body. Useful as a quiet right-aligned identifier in the UI card.

CREATE OR REPLACE VIEW v_member_questions AS
SELECT
    unique_member_code,
    td_name,
    question_date,
    year                                                 AS question_year,
    question_type,            -- 'written' | 'oral'
    ministry,
    topic,
    question_text,
    question_number,
    question_ref,
    debate_section_id,
    'https://www.oireachtas.ie/en/debates/question/'
        || context_date || '/'
        || CAST(question_number AS VARCHAR) || '/'       AS oireachtas_url
FROM read_parquet('data/silver/parquet/questions.parquet')
WHERE unique_member_code IS NOT NULL;
