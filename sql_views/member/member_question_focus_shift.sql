-- v_member_question_focus_shift — TDs whose top-questioned ministry
-- differs between the 33rd-Dáil-era window and the 34th-Dáil window.
--
-- Grain: one row per unique_member_code, ONLY for TDs where a shift is
-- visible AND both windows have enough questions to be meaningful. The
-- member-overview page renders an inline italics subtitle below the
-- concentration pill when this view returns a row for the TD.
--
-- Window boundary: 2024-11-29 (the 34th Dáil sitting day). Chosen so
-- "past" captures the previous-Dáil brief and "recent" captures the
-- current brief. Minimum 30 questions in each window — below that the
-- top-ministry pick is too noisy to claim a "shift" honestly.
--
-- Returns 28 TDs in the May 2026 data snapshot (16% of the Dáil). Example
-- rows surface real brief changes: Gary Gannon Education → Justice, Alan
-- Kelly Health → Justice, Ivana Bacik Housing → Taoiseach (Leaders'
-- Questions transition), Aengus Ó Snodaigh Tourism → Education.

CREATE OR REPLACE VIEW v_member_question_focus_shift AS
WITH base AS (
    SELECT
        unique_member_code,
        ministry,
        question_date,
        CASE
            WHEN question_date >= TIMESTAMP '2024-11-29 00:00:00' THEN 'recent'
            ELSE 'past'
        END AS window_bucket
    FROM read_parquet('data/silver/parquet/questions.parquet')
    WHERE unique_member_code IS NOT NULL
      AND ministry IS NOT NULL
),
top_per_window AS (
    SELECT
        unique_member_code,
        window_bucket,
        ministry,
        COUNT(*) AS n,
        MIN(question_date) AS window_min_date,
        MAX(question_date) AS window_max_date
    FROM base
    GROUP BY unique_member_code, window_bucket, ministry
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY unique_member_code, window_bucket
        ORDER BY COUNT(*) DESC, ministry ASC
    ) = 1
)
SELECT
    p.unique_member_code,
    p.ministry      AS past_top,
    p.n             AS past_n,
    CAST(EXTRACT(YEAR FROM p.window_min_date) AS INTEGER) AS past_year_min,
    CAST(EXTRACT(YEAR FROM p.window_max_date) AS INTEGER) AS past_year_max,
    r.ministry      AS recent_top,
    r.n             AS recent_n,
    CAST(EXTRACT(YEAR FROM r.window_min_date) AS INTEGER) AS recent_year_min,
    CAST(EXTRACT(YEAR FROM r.window_max_date) AS INTEGER) AS recent_year_max
FROM top_per_window p
JOIN top_per_window r USING (unique_member_code)
WHERE p.window_bucket = 'past'
  AND r.window_bucket = 'recent'
  AND p.ministry <> r.ministry
  AND p.n >= 30
  AND r.n >= 30;
