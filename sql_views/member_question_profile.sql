-- v_member_question_profile — aggregate question signals per TD.
--
-- Grain: one row per unique_member_code.
-- Drives the compact header strip on the Questions section:
--   total_qs                — "7,052 questions on file"
--   distinct_ministries     — informs the constituency-generalist label
--   top_ministry, top_count — "Most-questioned ministry: Health (5,957)"
--   top_pct                 — "(84.5% of all questions)"
--
-- The UI rule of thumb: render top_pct only when total_qs >= 100. Below
-- that threshold the percentage is too unstable to be journalistically
-- meaningful, and the header strip falls back to showing total_qs alone
-- with a "recently elected" framing.

CREATE OR REPLACE VIEW v_member_question_profile AS
WITH base AS (
    SELECT unique_member_code, ministry
    FROM read_parquet('data/silver/parquet/questions.parquet')
    WHERE unique_member_code IS NOT NULL
      AND ministry IS NOT NULL
),
totals AS (
    SELECT
        unique_member_code,
        COUNT(*)                       AS total_qs,
        COUNT(DISTINCT ministry)       AS distinct_ministries
    FROM base
    GROUP BY unique_member_code
),
top_min AS (
    -- One row per (member, ministry); QUALIFY keeps only the member's
    -- top ministry (ties broken alphabetically by ministry for determinism).
    SELECT
        unique_member_code,
        ministry                       AS top_ministry,
        COUNT(*)                       AS top_count
    FROM base
    GROUP BY unique_member_code, ministry
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY unique_member_code
        ORDER BY COUNT(*) DESC, ministry ASC
    ) = 1
)
SELECT
    t.unique_member_code,
    t.total_qs,
    t.distinct_ministries,
    tm.top_ministry,
    tm.top_count,
    ROUND(100.0 * tm.top_count / NULLIF(t.total_qs, 0), 1) AS top_pct
FROM totals t
LEFT JOIN top_min tm USING (unique_member_code);
