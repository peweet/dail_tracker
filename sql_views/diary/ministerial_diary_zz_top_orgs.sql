-- v_ministerial_diary_top_orgs — the "Most-met · A · B · C" card context: for
-- each minister and each department, the organisations most often named in
-- their engagement subjects, ranked within every period grain the page's
-- Year/Month filter can ask for. Graduated out of
-- utility/pages_code/ministerial_diaries.py::_top_orgs (logic-firewall audit
-- 2026-07-16).
--
-- Source is v_ministerial_diary_engagements (entry × matched-org grain, travel/
-- media excluded) — the ~quarter of meetings whose counterparty matched the
-- gazetteer, so this is CONTEXT for a card strip, not a ranked influence
-- metric (the page copy frames it that way).
--
-- Period-grain encoding (shared by all the ministerial_diary_zz_* rollups):
--   period_grain = 'all'   → whole corpus      (period_year, period_month NULL)
--   period_grain = 'year'  → one year          (period_month NULL)
--   period_grain = 'month' → one year + month
--
-- rnk = 1-based rank of the org within (entity_kind, entity, period), most
-- mentions first, name-alphabetical tiebreak — retrieval slices "top N" with a
-- WHERE rnk <= N, no page-side aggregation.
--
-- Grain: entity_kind ('minister' | 'department') × entity × organisation × period.
-- Depends on v_ministerial_diary_engagements — the zz_ filename keeps it
-- loading after ministerial_diary_engagements.sql within the
-- ministerial_diary_*.sql glob.
CREATE OR REPLACE VIEW v_ministerial_diary_top_orgs AS
WITH base AS (
    SELECT
        organisation,
        minister,
        department,
        CAST(EXTRACT(year FROM entry_date) AS INTEGER)  AS period_year,
        CAST(EXTRACT(month FROM entry_date) AS INTEGER) AS period_month
    FROM v_ministerial_diary_engagements
    WHERE organisation IS NOT NULL AND organisation <> ''
),
counts AS (
    SELECT
        'minister' AS entity_kind,
        minister   AS entity,
        organisation,
        CASE
            WHEN GROUPING(period_year) = 1  THEN 'all'
            WHEN GROUPING(period_month) = 1 THEN 'year'
            ELSE 'month'
        END        AS period_grain,
        period_year,
        period_month,
        COUNT(*)   AS n
    FROM base
    WHERE minister IS NOT NULL AND minister <> ''
    GROUP BY GROUPING SETS (
        (minister, organisation),
        (minister, organisation, period_year),
        (minister, organisation, period_year, period_month)
    )
    UNION ALL
    SELECT
        'department' AS entity_kind,
        department   AS entity,
        organisation,
        CASE
            WHEN GROUPING(period_year) = 1  THEN 'all'
            WHEN GROUPING(period_month) = 1 THEN 'year'
            ELSE 'month'
        END          AS period_grain,
        period_year,
        period_month,
        COUNT(*)     AS n
    FROM base
    WHERE department IS NOT NULL AND department <> ''
    GROUP BY GROUPING SETS (
        (department, organisation),
        (department, organisation, period_year),
        (department, organisation, period_year, period_month)
    )
)
SELECT
    entity_kind,
    entity,
    organisation,
    period_grain,
    period_year,
    period_month,
    n,
    row_number() OVER (
        PARTITION BY entity_kind, entity, period_grain, period_year, period_month
        ORDER BY n DESC, organisation
    ) AS rnk
FROM counts;
