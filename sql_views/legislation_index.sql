-- v_legislation_index — one row per bill, primary sponsor only
-- Source: data/silver/parquet/sponsors.parquet
-- Deduplicates to one row per (bill_year, bill_no), preferring the primary sponsor.
--
-- TODO_GOVT_BILLS: Three changes needed when the unscoped legislation feed lands.
--   (a) WHERE sponsor_by_show_as IS NOT NULL drops every Government bill (those
--       carry sponsor in sponsor_as_show_as). Change to:
--         WHERE COALESCE(sponsor_by_show_as, sponsor_as_show_as) IS NOT NULL
--       and update the `sponsor` derivation to coalesce the same two columns.
--   (b) Add `source` and `origin_house` to the projection — currently neither
--       reaches the page, so no Govt-vs-PM filter is possible at the index level.
--   (c) The bill_phase CASE assumes Dáil-origin. Make origin-aware: a Seanad-
--       origin bill at stage 2 should land in 'seanad', not 'dail'. Use
--       `origin_house ILIKE '%Seanad%'` (values are 'Seanad Éireann' / 'Dáil Éireann').
-- Full plan + verified sandbox impl:
--   pipeline_sandbox/legislation_unscoped_integration_plan.md §2a
--   pipeline_sandbox/legislation_unscoped_silver_views.py

CREATE OR REPLACE VIEW v_legislation_index AS
WITH ranked AS (
    SELECT
        bill_year || '_' || bill_no                      AS bill_id,
        COALESCE(short_title_en, '(Untitled)')           AS bill_title,
        COALESCE(status, '—')                            AS bill_status,
        COALESCE(bill_type, source, '—')                 AS bill_type,
        COALESCE(sponsor_by_show_as, '—')                AS sponsor,
        TRY_CAST(context_date AS DATE)                   AS introduced_date,
        most_recent_stage_event_show_as                  AS current_stage,
        TRY_CAST(most_recent_stage_event_progress_stage AS INTEGER) AS stage_number,
        bill_url                                         AS oireachtas_url,
        bill_no,
        bill_year,
        ROW_NUMBER() OVER (
            PARTITION BY bill_year, bill_no
            ORDER BY CASE WHEN sponsor_is_primary = true THEN 0 ELSE 1 END
        ) AS rn
    FROM read_parquet('data/silver/parquet/sponsors.parquet')
    WHERE COALESCE(sponsor_by_show_as, sponsor_as_show_as) IS NOT NULL
)
SELECT
    bill_id,
    bill_title,
    bill_status,
    bill_type,
    sponsor,
    source,
    origin_house,
    introduced_date,
    current_stage,
    stage_number,
    oireachtas_url,
    bill_no,
    bill_year,
    CASE
        WHEN LOWER(bill_status) LIKE '%enact%'
          OR LOWER(bill_status) LIKE '%sign%'
          OR COALESCE(stage_number, 0) >= 11
        THEN 'enacted'
        WHEN COALESCE(stage_number, 0) >= 6 THEN 'seanad'
        WHEN origin_house ILIKE '%Seanad%' AND stage_number < 6 THEN 'seanad'
        WHEN origin_house ILIKE '%Seanad%' AND stage_number >= 6 THEN 'dail'
        ELSE 'dail'
    END AS bill_phase
FROM ranked
WHERE rn = 1
ORDER BY introduced_date DESC NULLS LAST;
