-- ============================================================================
--  EXPERIMENTAL — DELETE ON INTEGRATION
-- ============================================================================
--  This view is a sandbox preview of the unscoped legislation feed
--  (Government + Private Member bills). It reads from the SANDBOX silver at
--  pipeline_sandbox/out/silver/sponsors.parquet (produced by
--  pipeline_sandbox/legislation_unscoped_silver_views.py) — NOT from the
--  production silver at data/silver/parquet/sponsors.parquet.
--
--  REMOVAL CHECKLIST when graduating to production:
--    1. Apply the §2a fixes inline to:
--         sql_views/legislation_index.sql
--         sql_views/legislation_detail.sql
--       (sponsor coalesce, source/origin_house projection, origin-aware
--        bill_phase). See pipeline_sandbox/legislation_unscoped_integration_plan.md.
--    2. Add `originHouse` to BILL_META in legislation.py and re-run the
--       silver build so production silver/sponsors.parquet carries it.
--    3. Switch production legislation fetch from per-TD member_id to the
--       unscoped feed (services/urls.py + legislation.py).
--    4. Delete this file and the four sibling experimental_legislation_*.sql.
--    5. Remove the experimental preview page from utility/app.py and
--       delete utility/pages_code/experimental_preview.py +
--       utility/data_access/experimental_data.py.
--
--  This file matches `experimental_*.sql` and is therefore NOT loaded by
--  the production legislation_data.get_legislation_conn() loader (which
--  globs `legislation_*.sql`). It is loaded only by the experimental
--  data access layer in utility/data_access/experimental_data.py.
-- ============================================================================

CREATE OR REPLACE VIEW v_experimental_legislation_index AS
WITH ranked AS (
    SELECT
        bill_year || '_' || bill_no                               AS bill_id,
        COALESCE(short_title_en, '(Untitled)')                    AS bill_title,
        COALESCE(status, '—')                                     AS bill_status,
        COALESCE(bill_type, source, '—')                          AS bill_type,
        source,
        origin_house,
        COALESCE(sponsor_by_show_as, sponsor_as_show_as, '—')     AS sponsor,
        TRY_CAST(context_date AS DATE)                            AS introduced_date,
        most_recent_stage_event_show_as                           AS current_stage,
        TRY_CAST(most_recent_stage_event_progress_stage AS INTEGER) AS stage_number,
        bill_url                                                  AS oireachtas_url,
        bill_no,
        bill_year,
        ROW_NUMBER() OVER (
            PARTITION BY bill_year, bill_no
            ORDER BY CASE WHEN sponsor_is_primary = true THEN 0 ELSE 1 END
        ) AS rn
    FROM read_parquet('pipeline_sandbox/out/silver/sponsors.parquet')
    WHERE COALESCE(sponsor_by_show_as, sponsor_as_show_as) IS NOT NULL
)
SELECT
    bill_id, bill_title, bill_status, bill_type,
    source, origin_house,
    sponsor, introduced_date, current_stage, stage_number,
    oireachtas_url, bill_no, bill_year,
    CASE
        WHEN LOWER(bill_status) LIKE '%enact%'
          OR LOWER(bill_status) LIKE '%sign%'
          OR COALESCE(stage_number, 0) >= 11
        THEN 'enacted'
        WHEN origin_house ILIKE '%Seanad%' AND COALESCE(stage_number, 0) < 6
        THEN 'seanad'
        WHEN origin_house ILIKE '%Seanad%' AND COALESCE(stage_number, 0) >= 6
        THEN 'dail'
        WHEN COALESCE(stage_number, 0) >= 6 THEN 'seanad'
        ELSE 'dail'
    END AS bill_phase
FROM ranked
WHERE rn = 1
ORDER BY introduced_date DESC NULLS LAST;
