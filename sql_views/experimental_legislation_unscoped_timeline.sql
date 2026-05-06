-- ============================================================================
--  EXPERIMENTAL — DELETE ON INTEGRATION
-- ============================================================================
--  Sandbox stages timeline. Mirrors v_legislation_timeline column-for-column
--  but reads from pipeline_sandbox/out/silver/stages.parquet. See
--  experimental_legislation_unscoped_index.sql for the full removal checklist.
-- ============================================================================

CREATE OR REPLACE VIEW v_experimental_legislation_timeline AS
SELECT
    bill_year || '_' || bill_no                  AS bill_id,
    COALESCE("event.showAs", '—')                AS stage_name,
    TRY_CAST("event.dates"[1].date AS DATE)      AS stage_date,
    TRY_CAST("event.progressStage" AS INTEGER)   AS stage_number,
    COALESCE("event.stageCompleted", false)      AS is_current_stage,
    "event.house.showAs"                         AS chamber,
    bill_no, bill_year
FROM read_parquet('pipeline_sandbox/out/silver/stages.parquet');
