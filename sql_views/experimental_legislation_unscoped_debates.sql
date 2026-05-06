-- ============================================================================
--  EXPERIMENTAL — DELETE ON INTEGRATION
-- ============================================================================
--  Sandbox debates view. Reads pipeline_sandbox/out/silver/debates.parquet.
--  See experimental_legislation_unscoped_index.sql for the full removal
--  checklist.
-- ============================================================================

CREATE OR REPLACE VIEW v_experimental_legislation_debates AS
SELECT
    "bill.billYear" || '_' || "bill.billNo"  AS bill_id,
    TRY_CAST(date AS DATE)                   AS debate_date,
    showAs                                   AS debate_title,
    "chamber.showAs"                         AS chamber,
    "bill.billNo"                            AS bill_no,
    "bill.billYear"                          AS bill_year
FROM read_parquet('pipeline_sandbox/out/silver/debates.parquet')
WHERE "bill.billNo" IS NOT NULL AND "bill.billYear" IS NOT NULL;
