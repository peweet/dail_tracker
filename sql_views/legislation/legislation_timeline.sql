-- v_legislation_timeline — stage progression rows for the bill detail timeline
-- Source: data/silver/parquet/stages.parquet
-- event.dates is STRUCT(date VARCHAR)[] — first element extracted for stage_date.

CREATE OR REPLACE VIEW v_legislation_timeline AS
SELECT
    bill_year || '_' || bill_no                             AS bill_id,
    COALESCE("event.showAs", '—')                           AS stage_name,
    TRY_CAST("event.dates"[1].date AS DATE)                 AS stage_date,
    TRY_CAST("event.progressStage" AS INTEGER)              AS stage_number,
    COALESCE("event.stageCompleted", false)                 AS is_current_stage,
    "event.house.showAs"                                    AS chamber,
    bill_no,
    bill_year
FROM read_parquet('data/silver/parquet/stages.parquet')
ORDER BY bill_year, bill_no, stage_number ASC NULLS LAST, stage_date ASC NULLS LAST;
