-- v_legislation_detail — full bill record for the Stage 2 drilldown panel
-- Source: data/silver/parquet/sponsors.parquet
-- One row per bill_id; all available metadata columns included.
--
-- TODO_GOVT_BILLS: Mirror the index-view fix — replace
--     WHERE sponsor_by_show_as IS NOT NULL
-- with
--     WHERE COALESCE(sponsor_by_show_as, sponsor_as_show_as) IS NOT NULL
-- and coalesce both columns in the `sponsor` derivation. Without this, the
-- index ↔ detail drift filter in legislation_data.fetch_bill_detail returns
-- empty for every Government bill the user clicks.
-- See pipeline_sandbox/legislation_unscoped_integration_plan.md §2a.

CREATE OR REPLACE VIEW v_legislation_detail AS
WITH ranked AS (
    SELECT
        bill_year || '_' || bill_no                      AS bill_id,
        COALESCE(short_title_en, '(Untitled)')           AS bill_title,
        COALESCE(long_title_en, '')                      AS long_title,
        COALESCE(status, '—')                            AS bill_status,
        COALESCE(bill_type, source, '—')                 AS bill_type,
        COALESCE(sponsor_by_show_as, '—')                AS sponsor,
        unique_member_code,
        TRY_CAST(context_date AS DATE)                   AS introduced_date,
        last_updated,
        source,
        method,
        most_recent_stage_event_show_as                  AS current_stage,
        most_recent_stage_event_house_show_as            AS current_house,
        most_recent_stage_event_stage_completed          AS stage_completed,
        bill_url                                         AS oireachtas_url,
        bill_no,
        bill_year,
        ROW_NUMBER() OVER (
            PARTITION BY bill_year, bill_no
            ORDER BY CASE WHEN sponsor_is_primary = true THEN 0 ELSE 1 END
        ) AS rn
    FROM read_parquet('data/silver/parquet/sponsors.parquet')
    WHERE sponsor_by_show_as IS NOT NULL
)
SELECT
    bill_id,
    bill_title,
    long_title,
    bill_status,
    bill_type,
    sponsor,
    unique_member_code,
    introduced_date,
    last_updated,
    source,
    method,
    current_stage,
    current_house,
    stage_completed,
    oireachtas_url,
    bill_no,
    bill_year
FROM ranked
WHERE rn = 1;
