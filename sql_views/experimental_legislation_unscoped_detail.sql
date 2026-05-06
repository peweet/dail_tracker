-- ============================================================================
--  EXPERIMENTAL — DELETE ON INTEGRATION
-- ============================================================================
--  Companion to experimental_legislation_unscoped_index.sql. Same source,
--  detail-level projection. See sibling file for the full integration
--  removal checklist.
-- ============================================================================

CREATE OR REPLACE VIEW v_experimental_legislation_detail AS
WITH ranked AS (
    SELECT
        bill_year || '_' || bill_no                               AS bill_id,
        COALESCE(short_title_en, '(Untitled)')                    AS bill_title,
        COALESCE(long_title_en, '')                               AS long_title,
        COALESCE(status, '—')                                     AS bill_status,
        COALESCE(bill_type, source, '—')                          AS bill_type,
        COALESCE(sponsor_by_show_as, sponsor_as_show_as, '—')     AS sponsor,
        unique_member_code,
        TRY_CAST(context_date AS DATE)                            AS introduced_date,
        last_updated,
        source,
        origin_house,
        method,
        most_recent_stage_event_show_as                           AS current_stage,
        most_recent_stage_event_house_show_as                     AS current_house,
        most_recent_stage_event_stage_completed                   AS stage_completed,
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
SELECT * EXCLUDE rn FROM ranked WHERE rn = 1;
