-- v_legislation_index — one row per bill, primary sponsor only
-- Source: data/silver/parquet/sponsors.parquet
-- Deduplicates to one row per (bill_year, bill_no), preferring the primary sponsor.
--
CREATE OR REPLACE VIEW v_legislation_index AS
WITH ranked AS (
    SELECT
        bill_year || '_' || bill_no                      AS bill_id,
        COALESCE(short_title_en, '(Untitled)')           AS bill_title,
        COALESCE(status, '—')                            AS bill_status,
        COALESCE(bill_type, source, '—')                 AS bill_type,
        -- fall back to sponsor_as_show_as like v_legislation_detail does — 557 bills
        -- have only that field populated and rendered '—' when it was skipped
        COALESCE(sponsor_by_show_as, sponsor_as_show_as, '—') AS sponsor,
        unique_member_code                               AS sponsor_join_key,
        TRY_CAST(context_date AS DATE)                   AS introduced_date,
        most_recent_stage_event_show_as                  AS current_stage,
        TRY_CAST(most_recent_stage_event_progress_stage AS INTEGER) AS stage_number,
        bill_url                                         AS oireachtas_url,
        bill_no,
        bill_year,
        source,
        origin_house,
        ROW_NUMBER() OVER (
            PARTITION BY bill_year, bill_no
            ORDER BY
                CASE WHEN sponsor_is_primary = true THEN 0 ELSE 1 END,
                COALESCE(sponsor_by_show_as, ''),
                COALESCE(unique_member_code, '')
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
    sponsor_join_key,
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
        WHEN origin_house ILIKE '%Seanad%' AND COALESCE(stage_number, 0) < 6 THEN 'seanad'
        WHEN origin_house ILIKE '%Seanad%' AND COALESCE(stage_number, 0) >= 6 THEN 'dail'
        ELSE 'dail'
    END AS bill_phase
FROM ranked
WHERE rn = 1
ORDER BY introduced_date DESC NULLS LAST;
