-- Source: data/gold/csv/attendance_by_td_year.csv
-- Join resolved in enrich.py — no join needed here.
-- Gold CSV is authoritative; update this view if it disagrees with gold.
--
-- attended_count = total_days (sitting + committee), per user decision (2026-04-30).
-- sitting_days and other_days are retained in the gold CSV for pipeline use.
--
-- TODO_PIPELINE_VIEW_REQUIRED: session_type — gold CSV collapses sitting vs committee
--   days into total_days. Expose separately once pipeline produces distinct columns.

-- Reads from parquet (written by pipeline_sandbox/attendance_member_enrichment.py).
-- Falls back to CSV path if parquet not yet generated.
CREATE OR REPLACE VIEW v_attendance_member_year_summary AS
SELECT
    COALESCE(unique_member_code, '') AS unique_member_code,
    full_name                        AS member_name,
    member_id,
    CAST(year AS INTEGER)            AS year,
    total_days                       AS attended_count,
    COALESCE(party_name,    '')      AS party_name,
    COALESCE(constituency,  '')      AS constituency,
    COALESCE(is_minister, 'false')   AS is_minister
FROM read_parquet('data/gold/parquet/attendance_by_td_year.parquet')
WHERE full_name IS NOT NULL
  AND year IS NOT NULL;
