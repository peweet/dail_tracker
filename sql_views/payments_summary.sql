-- v_payments_summary — dataset-level summary statistics
-- Depends on: v_payments_base

CREATE OR REPLACE VIEW v_payments_summary AS
SELECT
    COUNT(DISTINCT member_name) AS members_count,
    COUNT(*)                    AS payment_count,
    SUM(amount_num)             AS total_paid,
    MIN(date_paid)              AS first_payment_date,
    MAX(date_paid)              AS last_payment_date,
    MIN(payment_year)           AS first_year,
    MAX(payment_year)           AS last_year,
    -- TODO_PIPELINE_VIEW_REQUIRED: source_summary, latest_fetch_timestamp_utc, mart_version, code_version
    'Oireachtas Payment Records (aggregated_payment_tables.csv)' AS source_summary,
    NULL::VARCHAR               AS latest_fetch_timestamp_utc,
    NULL::VARCHAR               AS mart_version,
    NULL::VARCHAR               AS code_version
FROM v_payments_base;
