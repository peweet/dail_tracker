-- Chief Executive monthly reports are officer reports, not council minutes.
-- These views expose document coverage and source-reviewed forward-work signals
-- without publishing the unreviewed classifier queue as fact.

CREATE OR REPLACE VIEW v_la_ce_report_documents AS
SELECT
    document_id,
    CASE
        WHEN council = 'Dún Laoghaire-Rathdown' THEN 'Dun Laoghaire-Rathdown'
        ELSE council
    END AS local_authority,
    any_value(report_title) AS report_title,
    any_value(report_month) AS report_month,
    any_value(date_parse_status) AS date_parse_status,
    any_value(source_status) AS source_status,
    any_value(source_landing_url) AS source_landing_url,
    any_value(source_url) AS source_url,
    max(source_page) AS source_pages
FROM read_parquet('data/gold/parquet/council_ce_reports_corpus.parquet')
GROUP BY document_id, council;

-- The unreviewed lead queue is private and deliberately absent from deployments.
-- Replace this typed empty contract only with a separately published artifact
-- containing source-reviewed, promotion-permitted rows.
CREATE OR REPLACE VIEW v_la_ce_report_signals AS
SELECT
    CAST(NULL AS VARCHAR) AS lead_id,
    CAST(NULL AS VARCHAR) AS local_authority,
    CAST(NULL AS VARCHAR) AS report_title,
    CAST(NULL AS VARCHAR) AS report_month,
    CAST(NULL AS VARCHAR) AS source_url,
    CAST(NULL AS BIGINT) AS source_page,
    CAST(NULL AS VARCHAR) AS source_locator,
    CAST(NULL AS VARCHAR) AS quote,
    CAST(NULL AS VARCHAR[]) AS lead_types,
    CAST(NULL AS VARCHAR[]) AS amount_mentions,
    CAST(NULL AS VARCHAR) AS evidence_band,
    CAST(NULL AS VARCHAR) AS reviewed_project_name,
    CAST(NULL AS VARCHAR) AS reviewed_stage,
    CAST(NULL AS TIMESTAMPTZ) AS reviewed_utc
WHERE FALSE;

CREATE OR REPLACE VIEW v_la_ce_report_coverage AS
WITH documents AS (
    SELECT
        local_authority,
        count(*) AS documents,
        min(report_month) AS first_report_month,
        max(report_month) AS latest_report_month
    FROM v_la_ce_report_documents
    GROUP BY local_authority
)
SELECT
    d.local_authority,
    d.documents,
    d.first_report_month,
    d.latest_report_month,
    CAST(0 AS BIGINT) AS extracted_leads,
    CAST(0 AS BIGINT) AS review_queue_leads,
    CAST(0 AS BIGINT) AS reviewed_signals
FROM documents d;
