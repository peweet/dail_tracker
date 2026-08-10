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

CREATE OR REPLACE VIEW v_la_ce_report_signals AS
SELECT
    lead_id,
    CASE
        WHEN council = 'Dún Laoghaire-Rathdown' THEN 'Dun Laoghaire-Rathdown'
        ELSE council
    END AS local_authority,
    report_title,
    report_month,
    source_url,
    source_page,
    source_locator,
    quote,
    lead_types,
    amount_mentions,
    evidence_band,
    reviewed_project_name,
    reviewed_stage,
    reviewed_utc
FROM read_parquet('data/gold/parquet/council_ce_report_leads.parquet')
WHERE promotion_permitted;

CREATE OR REPLACE VIEW v_la_ce_report_coverage AS
WITH documents AS (
    SELECT
        local_authority,
        count(*) AS documents,
        min(report_month) AS first_report_month,
        max(report_month) AS latest_report_month
    FROM v_la_ce_report_documents
    GROUP BY local_authority
),
leads AS (
    SELECT
        CASE
            WHEN council = 'Dún Laoghaire-Rathdown' THEN 'Dun Laoghaire-Rathdown'
            ELSE council
        END AS local_authority,
        count(*) AS extracted_leads,
        count(*) FILTER (WHERE reviewer_state = 'NOT_REVIEWED') AS review_queue_leads,
        count(*) FILTER (WHERE promotion_permitted) AS reviewed_signals
    FROM read_parquet('data/gold/parquet/council_ce_report_leads.parquet')
    GROUP BY local_authority
)
SELECT
    d.local_authority,
    d.documents,
    d.first_report_month,
    d.latest_report_month,
    coalesce(l.extracted_leads, 0) AS extracted_leads,
    coalesce(l.review_queue_leads, 0) AS review_queue_leads,
    coalesce(l.reviewed_signals, 0) AS reviewed_signals
FROM documents d
LEFT JOIN leads l USING (local_authority);
