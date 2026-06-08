-- v_payments_sources — source link stubs (placeholder until pipeline exposes URLs)
-- TODO_PIPELINE_VIEW_REQUIRED: per-year source PDF URL (official Oireachtas payment record)
-- TODO_PIPELINE_VIEW_REQUIRED: source_document_url, official_pdf_url, oireachtas_url

CREATE OR REPLACE VIEW v_payments_sources AS
SELECT
    NULL::VARCHAR AS source_url,
    NULL::VARCHAR AS source_document_url,
    NULL::VARCHAR AS official_pdf_url,
    NULL::VARCHAR AS oireachtas_url,
    'Oireachtas Payment Records' AS source_summary
FROM (SELECT 1) t;
