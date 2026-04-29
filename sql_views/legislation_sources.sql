-- v_legislation_sources — official source URLs for each bill
-- Source: data/silver/parquet/sponsors.parquet (oireachtas_url)
-- TODO_PIPELINE_VIEW_REQUIRED: legislation_url (Statute Book URI for enacted bills)
-- TODO_PIPELINE_VIEW_REQUIRED: official_pdf_url (bill text PDF from versions.parquet)
-- TODO_PIPELINE_VIEW_REQUIRED: source_document_url (Explanatory Memo from related_docs.parquet)

CREATE OR REPLACE VIEW v_legislation_sources AS
SELECT DISTINCT
    bill_year || '_' || bill_no     AS bill_id,
    bill_url                        AS oireachtas_url,
    NULL::VARCHAR                   AS legislation_url,
    NULL::VARCHAR                   AS official_pdf_url,
    NULL::VARCHAR                   AS source_url,
    NULL::VARCHAR                   AS source_document_url,
    bill_no,
    bill_year
FROM read_parquet('data/silver/parquet/sponsors.parquet');
