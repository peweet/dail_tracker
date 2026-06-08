-- v_lobbying_return_documents — third-party PDF URLs embedded in lobbying.ie
-- return free-text fields (position papers, pre-budget submissions, briefs).
-- Source: data/silver/parquet/lobbying_return_documents.parquet
--
-- One row per (lobbying return, PDF). These links are lobbyist-supplied —
-- they point to external hosts (e.g. chambers.ie, amcham.ie) and may rot.
-- Distinguish from Oireachtas-issued PDFs surfaced in v_legislation_pdfs.

CREATE OR REPLACE VIEW v_lobbying_return_documents AS
SELECT
    primary_key                         AS return_id,
    lobbyist_name,
    lobby_url,
    source_field,
    pdf_url,
    host,
    date_published_timestamp,
    public_policy_area
FROM read_parquet('data/silver/parquet/lobbying_return_documents.parquet');
