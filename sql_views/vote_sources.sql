CREATE OR REPLACE VIEW v_vote_sources AS
SELECT DISTINCT
    vote_id,
    vote_url                    AS source_url,
    NULL::VARCHAR               AS source_document_url,
    NULL::VARCHAR               AS official_pdf_url,
    NULL::VARCHAR               AS legislation_url,
    'Oireachtas division record' AS source_label
FROM read_parquet('{PARQUET_PATH}')
WHERE vote_url IS NOT NULL
  AND vote_url != '';
