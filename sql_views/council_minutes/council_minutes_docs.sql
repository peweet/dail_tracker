-- v_council_minutes_docs — the searchable council-minutes text corpus (gold parquet
-- materialised by extractors/council_minutes_corpus_build.py). One row per clean doc:
-- council, meeting file, best-known date, doc_type, source_status (text|ocr_winocr|html
-- — ocr_winocr = Extracted-band OCR text, badge accordingly), source_url, full body.
-- Consumed by mcp_server/text_fts.py corpus 'council_minutes' (search_council_minutes).
CREATE OR REPLACE VIEW v_council_minutes_docs AS
SELECT
    document_id,
    entity_type,
    council,
    meeting,
    -- Typed DATE: text_fts's year() filter needs a DATE column (year(VARCHAR) has no
    -- overload — found in-practice 2026-08-01), and the raw field holds '' and
    -- '2026 February' forms. try_cast nulls those; they stay retrievable via `meeting`.
    try_cast(meeting_date AS DATE) AS meeting_date,
    doc_type,
    meeting_scope,
    source_status,
    source_url,
    chunk,
    body
FROM read_parquet('data/gold/parquet/council_minutes_corpus.parquet');
