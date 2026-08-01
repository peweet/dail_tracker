-- v_council_minutes_docs — the searchable council-minutes text corpus (gold parquet
-- materialised by extractors/council_minutes_corpus_build.py). One row per clean doc:
-- council, meeting file, best-known date, doc_type, source_status (text|ocr_winocr|html
-- — ocr_winocr = Extracted-band OCR text, badge accordingly), source_url, full body.
-- Consumed by mcp_server/text_fts.py corpus 'council_minutes' (search_council_minutes).
CREATE OR REPLACE VIEW v_council_minutes_docs AS
SELECT
    council,
    meeting,
    meeting_date,
    doc_type,
    source_status,
    source_url,
    chunk,
    body
FROM read_parquet('data/gold/parquet/council_minutes_corpus.parquet');
