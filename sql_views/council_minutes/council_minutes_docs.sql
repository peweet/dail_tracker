-- v_council_minutes_docs: searchable council-minute passages from the gold parquet
-- materialised by extractors/council_minutes_corpus_build.py. Grain is one bounded
-- passage (document_id + chunk), not one document or decision. Agendas are excluded;
-- doc_type/meeting_scope preserve plenary, municipal-district and committee scope.
-- source_status=ocr_winocr is machine OCR and must carry that caveat.
-- Consumed by mcp_server/text_fts.py corpus 'council_minutes'.
CREATE OR REPLACE VIEW v_council_minutes_docs AS
SELECT
    document_id,
    entity_type,
    council,
    meeting,
    -- Typed DATE: text_fts's year filter needs DATE. The raw field also holds
    -- blanks and month-grain strings; try_cast keeps those documents searchable.
    try_cast(meeting_date AS DATE) AS meeting_date,
    doc_type,
    meeting_scope,
    source_status,
    source_url,
    participant_categories,
    issue_themes,
    planning_references,
    board_references,
    collective_organisation_names,
    chunk,
    body
FROM read_parquet('data/gold/parquet/council_minutes_corpus.parquet');
