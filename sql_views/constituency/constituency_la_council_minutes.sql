-- v_la_council_minutes_docs: council-app serving view over the vetted minutes
-- corpus. Grain is one bounded passage (document_id + chunk), not one decision.
-- Text is machine extracted; source_status=ocr_winocr must be badged in the UI.
CREATE OR REPLACE VIEW v_la_council_minutes_docs AS
SELECT
    document_id,
    council AS local_authority,
    meeting,
    meeting_date,
    try_cast(meeting_date AS DATE) AS meeting_date_parsed,
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

-- One row per source document for dossier recency/coverage tables. The passage
-- view above remains the search grain; this view prevents chunk counts from
-- being mistaken for meeting-document counts.
CREATE OR REPLACE VIEW v_la_council_minutes_documents AS
SELECT
    document_id,
    council AS local_authority,
    any_value(meeting) AS meeting,
    any_value(meeting_date) AS meeting_date,
    try_cast(any_value(meeting_date) AS DATE) AS meeting_date_parsed,
    any_value(doc_type) AS doc_type,
    any_value(meeting_scope) AS meeting_scope,
    any_value(source_status) AS source_status,
    any_value(source_url) AS source_url
FROM read_parquet('data/gold/parquet/council_minutes_corpus.parquet')
GROUP BY document_id, council;

-- One row per council, computed at document grain. These are corpus coverage
-- facts, not claims about how many meetings the council actually held.
CREATE OR REPLACE VIEW v_la_council_minutes_coverage AS
SELECT
    local_authority,
    count(*) AS documents,
    count(*) FILTER (WHERE meeting_scope = 'plenary') AS plenary_documents,
    count(*) FILTER (WHERE meeting_scope = 'municipal_district') AS municipal_documents,
    count(*) FILTER (WHERE meeting_scope = 'committee') AS committee_documents,
    count(*) FILTER (WHERE source_status = 'ocr_winocr') AS ocr_documents,
    count(*) FILTER (WHERE meeting_date_parsed IS NOT NULL) AS dated_documents,
    count(*) FILTER (WHERE source_url <> '') AS sourced_documents,
    min(meeting_date_parsed) AS first_meeting_date,
    max(meeting_date_parsed) AS last_meeting_date
FROM v_la_council_minutes_documents
GROUP BY local_authority;
