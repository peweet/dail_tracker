-- v_debate_listings — structural day-window listing of every debate section.
-- Source: data/silver/parquet/debate_listings.parquet
--
-- Stage 1 of the debates integration plan (see
-- pipeline_sandbox/dbsect_integration_plan.md §3, §10). One row per
-- (date, chamber, debate_section_id) — the composite identity. dbsect_2
-- recurs every sitting day, so downstream views must never join on
-- debate_section_id alone.
--
-- This view supersedes the bill-only data/silver/parquet/debates.parquet
-- that v_legislation_debates reads from. Once the harvester / flattener
-- graduate from pipeline_sandbox/, v_legislation_debates will be
-- refactored to read from this view filtered to bill_ref IS NOT NULL.

CREATE OR REPLACE VIEW v_debate_listings AS
SELECT
    debate_section_id,
    TRY_CAST(date AS DATE)                          AS debate_date,
    chamber,
    parent_section_id,
    parent_section_title,
    bill_ref,
    debate_type,
    CAST(speaker_count AS INTEGER)                  AS speaker_count,
    CAST(speech_count  AS INTEGER)                  AS speech_count,
    akn_xml_url,
    debate_url_web,
    show_as                                         AS debate_title
FROM read_parquet('data/silver/parquet/debate_listings.parquet')
WHERE debate_section_id IS NOT NULL
  AND chamber IN ('dail', 'seanad')
ORDER BY debate_date DESC NULLS LAST, chamber, debate_section_id;
