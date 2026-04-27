-- TODO: Switch read_csv_auto to parquet once available
-- TODO: Add directorship_flag, shareholding_flag, source_pdf_url, source_page_number, member_id, property_count when pipeline supports them

CREATE OR REPLACE VIEW v_member_interests AS
WITH base AS (
    SELECT *,
        row_number() OVER () AS interest_record_id
    FROM read_csv_auto('data/silver/dail_member_interests_combined.csv')
    WHERE interest_category IS DISTINCT FROM '15'
)
SELECT
    interest_record_id,
    NULL::VARCHAR AS member_id, -- TODO: stable Oireachtas API member URI
    full_name AS member_name,
    COALESCE(party, '') AS party_name,
    COALESCE(constituency_name, '') AS constituency,
    'Dáil' AS house,
    NULL::VARCHAR AS dail_term,
    TRY_CAST(year_declared AS INTEGER) AS declaration_year,
    interest_category,
    COALESCE(interest_description_cleaned, '') AS interest_text,
    CASE WHEN lower(TRY_CAST(is_landlord AS VARCHAR)) = 'true' THEN TRUE ELSE FALSE END AS landlord_flag,
    CASE WHEN lower(TRY_CAST(is_property_owner AS VARCHAR)) = 'true' THEN TRUE ELSE FALSE END AS property_flag,
    FALSE AS directorship_flag, -- TODO: derive from interest_category
    FALSE AS shareholding_flag, -- TODO: derive from interest_category
    NULL::VARCHAR AS source_document_name,
    NULL::VARCHAR AS source_pdf_url, -- TODO: add to silver output
    NULL::INTEGER AS source_page_number, -- TODO: add to silver output
    current_timestamp AS latest_fetch_timestamp_utc
FROM base;

CREATE OR REPLACE VIEW v_member_interests_summary AS
SELECT
    'pipeline' AS latest_run_id,
    COUNT(DISTINCT full_name) AS members_with_interests_count,
    COUNT(*) AS declarations_count,
    MAX(TRY_CAST(year_declared AS INTEGER)) AS latest_declaration_year,
    1 AS source_documents_count,
    current_timestamp AS latest_fetch_timestamp_utc,
    'data/silver/dail_member_interests_combined.csv' AS source_summary,
    NULL::VARCHAR AS mart_version,
    NULL::VARCHAR AS code_version
FROM read_csv_auto('data/silver/dail_member_interests_combined.csv')
WHERE interest_category IS DISTINCT FROM '15';

CREATE OR REPLACE VIEW v_member_interests_category_summary AS
SELECT interest_category, COUNT(*) AS declarations_count
FROM read_csv_auto('data/silver/dail_member_interests_combined.csv')
WHERE interest_category IS DISTINCT FROM '15' AND interest_category IS NOT NULL
GROUP BY interest_category
ORDER BY declarations_count DESC;
