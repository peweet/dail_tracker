-- Source: data/silver/parquet/dail_member_interests_combined.parquet
-- Produced by member_interests.py (combine_years writes csv + parquet).
-- Streamlit reads silver via these views; gold ranking parquet is no longer used.
-- TODO: Add directorship_flag, shareholding_flag, source_pdf_url, source_page_number when pipeline supports them.

CREATE OR REPLACE VIEW v_member_interests AS
WITH base AS (
    SELECT *,
        row_number() OVER () AS interest_record_id
    FROM read_parquet('data/silver/parquet/dail_member_interests_combined.parquet')
    WHERE interest_category IS DISTINCT FROM '15'
)
SELECT
    interest_record_id,
    unique_member_code AS member_id,
    full_name          AS member_name,
    COALESCE(party, '')             AS party_name,
    COALESCE(constituency_name, '') AS constituency,
    'Dáil' AS house,
    NULL::VARCHAR AS dail_term,
    TRY_CAST(year_declared AS INTEGER) AS declaration_year,
    interest_category,
    COALESCE(interest_description_cleaned, '') AS interest_text,
    CASE WHEN lower(TRY_CAST(is_landlord AS VARCHAR))       = 'true' THEN TRUE ELSE FALSE END AS landlord_flag,
    CASE WHEN lower(TRY_CAST(is_property_owner AS VARCHAR)) = 'true' THEN TRUE ELSE FALSE END AS property_flag,
    FALSE AS directorship_flag,        -- TODO: derive from interest_category
    FALSE AS shareholding_flag,        -- TODO: derive from interest_category
    NULL::VARCHAR AS source_document_name,
    NULL::VARCHAR AS source_pdf_url,   -- TODO: add to silver output
    NULL::INTEGER AS source_page_number,
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
    'data/silver/parquet/dail_member_interests_combined.parquet' AS source_summary,
    NULL::VARCHAR AS mart_version,
    NULL::VARCHAR AS code_version
FROM read_parquet('data/silver/parquet/dail_member_interests_combined.parquet')
WHERE interest_category IS DISTINCT FROM '15';

CREATE OR REPLACE VIEW v_member_interests_category_summary AS
SELECT interest_category, COUNT(*) AS declarations_count
FROM read_parquet('data/silver/parquet/dail_member_interests_combined.parquet')
WHERE interest_category IS DISTINCT FROM '15' AND interest_category IS NOT NULL
GROUP BY interest_category
ORDER BY declarations_count DESC;

-- One row per (member, year). Rank is purely on declared-interest count, descending,
-- broken with rank() so ties share a rank. Replaces the orphaned
-- generate_interests_ranking.py / interests_member_ranking.parquet feature.
CREATE OR REPLACE VIEW v_member_interests_ranking AS
WITH counts AS (
    SELECT
        unique_member_code                       AS member_id,
        full_name                                AS member_name,
        COALESCE(party, '')                      AS party_name,
        COALESCE(constituency_name, '')          AS constituency,
        TRY_CAST(year_declared AS INTEGER)       AS declaration_year,
        SUM(CASE
            WHEN interest_description_cleaned IS NOT NULL
             AND interest_description_cleaned != 'No interests declared'
            THEN 1 ELSE 0
        END)                                     AS interest_count
    FROM read_parquet('data/silver/parquet/dail_member_interests_combined.parquet')
    WHERE interest_category IS DISTINCT FROM '15'
    GROUP BY unique_member_code, full_name, party, constituency_name, year_declared
)
SELECT
    member_id,
    member_name,
    party_name,
    constituency,
    declaration_year,
    interest_count,
    rank() OVER (
        PARTITION BY declaration_year
        ORDER BY interest_count DESC
    ) AS rank
FROM counts
WHERE declaration_year IS NOT NULL
ORDER BY declaration_year DESC, rank ASC;
