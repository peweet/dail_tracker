-- v_member_interests_detail — Register of Members' Interests as a contract-shaped
-- row set, one row per declaration. UNION of Dáil + Seanad silver parquets with
-- a `house` column added.
--
-- Replaces the in-page _load_interests() pattern (read_parquet + rename + flag
-- coercion + category filter) that previously lived in utility/pages_code/interests.py.
--
-- Columns expose the contract the UI consumes:
--   member_name, party_name, constituency, declaration_year,
--   interest_category, interest_text, landlord_flag, property_flag, house
-- plus the four TODO_PIPELINE_VIEW_REQUIRED placeholder columns the page
-- defaulted in-line:
--   directorship_flag, shareholding_flag, source_pdf_url, source_page_number
--
-- File name starts with 'member_' so any future member-focused data-access
-- module can pick it up, but interests has its own dedicated glob in
-- utility/data_access/interests_data.py (sql_views/member_interests_*.sql).

CREATE OR REPLACE VIEW v_member_interests_detail AS
WITH dail AS (
    SELECT
        full_name                       AS member_name,
        party                           AS party_name,
        constituency_name               AS constituency,
        CAST(year_declared AS INTEGER)  AS declaration_year,
        interest_category,
        interest_description_cleaned    AS interest_text,
        is_landlord                     AS landlord_flag,
        is_property_owner               AS property_flag,
        FALSE                           AS directorship_flag,  -- TODO_PIPELINE_VIEW_REQUIRED
        FALSE                           AS shareholding_flag,  -- TODO_PIPELINE_VIEW_REQUIRED
        CAST(NULL AS VARCHAR)           AS source_pdf_url,     -- TODO_PIPELINE_VIEW_REQUIRED
        CAST(NULL AS INTEGER)           AS source_page_number, -- TODO_PIPELINE_VIEW_REQUIRED
        'Dáil'                          AS house
    FROM read_parquet('data/silver/parquet/dail_member_interests_combined.parquet')
    WHERE interest_category IS DISTINCT FROM '15'
),
seanad AS (
    SELECT
        full_name                       AS member_name,
        party                           AS party_name,
        constituency_name               AS constituency,
        CAST(year_declared AS INTEGER)  AS declaration_year,
        interest_category,
        interest_description_cleaned    AS interest_text,
        is_landlord                     AS landlord_flag,
        is_property_owner               AS property_flag,
        FALSE                           AS directorship_flag,
        FALSE                           AS shareholding_flag,
        CAST(NULL AS VARCHAR)           AS source_pdf_url,
        CAST(NULL AS INTEGER)           AS source_page_number,
        'Seanad'                        AS house
    FROM read_parquet('data/silver/parquet/seanad_member_interests_combined.parquet')
    WHERE interest_category IS DISTINCT FROM '15'
)
SELECT * FROM dail
UNION ALL
SELECT * FROM seanad;
