-- v_member_registry_all — v_member_registry PLUS former members (past terms).
--
-- v_member_registry stays current-term-only (the authoritative list every other
-- consumer reads). This view ADDS historic members so the Member Overview browse
-- page can offer an "Include historic TDs" toggle + Dáil/year filters without
-- changing search / MCP / API behaviour.
--
-- Sources:
--   v_member_registry                                    current Dáil + Seanad
--   historic_members_dail.parquet  / _seanad.parquet     former members only
--   member_terms.parquet (member × term)                 dails_served + year span
--
-- Extra columns vs v_member_registry:
--   is_current        BOOLEAN  — sitting member (true) vs former (false)
--   dails_served      VARCHAR  — comma list of house numbers, e.g. '32,33,34'
--   served_from_year  INTEGER  — earliest membership start year
--   served_to_year    INTEGER  — latest membership end year (NULL = still serving)
-- Depends on v_member_registry, so it MUST register after member_registry.sql.

CREATE OR REPLACE VIEW v_member_registry_all AS
WITH terms AS (
    SELECT
        unique_member_code,
        string_agg(d, ',' ORDER BY d)                              AS dails_served,
        MIN(start_yr)                                              AS served_from_year,
        CASE WHEN bool_or(end_yr IS NULL) THEN NULL ELSE MAX(end_yr) END AS served_to_year
    FROM (
        SELECT DISTINCT
            unique_member_code,
            CAST(dail_number AS VARCHAR)                           AS d,
            CAST(strftime(membership_start_date, '%Y') AS INTEGER) AS start_yr,
            CAST(strftime(membership_end_date, '%Y') AS INTEGER)   AS end_yr
        FROM read_parquet('{MEMBER_TERMS_PARQUET_PATH}')
        WHERE dail_number IS NOT NULL
    )
    GROUP BY unique_member_code
),
current_members AS (
    SELECT
        r.unique_member_code,
        r.member_name,
        r.constituency,
        r.party_name,
        r.is_minister,
        r.year_elected,
        CAST(r.membership_start_date AS VARCHAR)                   AS membership_start_date,
        CAST(r.membership_end_date AS VARCHAR)                     AS membership_end_date,
        r.house,
        TRUE                                                       AS is_current,
        t.dails_served,
        t.served_from_year,
        t.served_to_year
    FROM v_member_registry r
    LEFT JOIN terms t USING (unique_member_code)
),
historic_dail AS (
    SELECT
        h.unique_member_code,
        h.full_name                                               AS member_name,
        h.constituency_name                                       AS constituency,
        h.party                                                   AS party_name,
        CASE WHEN LOWER(CAST(h.ministerial_office AS VARCHAR)) = 'true'
             THEN 'true' ELSE 'false' END                         AS is_minister,
        h.year_elected,
        CAST(NULL AS VARCHAR)                                     AS membership_start_date,
        CAST(NULL AS VARCHAR)                                     AS membership_end_date,
        'Dáil'                                                    AS house,
        FALSE                                                     AS is_current,
        t.dails_served,
        t.served_from_year,
        t.served_to_year
    FROM read_parquet('{HISTORIC_DAIL_PARQUET_PATH}') h
    LEFT JOIN terms t USING (unique_member_code)
    WHERE h.full_name IS NOT NULL AND h.unique_member_code IS NOT NULL
),
historic_seanad AS (
    SELECT
        h.unique_member_code,
        h.full_name                                               AS member_name,
        h.constituency_name                                       AS constituency,
        h.party                                                   AS party_name,
        CASE WHEN LOWER(CAST(h.ministerial_office AS VARCHAR)) = 'true'
             THEN 'true' ELSE 'false' END                         AS is_minister,
        h.year_elected,
        CAST(NULL AS VARCHAR)                                     AS membership_start_date,
        CAST(NULL AS VARCHAR)                                     AS membership_end_date,
        'Seanad'                                                  AS house,
        FALSE                                                     AS is_current,
        t.dails_served,
        t.served_from_year,
        t.served_to_year
    FROM read_parquet('{HISTORIC_SEANAD_PARQUET_PATH}') h
    LEFT JOIN terms t USING (unique_member_code)
    WHERE h.full_name IS NOT NULL AND h.unique_member_code IS NOT NULL
)
SELECT * FROM current_members
UNION ALL
SELECT * FROM historic_dail
UNION ALL
SELECT * FROM historic_seanad
ORDER BY is_current DESC, member_name;
