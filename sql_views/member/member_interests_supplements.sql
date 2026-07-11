-- v_member_interests_supplements — Section 29 statements (supplements to the
-- Register of Members' Interests): the late filings and corrections laid before
-- the Houses between annual registers. One row per (supplement file × member
-- statement). Source parquet built by members/member_interests_supplements.py.
--
-- DIFFERENT GRAIN from v_member_interests_detail: the annual register is a full
-- restatement, a supplement is an incremental correction event. Never union or
-- count the two together.
--
-- v_member_interests_backfill — per-member roll-up of the correction record.
-- max_years_one_supplement is the red-flag lead: the most registration years a
-- member back-filled in a single supplement (Robert Troy's Aug-2022 filing
-- covers ten years — the documentary signature of the non-declaration that
-- forced his resignation). A LEAD, NOT A VERDICT: routine first/consolidated
-- filings also score high.

CREATE OR REPLACE VIEW v_member_interests_supplements AS
SELECT
    unique_member_code               AS member_id,
    COALESCE(full_name, member_name) AS member_name,
    party                            AS party_name,
    constituency_name                AS constituency,
    house,
    CAST(supplement_date AS DATE)    AS supplement_date,
    years_declared,
    n_years,
    categories,
    n_categories,
    text_source,   -- 'embedded' | 'ocr' (scanned supplements recovered via Tesseract)
    source_file
FROM read_parquet('data/silver/parquet/member_interests_supplements.parquet');

CREATE OR REPLACE VIEW v_member_interests_backfill AS
WITH per_file AS (
    -- one row per member × supplement file; years counted DISTINCT across the
    -- file's statements (a multi-statement file like Troy's Aug-2022 one files
    -- each year as its own statement)
    SELECT
        s.member_id,
        ANY_VALUE(s.member_name)   AS member_name,
        ANY_VALUE(s.party_name)    AS party_name,
        ANY_VALUE(s.constituency)  AS constituency,
        ANY_VALUE(s.house)         AS house,
        s.source_file,
        MIN(s.supplement_date)     AS supplement_date,
        COUNT(*)                   AS n_statements,
        COUNT(DISTINCT u.y)        AS years_in_file
    FROM v_member_interests_supplements s
    LEFT JOIN LATERAL UNNEST(string_split(NULLIF(s.years_declared, ''), ';')) AS u(y) ON TRUE
    GROUP BY s.member_id, s.source_file
),
per_member_years AS (
    SELECT
        s.member_id,
        COUNT(DISTINCT u.y) AS years_backfilled,
        string_agg(DISTINCT u.y, ';' ORDER BY u.y) AS all_years
    FROM v_member_interests_supplements s
    JOIN LATERAL UNNEST(string_split(NULLIF(s.years_declared, ''), ';')) AS u(y) ON TRUE
    GROUP BY s.member_id
)
SELECT
    f.member_id,
    ANY_VALUE(f.member_name)      AS member_name,
    ANY_VALUE(f.party_name)       AS party_name,
    ANY_VALUE(f.constituency)     AS constituency,
    ANY_VALUE(f.house)            AS house,
    COUNT(*)                      AS n_supplements,
    SUM(f.n_statements)           AS n_statements,
    MAX(f.years_in_file)          AS max_years_one_supplement,
    COALESCE(ANY_VALUE(y.years_backfilled), 0) AS years_backfilled,
    ANY_VALUE(y.all_years)        AS years_declared,
    MIN(f.supplement_date)        AS first_supplement_date,
    MAX(f.supplement_date)        AS latest_supplement_date
FROM per_file f
LEFT JOIN per_member_years y USING (member_id)
GROUP BY f.member_id
ORDER BY max_years_one_supplement DESC, n_supplements DESC;
