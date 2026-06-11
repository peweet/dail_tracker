-- v_judiciary_plaintiff_league — repeat INSTITUTIONAL applicants across the whole
-- captured Legal Diary archive (the "who is suing" accountability signal).
-- Source: data/gold/parquet/judicial_legal_diary_cases.parquet (Tier C, ANONYMISED).
--
-- SCOPE (privacy): organisations and State bodies ONLY — the parties the privacy
-- contract keeps in clear. Individuals (initials) are never ranked. Mixed sides
-- where an initialed person rides alongside an org ("S. and Company Bank …") are
-- EXCLUDED rather than mis-ranked under a person-bearing label.
--
-- NAME CANONICALISATION is mechanical only (logic firewall owns it here, not the
-- UI): uppercase fold, legal-suffix abbreviation (DESIGNATED ACTIVITY COMPANY->DAC,
-- LIMITED->LTD, PUBLIC LIMITED COMPANY->PLC), bracketed liquidation notes and a
-- trailing "& Ors" stripped, whitespace collapsed. Known residue: list-codes glued
-- by the source ("SPMARS CAPITAL…") stay distinct rows — fixing that belongs to the
-- diary extractor, not to a guessier merge here. display_name is the most frequent
-- raw form. n_appearances counts LISTINGS (a matter listed on 3 days counts 3) —
-- present it as list appearances, never as "cases brought".
--
-- GRAIN: GROUPING SETS — one rollup row per canon name (court NULL, is_overall=1)
-- plus one row per canon name x court (is_overall=0). The UI filters; it never sums.
CREATE OR REPLACE VIEW v_judiciary_plaintiff_league AS
WITH base AS (
    SELECT
        diary_date,
        court,
        plaintiff,
        plaintiff_kind,
        trim(regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(upper(plaintiff), '\[[^\]]*\]', ' ', 'g'),
                        '\s*&\s*ORS\.?$', '', 'g'),
                    'DESIGNATED ACTIVITY COMPANY', 'DAC', 'g'),
                'PUBLIC LIMITED COMPANY', 'PLC', 'g'),
            '\s+', ' ', 'g')) AS plaintiff_canon_step
    FROM read_parquet('data/gold/parquet/judicial_legal_diary_cases.parquet')
    WHERE plaintiff_kind IN ('organisation', 'state-body')
      AND plaintiff IS NOT NULL
      AND length(plaintiff) > 2
      -- exclude mixed sides led by an anonymised individual ("C. and Family Agency")
      AND NOT regexp_matches(plaintiff, '^([A-Z]\.)+\s+and\s+')
),
canon AS (
    SELECT
        diary_date,
        court,
        plaintiff,
        plaintiff_kind,
        trim(regexp_replace(plaintiff_canon_step, ' LIMITED$', ' LTD', 'g')) AS plaintiff_canon
    FROM base
)
SELECT
    plaintiff_canon,
    mode(plaintiff)              AS display_name,
    mode(plaintiff_kind)         AS plaintiff_kind,
    court,
    CAST(GROUPING(court) AS BOOLEAN) AS is_overall,
    COUNT(*)                     AS n_appearances,
    COUNT(DISTINCT diary_date)   AS n_days,
    MIN(diary_date)              AS first_date,
    MAX(diary_date)              AS last_date
FROM canon
GROUP BY GROUPING SETS ((plaintiff_canon), (plaintiff_canon, court))
ORDER BY is_overall DESC, n_appearances DESC, plaintiff_canon;
