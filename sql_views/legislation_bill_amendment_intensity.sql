-- v_bill_amendment_intensity — per-bill amendment activity ("most contested bills").
-- Source: data/silver/parquet/bill_amendments.parquet (Oireachtas amendment-list
--   documents; 1,763 rows across 550 bills).
--
-- Grain: ONE row per bill_id (= bill_year || '_' || bill_no — identical key to
-- v_legislation_index, so this joins straight onto the legislation page).
--
-- WHAT IS COUNTED: each source row is a published AMENDMENT-LIST DOCUMENT at a
-- stage (a numbered list, or a "cream list" of additional amendments), NOT an
-- individual amendment. So `amendment_lists` is a faithful proxy for how
-- contested/reworked a bill was — the number of times amendments were formally
-- tabled — not a count of individual amendment clauses. Naming reflects that so
-- the figure is never overstated.
--
-- This promotes the amendment COUNT signal, which no view exposed before;
-- v_legislation_pdfs already lists the underlying documents themselves.

CREATE OR REPLACE VIEW v_bill_amendment_intensity AS
SELECT
    bill_id,
    ANY_VALUE(bill_short_title_en)                                   AS bill_title,
    ANY_VALUE(bill_type)                                             AS bill_type,
    ANY_VALUE(bill_status)                                           AS bill_status,
    COUNT(*)                                                         AS amendment_lists,
    COUNT(DISTINCT stage_show_as)                                    AS distinct_stages,
    COUNT(*) FILTER (WHERE stage_show_as = 'Committee Stage')        AS committee_lists,
    COUNT(*) FILTER (WHERE stage_show_as = 'Report Stage')           AS report_lists,
    COUNT(*) FILTER (WHERE stage_show_as = 'Cream List')             AS cream_lists,
    COUNT(*) FILTER (WHERE chamber = 'dail')                         AS dail_lists,
    COUNT(*) FILTER (WHERE chamber = 'seanad')                       AS seanad_lists,
    MIN(amendment_date)                                              AS first_amendment_date,
    MAX(amendment_date)                                              AS last_amendment_date
FROM read_parquet('data/silver/parquet/bill_amendments.parquet')
WHERE bill_id IS NOT NULL
GROUP BY bill_id
ORDER BY amendment_lists DESC, bill_id;
