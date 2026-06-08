-- v_sipo_expenses_base — reads the SIPO GE2024 candidate-expenses gold Parquet
-- produced by extractors/sipo_expenses_paddle_etl.py (PaddleOCR / text-layer
-- extraction of each party's National-Agent Election Expenses Statement).
--
-- Grain: one row per candidate per party (the "amount assigned to the party" /
-- "expenditure on the candidate by the national agent" Part-3 summary table).
--
-- These are ELECTION EXPENSES (money spent campaigning), NOT donations. The whole
-- corpus is the 2024 general election (34th Dáil) only — there is no year axis.
--
-- OCR-derived: `flag` marks rows needing review and `*_confidence` carry the
-- per-cell OCR confidence. The UI must surface a "verify against the official SIPO
-- PDF" caveat and must not imply influence from any figure (no-inference rule).
-- Provenance / statutory limits: data/_meta/sipo_ge2024_expenses_sources.md.

CREATE OR REPLACE VIEW v_sipo_expenses_base AS
SELECT
    'General Election 2024'                       AS election,
    party,
    candidate_name_raw                            AS candidate_name,
    constituency,
    amount_assigned_eur,
    expenditure_eur,
    statutory_limit_eur,
    expenditure_confidence,
    row_min_confidence,
    constituency_match_score,
    flag,
    (flag = 'ok')                                 AS is_verified,
    source_pdf,
    source_page
FROM read_parquet('data/gold/parquet/sipo_expenses_fact.parquet');


-- v_sipo_expenses_by_party — party-level rollup for the Election-Expenses lens cards
-- (aggregation lives in the pipeline view, not the Streamlit retrieval layer).
--
-- This sums the national-agent EXPENDITURE-ON-CANDIDATES column (Part 3). It is NOT
-- the party's total campaign spend — parties that book spend centrally (e.g. Sinn
-- Féin) record little here and most of their outlay in the Part-4 national totals.
--
-- QUARANTINE (gold-layer quality gap, until the ÷100 cap-repair lands in the ETL):
-- rows flagged `over_limit_verify` are decimal-loss OCR mis-reads (a value > the
-- statutory limit, e.g. FF €709,513 = €7,095.13). They are EXCLUDED from the headline
-- total and counted in `excluded_count` so the UI can show the caveat — never summed
-- as fact. See doc/SIPO_CONSOLIDATION_PLAN.md (flag_amount cap-repair).
CREATE OR REPLACE VIEW v_sipo_expenses_by_party AS
SELECT
    election,
    party,
    COUNT(*)                                                              AS candidate_count,
    SUM(CASE WHEN flag <> 'over_limit_verify' THEN expenditure_eur ELSE 0 END) AS total_expenditure,
    MAX(CASE WHEN flag <> 'over_limit_verify' THEN expenditure_eur END)        AS max_expenditure,
    SUM(CASE WHEN flag <> 'ok' THEN 1 ELSE 0 END)                         AS verify_count,
    SUM(CASE WHEN flag = 'over_limit_verify' THEN 1 ELSE 0 END)           AS excluded_count
FROM v_sipo_expenses_base
GROUP BY election, party
ORDER BY total_expenditure DESC;
