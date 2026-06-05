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
