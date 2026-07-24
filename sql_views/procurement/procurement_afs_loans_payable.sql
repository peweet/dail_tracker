-- v_procurement_afs_loans_payable — a council's LOANS PAYABLE (long-term borrowing outstanding
-- at 31 December) from the audited AFS Balance Sheet. A focused convenience view over the full
-- balance-sheet fact (item='loans_payable'), kept so the debt league table + trend read cleanly.
--
-- Source + grain: data/silver/parquet/la_afs_balance_sheet.parquet, one row per (council, year).
-- This is a STOCK — a debt LIABILITY at a point in time.
--
-- ⚠️ STOCK grain — NOT a flow. NEVER sum or reconcile with AFS expenditure, procurement-awarded,
-- payments or budget euros; sum across councils only for a point-in-time total, never across years
-- for one council. The in-year borrow/repay FLOW is a separate fact (v_procurement_afs_loan_movement),
-- and the Note-7 gross total there legitimately differs from this line (mortgage reclassification).
CREATE OR REPLACE VIEW v_procurement_afs_loans_payable AS
SELECT
    council,
    slug,
    region,
    year,
    value_eur               AS loans_payable_eur,
    is_statement_year,
    statement_year,
    source_file_url,
    source_page_number,
    parse_method            AS parser
FROM read_parquet('data/silver/parquet/la_afs_balance_sheet.parquet')
WHERE item = 'loans_payable' AND value_eur IS NOT NULL;
