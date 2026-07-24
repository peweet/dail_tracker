-- v_procurement_afs_loan_movement — per-council AFS Note 7 movement in Loans Payable: the
-- in-year borrow/repay FLOW behind the debt stock. Answers "is this council ramping borrowing
-- or paying debt down?" — item='borrowings' is NEW debt raised in the year.
--
-- Source + grain: data/silver/parquet/la_afs_loan_movement.parquet, one row per (council, year,
-- item). Every stored (council, year) is reconcile-gated (opening + flows == closing); a
-- non-reconciling year is dropped at extract, so a wrong figure is never surfaced.
--
-- ⚠️ MIXED grain, tagged by is_flow: opening_balance/closing_balance are STOCKS; borrowings /
-- repayment_of_principal / early_redemptions / other_adjustments are in-year FLOWS (repayments &
-- redemptions negative). NEVER sum a flow with a stock, nor either with any spend fact. The Note-7
-- closing is GROSS borrowing and legitimately differs from the Balance-Sheet Loans Payable line —
-- never reconcile the two facts against each other.
CREATE OR REPLACE VIEW v_procurement_afs_loan_movement AS
SELECT
    council,
    slug,
    region,
    year,
    item,
    value_eur,
    is_flow,
    is_statement_year,
    statement_year,
    source_file_url,
    source_page_number,
    parse_method            AS parser
FROM read_parquet('data/silver/parquet/la_afs_loan_movement.parquet')
WHERE value_eur IS NOT NULL;
