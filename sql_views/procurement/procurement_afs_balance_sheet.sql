-- v_procurement_afs_balance_sheet — per-council AFS Balance Sheet (Statement of Financial
-- Position) line items in LONG form: what a council owns, owes and has borrowed at 31 December.
--
-- Source + grain: data/silver/parquet/la_afs_balance_sheet.parquet, one row per
-- (council, year, item). `section` groups items (fixed_assets / assets / current_assets /
-- current_liabilities / long_term_creditors / net). Display passthrough — the page pivots/sums.
--
-- ⚠️ STOCK grain — positions at a point in time, NOT flows. NEVER sum or reconcile any item with
-- AFS revenue/capital EXPENDITURE, procurement-awarded, payments or budget euros; and NEVER sum
-- one item across YEARS for a council (double-counts a carried balance). Sub-total lines are not
-- stored — sum the labelled components (fixed assets = the fa_* items; current assets = the
-- current_assets section). NOAC publishes none of this — it is the only source of council
-- financial position.
CREATE OR REPLACE VIEW v_procurement_afs_balance_sheet AS
SELECT
    council,
    slug,
    region,
    year,
    section,
    item,
    value_eur,
    is_statement_year,      -- TRUE = the statement's own audited year; FALSE = a prior-year comparative
    statement_year,
    source_file_url,
    source_page_number,
    parse_method            AS parser
FROM read_parquet('data/silver/parquet/la_afs_balance_sheet.parquet')
WHERE value_eur IS NOT NULL;
