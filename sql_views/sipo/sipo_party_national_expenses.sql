-- Part-4 NATIONAL-AGENT itemised expenses (the party's own central campaign spend),
-- distinct from the Part-3 per-candidate apportionment in sipo_expenses_base.sql.
--
-- Reads the gold parquets promoted by extractors/sipo_promote_to_gold.py from the
-- Part-4 silver facts (extractors/sipo_expense_items_paddle_etl.py):
--   * sipo_expense_categories.parquet — the 8 statutory headings (4A–4H) + Overall.
--   * sipo_expense_items.parquet      — each Ref line (A1, A10, …): description + cost.
--
-- GE2024 only (34th Dáil); no year axis. OCR-derived from the scanned National-Agent
-- returns — `flag`/confidence carry review state and the UI must surface a "verify
-- against the official SIPO PDF" caveat (no-inference). COVERAGE IS INCREMENTAL: only
-- parties whose Part-4 pages have been OCR'd appear (FF/SF/Aontú at first promotion).
--
-- ⚠ DO NOT SUM Part-4 (these views) WITH Part-3 (v_sipo_expenses_*): same return,
-- different purpose — Part-3 apportions a limit-bound amount to each candidate, Part-4
-- itemises what the agent actually spent centrally. They relate but are not additive.
-- `category_total_eur` is the PRINTED official figure off the "Expenses Review" page;
-- `items_sum_eur`/`reconciles` flag headings where our line-item OCR is incomplete, so
-- the total stays trustworthy even where the itemised list under-captures.


-- v_sipo_party_national_categories — one row per party × heading (8 sections + Overall).
CREATE OR REPLACE VIEW v_sipo_party_national_categories AS
SELECT
    'General Election 2024'  AS election,
    party,
    section,
    category                 AS category_label,
    category_total_eur,
    items_sum_eur,
    reconciles,
    is_overall,
    total_confidence,
    source_page
FROM read_parquet('data/gold/parquet/sipo_expense_categories.parquet');


-- v_sipo_party_national_items — line-item grain. `item_description` is the return's
-- free-text "Expenditure Item" (a mix of supplier names + descriptions, like the
-- candidate Part-5 `detail` — never asserted as a clean vendor field).
CREATE OR REPLACE VIEW v_sipo_party_national_items AS
SELECT
    'General Election 2024'  AS election,
    party,
    section,
    category                 AS category_label,
    ref,
    item_description,
    cost_eur,
    cost_confidence,
    flag,
    (flag = 'ok')            AS is_verified,
    source_page
FROM read_parquet('data/gold/parquet/sipo_expense_items.parquet');
