-- v_procurement_afs_capital_by_division — one council-year's CAPITAL investment broken out by
-- service division (Housing / Roads / Recreation / …) for the dossier's BUILDING-lane breakdown.
--
-- Source + grain: see v_procurement_afs_capital_by_year. Each row is one service division's audited
-- capital-account expenditure (the build/acquire programme) and the income (grants/loans/levies)
-- that financed it, for one council-year. Display passthrough of the reconcile-gated fact — no
-- aggregation, the page renders rows as-is.
--
-- ⚠️ CAPITAL grain — a DISTINCT fact, never summed with revenue net-cost, PO/payment or award euros;
-- never call it the council's "total spend". This is the investment programme, dominated by central
-- (DHLGH) housing grants — which is precisely why housing dwarfs the other divisions here while it
-- nets to ~€0 in the revenue account.
CREATE OR REPLACE VIEW v_procurement_afs_capital_by_division AS
SELECT
    council,
    slug,
    region,
    year,
    division,
    capital_expenditure   AS capital_expenditure_eur,
    capital_income        AS capital_income_eur,
    reconciled,
    parse_method          AS parser,
    source_file_url,
    source_page_number
FROM read_parquet('data/silver/parquet/la_afs_capital_divisions.parquet')
WHERE capital_expenditure > 0;  -- drop the handful of negative/writeback rows (corrections, not investment)
