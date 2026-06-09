-- v_procurement_afs_by_division — one council-year's REVENUE spending broken out by service
-- division (Housing / Roads / Environment / …) for the public-body dossier's by-function panel.
--
-- Source + grain: see v_procurement_afs_total_by_year. Each row is one service division's
-- audited operating expenditure (gross), the income/grants against it, and the net cost. Display
-- passthrough of the reconcile-gated fact — no aggregation, the page renders rows as-is.
--
-- ⚠️ BUDGET grain — never summed with PO/payment or award euros; never call gross the council's
-- "total spend" (it is operating expenditure by division, excl. transfers + capital).
CREATE OR REPLACE VIEW v_procurement_afs_by_division AS
SELECT
    council,
    slug,
    region,
    year,
    division,
    gross_expenditure   AS gross_expenditure_eur,
    income              AS income_eur,
    net_expenditure     AS net_expenditure_eur,
    reconciled,
    parser,
    source_file_url,
    source_page_number
FROM read_parquet('data/silver/parquet/la_afs_divisions.parquet');
