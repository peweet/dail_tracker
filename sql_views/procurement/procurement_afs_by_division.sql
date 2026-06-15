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
    -- pct_self_funded = income/gross, computed HERE (pipeline) never in the UI: "how much of running
    -- this service is covered by its own charges/rents/grants vs. funded by the local taxpayer".
    -- Recreation/libraries sit near 0% (you fund them); housing near 100% (rents+recoupment cover it).
    -- Clamped to [0,999], NULL when gross<=0. ⚠️ Miscellaneous can exceed 100% (it carries the
    -- rates/LPT allocation as "income"), so it is not a like-for-like figure — the UI labels it so.
    CASE WHEN gross_expenditure > 0
         THEN LEAST(999.0, ROUND(100.0 * income / gross_expenditure, 0))
    END                 AS pct_self_funded,
    reconciled,
    parser,
    source_file_url,
    source_page_number
FROM read_parquet('data/silver/parquet/la_afs_divisions.parquet');
