-- v_procurement_afs_national_* — the NATIONAL amalgamated Local Authority Annual Financial
--   Statement: all 31 county & city councils' audited Income & Expenditure by service division,
--   every published year (2016–2023). Source: Dept of Housing's audited amalgamation of all LAs
--   (gov.ie), promoted to data/silver/parquet/afs_amalgamated_divisions.parquet by
--   extractors/afs_amalgamated_extract.py.
--
-- This is the ONLY complete, AUDITED national picture of what local government spends by service
-- — a BUDGET grain (a sibling of the per-council AFS in v_procurement_afs_by_division), and NEVER
-- summed with the over-€20k purchase-order euros (a different, far narrower register). net cost is
-- gross expenditure minus the service's own income/grants — what the local taxpayer ultimately
-- funds. Pre-aggregated/pre-ordered here; the page selects and renders, computing no metric.

CREATE OR REPLACE VIEW v_procurement_afs_national_by_division AS
SELECT
    year,
    division,
    gross_expenditure          AS gross_expenditure_eur,
    income                     AS income_eur,
    net_expenditure            AS net_expenditure_eur,
    net_expenditure_prior_yr   AS net_expenditure_prior_yr_eur,
    realisation_tier,
    value_kind
FROM read_parquet('data/silver/parquet/afs_amalgamated_divisions.parquet')
WHERE division IS NOT NULL
ORDER BY year DESC, net_expenditure DESC NULLS LAST;

-- Per-year national totals (Σ across the service divisions) — the "over time" spine. The GROUP BY
-- lives here so the page never aggregates.
CREATE OR REPLACE VIEW v_procurement_afs_national_by_year AS
SELECT
    year,
    SUM(gross_expenditure)  AS gross_expenditure_eur,
    SUM(income)             AS income_eur,
    SUM(net_expenditure)    AS net_expenditure_eur,
    COUNT(*)                AS n_divisions
FROM read_parquet('data/silver/parquet/afs_amalgamated_divisions.parquet')
WHERE division IS NOT NULL
GROUP BY year
ORDER BY year;
