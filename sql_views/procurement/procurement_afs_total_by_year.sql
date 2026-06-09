-- v_procurement_afs_total_by_year — one council's REVENUE-account spend per year, the
-- "complete spending" context for the public-body (local-authority) dossier.
--
-- Source: data/silver/parquet/la_afs_divisions.parquet (extractors/la_afs_extract.py) — the
-- per-LA audited Annual Financial Statement Income & Expenditure account, parsed by service
-- division, every year reconcile-gated to the statement's own printed total.
--
-- ⚠️ GRAIN — this is a BUDGET/accounts fact, a SIBLING the procurement award/payment facts sit
-- inside; it is NEVER summed or unioned with eTenders/TED award ceilings or PO/payment euros.
-- gross_expenditure is the I&E account's operating expenditure BY DIVISION (excludes inter-
-- account transfers and the capital programme) — it is NOT the council's headline total outturn.
-- Label it "spending (revenue account)", never "total spend". Capital lives in a separate fact
-- (la_afs_capital_divisions). Sum only within a (council, year).
CREATE OR REPLACE VIEW v_procurement_afs_total_by_year AS
SELECT
    council,
    slug,
    region,
    year,
    SUM(gross_expenditure)        AS gross_expenditure_eur,  -- Σ operating expenditure by service
    SUM(income)                   AS income_eur,             -- Σ income/grants offsetting it
    SUM(net_expenditure)          AS net_expenditure_eur,    -- Σ net cost to the council
    COUNT(*)                      AS n_divisions,
    MAX(printed_total_eur)        AS printed_total_eur,       -- the statement's own printed total (reconcile anchor)
    bool_and(reconciled)          AS reconciled,
    MAX(parser)                   AS parser
FROM read_parquet('data/silver/parquet/la_afs_divisions.parquet')
GROUP BY council, slug, region, year;
