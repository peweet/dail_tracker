-- v_procurement_afs_capital_by_year — one council's CAPITAL-account investment per year: the
-- "what your council is building / acquiring" spine of the local-authority dossier's BUILDING lane.
--
-- Source: data/silver/parquet/la_afs_capital_divisions.parquet (extractors/la_afs_capital_extract.py)
-- — the per-LA audited "Analysis of Expenditure and Income on Capital Account" appendix, parsed by
-- service division, every year reconcile-gated to the statement's own printed total.
--
-- ⚠️ GRAIN — capital is a THIRD, DISTINCT fact. It is NEVER summed, unioned or reconciled with the
-- revenue account (v_procurement_afs_*_by_*), the PO/payment fact, or award ceilings. A capital build
-- euro (a road, a housing scheme) and a revenue net-cost euro (running a service for a year) are not
-- the same money. The revenue I&E account shows housing netting to ~€0 (rents/HAP recoupment pass
-- through); the ACTUAL housing investment lives here. Label it "invested / built", never "spent" or
-- "total spend". Sum only within a (council, year). capital_income (grants/loans/levies that finance
-- the programme) is carried but is NOT a sibling to revenue income — different account entirely.
CREATE OR REPLACE VIEW v_procurement_afs_capital_by_year AS
SELECT
    council,
    slug,
    region,
    year,
    SUM(capital_expenditure)   AS capital_expenditure_eur,  -- Σ investment by service that year
    SUM(capital_income)        AS capital_income_eur,        -- Σ grants/loans/levies financing it
    COUNT(*)                   AS n_divisions,
    bool_and(reconciled)       AS reconciled,
    MAX(parse_method)          AS parser
FROM read_parquet('data/silver/parquet/la_afs_capital_divisions.parquet')
GROUP BY council, slug, region, year;
