-- v_procurement_la_budget_vs_actual — a council's ADOPTED budget (BUDGETED grain, DHLGH
-- consolidated publication) set BESIDE its audited AFS outturn (accounts grain, the council's
-- own audited I&E statement) for the same (council, year, division).
--
-- ⚠️ SIDE-BY-SIDE, NEVER SUMMED: the two columns are different money grains (plan vs audited
-- actual) — this view exists so a page can show "budgeted €X, outturn €Y" honestly. The delta
-- is computed HERE (pipeline, never the UI) and is the only arithmetic allowed between the
-- grains. Basis caveat: the AFS gross is operating expenditure by division (excl. transfers),
-- the adopted budget is the pre-year plan on the same divisional layout — deltas of a few % are
-- normal; do not present the delta as an error or overspend verdict.
CREATE OR REPLACE VIEW v_procurement_la_budget_vs_actual AS
SELECT
    b.council,
    b.year,
    b.division,
    b.expenditure_adopted     AS budget_expenditure_eur,
    a.gross_expenditure       AS afs_gross_expenditure_eur,
    ROUND(a.gross_expenditure - b.expenditure_adopted, 0)  AS outturn_minus_budget_eur,
    CASE WHEN b.expenditure_adopted > 0
         THEN ROUND(100.0 * (a.gross_expenditure - b.expenditure_adopted) / b.expenditure_adopted, 1)
    END                       AS outturn_vs_budget_pct
FROM read_parquet('data/silver/parquet/la_budget_divisions.parquet') b
JOIN read_parquet('data/silver/parquet/la_afs_divisions.parquet') a
  ON a.council = b.council AND a.year = b.year AND a.division = b.division;
