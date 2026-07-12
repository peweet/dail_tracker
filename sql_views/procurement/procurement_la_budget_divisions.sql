-- v_procurement_la_budget_divisions — each council's ADOPTED annual budget by service
-- division, all 31 councils, from the DHLGH consolidated Local Authority Budget publication
-- (one national PDF per year; per-column printed-total reconcile gate in the extractor).
--
-- ⚠️ FOURTH MONEY GRAIN — BUDGETED (a plan, not spend). NEVER union or sum with the PO/payment
-- grains (COMMITTED/SPENT) or the AFS accounts grains; value_safe_to_sum is False on every row.
-- Compare against audited outturn via v_procurement_la_budget_vs_actual only.
CREATE OR REPLACE VIEW v_procurement_la_budget_divisions AS
SELECT
    council,
    year,
    division,
    expenditure_adopted AS expenditure_adopted_eur,
    income_adopted      AS income_adopted_eur,
    realisation_tier,
    value_kind,
    value_safe_to_sum,
    source_url
FROM read_parquet('data/silver/parquet/la_budget_divisions.parquet');
