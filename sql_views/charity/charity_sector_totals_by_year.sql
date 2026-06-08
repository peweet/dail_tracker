-- v_charity_sector_totals_by_year — register-wide charity money per year.
-- Source: v_charity_financials_by_year (one row per charity-year).
--
-- Rolls the per-charity series up to a single sector total per year: how much
-- income/expenditure flowed through the registered-charity sector, and how much
-- of it was government/local-authority funding. This is the "decade totals"
-- story the per-charity snapshot could never tell.
--
-- Aggregation lives HERE (in the view), not in the query layer, so the core
-- query function stays a plain SELECT. Totals are sums of as-filed figures —
-- resilient to the rare outlier filing only in aggregate; treat a single year's
-- spike as a data-quality signal, not a real jump.

CREATE OR REPLACE VIEW v_charity_sector_totals_by_year AS
SELECT
    period_year,
    COUNT(DISTINCT rcn)            AS n_charities,
    SUM(gross_income)             AS total_gross_income,
    SUM(gross_expenditure)        AS total_gross_expenditure,
    SUM(income_govt_or_la)        AS total_income_govt_or_la
FROM v_charity_financials_by_year
WHERE period_year IS NOT NULL
GROUP BY period_year
ORDER BY period_year;
