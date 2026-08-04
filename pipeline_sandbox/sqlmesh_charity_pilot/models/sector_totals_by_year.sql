MODEL (
  name charity_pilot.sector_totals_by_year,
  kind FULL,
  grain period_year,
  audits (
    not_null(columns := (period_year, n_charities)),
    unique_values(columns := (period_year)),
    number_of_rows(threshold := 1)
  )
);

SELECT
  period_year,
  COUNT(DISTINCT rcn) AS n_charities,
  SUM(gross_income) AS total_gross_income,
  SUM(gross_expenditure) AS total_gross_expenditure,
  SUM(income_govt_or_la) AS total_income_govt_or_la
FROM charity_pilot.financials_by_year
WHERE
  period_year IS NOT NULL
GROUP BY
  period_year;

