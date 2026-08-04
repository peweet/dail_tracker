MODEL (
  name charity_pilot.financials_by_year,
  kind FULL,
  grain (rcn, period_year),
  audits (
    not_null(columns := (rcn, period_year)),
    unique_combination_of_columns(columns := (rcn, period_year)),
    number_of_rows(threshold := 1)
  )
);

SELECT
  rcn,
  registered_charity_name,
  period_year,
  CAST(period_end_date AS DATE) AS period_end_date,
  gross_income,
  gross_expenditure,
  surplus_deficit,
  gov_share,
  income_govt_or_la,
  income_other_public_bodies,
  income_donations,
  income_trading,
  income_other,
  total_assets,
  net_assets,
  total_liabilities,
  cash_at_hand,
  employees_full_time,
  employees_part_time,
  employees_band,
  volunteers_band
FROM READ_PARQUET('../../../data/silver/charities/annual_reports.parquet')
WHERE
  rcn IS NOT NULL
  AND period_year IS NOT NULL
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY rcn, period_year
    ORDER BY period_end_date DESC NULLS LAST
  ) = 1;

