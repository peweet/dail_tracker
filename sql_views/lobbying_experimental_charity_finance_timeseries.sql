-- ════════════════════════════════════════════════════════════════════════════
-- v_experimental_charity_finance_timeseries
-- ════════════════════════════════════════════════════════════════════════════
--
-- STATUS: EXPERIMENTAL — backs the Stage 2 financial section on
--         utility/pages_code/lobbyist_poc.py.
--
-- Filename matches the `lobbying_*.sql` glob in
-- utility/data_access/lobbying_data.py — auto-loads with the rest. No loader
-- changes needed.
--
-- INPUT:
--   data/silver/charities/annual_reports.parquet  (pipeline_sandbox/charity_normalise.py)
--
-- GRAIN: one row per (RCN, period_year). When a charity files multiple
-- accounts in the same calendar year (rare), the row with the latest
-- period_end_date wins, since that report is the one their public profile
-- effectively shows.
--
-- OUTPUT COLUMNS (per CRO/INTEGRATION_PLAN.md §8.2 + financial detail expansion):
--   rcn, period_year, period_end_date,
--   gross_income, gross_expenditure, surplus_deficit,
--   gov_share, gov_eur, other_public_bodies_eur,
--   donations_eur, trading_eur, philanthropic_eur, other_income_eur, bequests_eur,
--   total_assets, total_liabilities, net_assets, cash_at_hand,
--   employees_band, volunteers_band
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW v_experimental_charity_finance_timeseries AS
WITH raw AS (
    SELECT
        rcn,
        period_year,
        period_end_date,
        gross_income,
        gross_expenditure,
        surplus_deficit,
        gov_share,
        income_govt_or_la              AS gov_eur,
        income_other_public_bodies     AS other_public_bodies_eur,
        income_donations               AS donations_eur,
        income_trading                 AS trading_eur,
        income_philanthropic_orgs      AS philanthropic_eur,
        income_other                   AS other_income_eur,
        income_bequests                AS bequests_eur,
        cash_at_hand,
        total_assets,
        total_liabilities,
        net_assets,
        employees_band,
        volunteers_band
    FROM read_parquet('data/silver/charities/annual_reports.parquet')
    WHERE rcn IS NOT NULL AND period_year IS NOT NULL
)
SELECT
    rcn,
    period_year,
    CAST(period_end_date AS VARCHAR) AS period_end_date,
    gross_income,
    gross_expenditure,
    surplus_deficit,
    gov_share,
    gov_eur,
    other_public_bodies_eur,
    donations_eur,
    trading_eur,
    philanthropic_eur,
    other_income_eur,
    bequests_eur,
    cash_at_hand,
    total_assets,
    total_liabilities,
    net_assets,
    employees_band,
    volunteers_band
FROM raw
QUALIFY ROW_NUMBER() OVER (PARTITION BY rcn, period_year ORDER BY period_end_date DESC) = 1
ORDER BY rcn ASC, period_year DESC;
