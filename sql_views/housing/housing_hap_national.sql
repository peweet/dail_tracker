-- v_housing_hap_national — national profile of the Housing Assistance Payment (HAP)
-- scheme: the state's main private-rental subsidy. Single row. The "working family on
-- a state rent subsidy, years from permanent housing" picture behind the headline
-- HAP household count. All from the CSO 'Ireland' national rows (latest = 2022).
--
-- Sources (gold, CSO PxStat):
--   cso_hap01 — Number of Households in HAP (All Family Types, latest year)
--   cso_hap17 — % of (All) HAP tenant households working in any employment
--   cso_hap20 — median HAP rent paid by the tenant as a % of disposable income
--   cso_hap32 — median years on the waiting list + HAP before reaching social housing
-- HAP detail ends 2022 (the CSO series stops there) — label as latest-available.
CREATE OR REPLACE VIEW v_housing_hap_national AS
WITH households AS (
    SELECT SUM(CAST(VALUE AS DOUBLE)) AS hap_households, MAX(Year) AS hap_period
    FROM read_parquet('data/gold/parquet/cso_hap01.parquet')
    WHERE "Statistic Label" = 'Number of Households in HAP' AND "Family Type" = 'All Family Types'
      AND Year = (SELECT MAX(Year) FROM read_parquet('data/gold/parquet/cso_hap01.parquet'))
),
working AS (
    SELECT CAST(VALUE AS DOUBLE) AS pct_working
    FROM read_parquet('data/gold/parquet/cso_hap17.parquet')
    WHERE "Statistic Label" = 'Percentage of Households in HAP that are working in Any Employment'
      AND "Local Authority" = 'Ireland' AND "HAP Tenants" = 'All HAP Tenants'
      AND Year = (SELECT MAX(Year) FROM read_parquet('data/gold/parquet/cso_hap17.parquet'))
),
burden AS (
    SELECT CAST(VALUE AS DOUBLE) AS rent_pct_of_disposable_income
    FROM read_parquet('data/gold/parquet/cso_hap20.parquet')
    WHERE "Statistic Label" = 'Median HAP Rent Paid by HAP Tenant as a % of Disposable Income'
      AND "Local Authority" = 'Ireland'
      AND Year = (SELECT MAX(Year) FROM read_parquet('data/gold/parquet/cso_hap20.parquet'))
),
wait AS (
    SELECT CAST(VALUE AS DOUBLE) AS median_years_to_social_housing
    FROM read_parquet('data/gold/parquet/cso_hap32.parquet')
    WHERE "Statistic Label" = 'Median Time (in Years) on Housing Waiting List and HAP to Social Housing'
      AND "Local Authority" = 'Ireland'
)
SELECT
    h.hap_households,
    h.hap_period,
    w.pct_working,
    b.rent_pct_of_disposable_income,
    wt.median_years_to_social_housing
FROM households h CROSS JOIN working w CROSS JOIN burden b CROSS JOIN wait wt;
