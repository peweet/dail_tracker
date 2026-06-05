-- v_gov_finance_annual — authoritative national general-government totals per
-- year, the DENOMINATOR for the app's public-money facts (procurement, payments,
-- LA finance). Turns an isolated € figure into a "share of total spend" or a
-- per-capita number that a reader can actually judge.
--
-- Source : data/gold/parquet/cso_gfa01.parquet
--          (extractors/cso_pxstat_extract.py — CSO PxStat GFA01,
--           "General Government Transactions ESA 2010", annual 1995–2025).
-- Grain  : one row per calendar year. GFA01's native grain is (Year × Item),
--          exactly one row per pair (31 yrs × 29 items = 899), so the conditional
--          pivot below cannot double-count.
-- Units  : CSO publishes €millions; multiplied to whole euros here for clean
--          ratios. Surplus is POSITIVE, deficit NEGATIVE (ESA2010 code B9) — e.g.
--          2024 = +€23.3bn (the Apple-CJEU windfall year), 2010 ≈ −€48bn.
-- Firewall: pure extraction/pivot. NO ratios and NO inference live here. A
--          share-of-total ("tracked spend ÷ national expenditure") belongs in a
--          DOWNSTREAM view that joins a specific tracked fact, where the metric
--          scope of that fact can be matched honestly (see project_la_afs_metric
--          _semantics — AFS division figures are NOT a council's headline total).
CREATE OR REPLACE VIEW v_gov_finance_annual AS
SELECT
    CAST("Year" AS INTEGER) AS year,
    ROUND(SUM(CASE WHEN "Item" = 'General Government transactions - Revenue - ESA2010 Code (TR)'
                   THEN CAST("VALUE" AS DOUBLE) END) * 1e6) AS revenue_eur,
    ROUND(SUM(CASE WHEN "Item" = 'General Government transactions - Expenditure - ESA2010 Code (TE)'
                   THEN CAST("VALUE" AS DOUBLE) END) * 1e6) AS expenditure_eur,
    ROUND(SUM(CASE WHEN "Item" = 'General Government Surplus/Deficit - ESA2010 Code (B9)'
                   THEN CAST("VALUE" AS DOUBLE) END) * 1e6) AS surplus_deficit_eur
FROM read_parquet('data/gold/parquet/cso_gfa01.parquet')
GROUP BY 1
ORDER BY 1;
