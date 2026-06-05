-- v_charity_financials_by_year — per-charity annual financial time-series.
-- Source: data/silver/charities/annual_reports.parquet (Charities Regulator
--   annual-report filings, 2014→present). Gold previously kept only the latest
--   snapshot (charity_latest → charities_enriched); this view promotes the full
--   multi-year series so trends can be drawn.
--
-- Grain: ONE row per (rcn, period_year). The source can hold up to 3 filings for
-- the same charity-year (amended/re-filed returns); we keep the one with the
-- latest period_end_date — the most complete view of that year.
--
-- Figures are reported as filed (no winsorising) — a handful of returns contain
-- implausible magnitudes (data-entry errors at source). Consumers that draw
-- trend lines should be resilient to outliers; this view stays faithful to the
-- filing and does not silently clean values. gov_share is the government-funded
-- proportion of income (0–1) already computed upstream.

CREATE OR REPLACE VIEW v_charity_financials_by_year AS
SELECT
    rcn,
    registered_charity_name,
    period_year,
    CAST(period_end_date AS DATE)                     AS period_end_date,
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
FROM read_parquet('data/silver/charities/annual_reports.parquet')
WHERE rcn IS NOT NULL AND period_year IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rcn, period_year
    ORDER BY period_end_date DESC NULLS LAST
) = 1
ORDER BY rcn, period_year;
