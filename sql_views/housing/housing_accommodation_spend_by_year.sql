-- v_accommodation_spend_by_year — State spend on international-protection (asylum) and
-- Ukraine accommodation, per year, split by stream. The "money to hotels and other
-- providers" picture, from the published over-€20k purchase-order registers.
--
-- Source: procurement_payments_fact (gold) — the regime-aware payments fact. Filtered
-- to value_safe_to_sum=TRUE (PO-committed amounts that may be summed) and to the precise
-- accommodation spend-categories below. A keyword sweep would wrongly pull in Homeless /
-- Student / Conference accommodation, Coastal/Data Protection etc., so the match is
-- deliberately narrow:
--   asylum* · 'IP Accommodation%' · 'Ukraine Accommodation' · 'Separated Children…Protection'
--
-- amount_semantics = po_committed (purchase-order committed, NOT confirmed cash).
--
-- Two complementary sources, NO overlap: the published payments fact never contained the
-- Dept of Children (DCEDIY), so the DCEDIY 2023-2024 legacy extract
-- (dceidy_ipas_legacy_spend.parquet — the years IPAS sat under DCEDIY) is purely additive.
-- DCEDIY 2025+ is EXCLUDED (the Dept of Justice register already covers 2025+ in the fact,
-- and IPAS transferred to Justice in 2025 — including both would double-count that year).
-- 2020-2022 remain thin (pre-surge; not separately published in a parsable register).
CREATE OR REPLACE VIEW v_accommodation_spend_by_year AS
WITH fact AS (
    SELECT
        year,
        CASE WHEN lower(spend_category) LIKE '%ukraine%' THEN 'Ukraine'
             ELSE 'International Protection' END AS stream,
        CAST(amount_eur AS DOUBLE) AS amount_eur,
        supplier_normalised AS provider
    FROM read_parquet('data/gold/parquet/procurement_payments_fact.parquet')
    WHERE value_safe_to_sum = TRUE
      AND (
        lower(spend_category) LIKE '%asylum%'
        OR lower(spend_category) LIKE '%ip accommodation%'
        OR lower(spend_category) LIKE '%ukraine accommodation%'
        OR lower(spend_category) LIKE '%separated children%'
      )
),
dceidy AS (
    SELECT year, stream, CAST(amount_eur AS DOUBLE) AS amount_eur, provider
    FROM read_parquet('data/gold/parquet/dceidy_ipas_legacy_spend.parquet')
    WHERE year IN (2023, 2024)
),
acc AS (SELECT * FROM fact UNION ALL SELECT * FROM dceidy)
SELECT
    year,
    ROUND(SUM(amount_eur) FILTER (WHERE stream = 'International Protection'), 0) AS ip_eur,
    ROUND(SUM(amount_eur) FILTER (WHERE stream = 'Ukraine'), 0) AS ukraine_eur,
    ROUND(SUM(amount_eur), 0) AS total_eur,
    COUNT(DISTINCT provider) AS n_providers
FROM acc
GROUP BY year
ORDER BY year;
