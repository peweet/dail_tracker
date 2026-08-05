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
-- Two complementary source partitions: the dedicated DCEDIY legacy extract is canonical for
-- that department in 2023-2024, so DCEDIY rows now present in the consolidated fact are excluded
-- for those years. DCEDIY 2025+ is also excluded because the Department of Justice register covers
-- 2025+ after the IPAS transfer. Other publishers (for example Tusla) remain in the fact.
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
      -- The legacy extract below is canonical for DCEDIY 2023-2024; Justice owns 2025+.
      AND NOT (publisher_id = 'dept_children' AND year >= 2023)
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
