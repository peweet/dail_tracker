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
-- COVERAGE: strong 2016-2019 (Dept of Justice/Reception & Integration) + 2025-2026
-- (Dept of Justice, IPAS). The 2020-2024 surge sat under the Dept of Children (DCEDIY),
-- whose register is not yet harvested — so those years are UNDER-COUNTED here. The page
-- states this and shows the C&AG 2024 denominator (~€978m commercial / €1.1bn total).
CREATE OR REPLACE VIEW v_accommodation_spend_by_year AS
WITH acc AS (
    SELECT
        year,
        CASE WHEN lower(spend_category) LIKE '%ukraine%' THEN 'Ukraine'
             ELSE 'International Protection' END AS stream,
        CAST(amount_eur AS DOUBLE) AS amount_eur,
        supplier_normalised
    FROM read_parquet('data/gold/parquet/procurement_payments_fact.parquet')
    WHERE value_safe_to_sum = TRUE
      AND (
        lower(spend_category) LIKE '%asylum%'
        OR lower(spend_category) LIKE '%ip accommodation%'
        OR lower(spend_category) LIKE '%ukraine accommodation%'
        OR lower(spend_category) LIKE '%separated children%'
      )
)
SELECT
    year,
    ROUND(SUM(amount_eur) FILTER (WHERE stream = 'International Protection'), 0) AS ip_eur,
    ROUND(SUM(amount_eur) FILTER (WHERE stream = 'Ukraine'), 0) AS ukraine_eur,
    ROUND(SUM(amount_eur), 0) AS total_eur,
    COUNT(DISTINCT supplier_normalised) AS n_providers
FROM acc
GROUP BY year
ORDER BY year;
