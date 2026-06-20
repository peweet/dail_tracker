-- v_accommodation_spend_providers — providers paid for international-protection (asylum)
-- and Ukraine accommodation, ranked by total committed spend. The "who collects the
-- money" list. One row per provider (supplier_normalised).
--
-- Same source / filter / caveats as v_accommodation_spend_by_year. amount = PO-committed.
-- Provider names are as normalised in the payments fact; some entities still fragment
-- (e.g. "MOSNEY" vs "MOSNEY HOLIDAYS") — a known supplier-spine limitation, so treat
-- ranks as indicative, not a definitive single-entity league.
CREATE OR REPLACE VIEW v_accommodation_spend_providers AS
WITH acc AS (
    SELECT
        supplier_normalised AS provider,
        CASE WHEN lower(spend_category) LIKE '%ukraine%' THEN 'Ukraine'
             ELSE 'International Protection' END AS stream,
        CAST(amount_eur AS DOUBLE) AS amount_eur,
        year
    FROM read_parquet('data/gold/parquet/procurement_payments_fact.parquet')
    WHERE value_safe_to_sum = TRUE
      AND supplier_normalised IS NOT NULL
      AND (
        lower(spend_category) LIKE '%asylum%'
        OR lower(spend_category) LIKE '%ip accommodation%'
        OR lower(spend_category) LIKE '%ukraine accommodation%'
        OR lower(spend_category) LIKE '%separated children%'
      )
)
SELECT
    provider,
    ROUND(SUM(amount_eur), 0) AS total_eur,
    ROUND(SUM(amount_eur) FILTER (WHERE stream = 'Ukraine'), 0) AS ukraine_eur,
    ROUND(SUM(amount_eur) FILTER (WHERE stream = 'International Protection'), 0) AS ip_eur,
    MIN(year) AS first_year,
    MAX(year) AS last_year
FROM acc
GROUP BY provider
HAVING SUM(amount_eur) > 0
ORDER BY total_eur DESC;
