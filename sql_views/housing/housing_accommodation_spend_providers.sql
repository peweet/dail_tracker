-- v_accommodation_spend_providers — providers paid for international-protection (asylum)
-- and Ukraine accommodation, ranked by total committed spend. The "who collects the
-- money" list. One row per provider.
--
-- UNION of the published payments fact + the DCEDIY 2023-2024 legacy extract (the years
-- IPAS sat under the Dept of Children). Disjoint publishers => additive, no double-count;
-- DCEDIY 2025+ excluded (Dept of Justice covers 2025+ in the fact). amount = PO-committed.
-- Provider names are as published; some entities still fragment (e.g. "MOSNEY" vs
-- "MOSNEY HOLIDAYS") — a known supplier-spine limitation, so treat ranks as indicative.
--
-- PRIVACY GATE — this view is NAME-LEVEL (one row per provider), so it is filtered to
-- public_display = TRUE, same as v_public_payments: rows quarantined upstream as
-- personal/sole-trader (privacy_status = 'review_personal_data') never surface a name here.
-- The aggregate totals view (v_accommodation_spend_by_year) intentionally keeps the full,
-- ungated sum — only NAMES are gated, not the anonymous spend total.
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
      AND public_display = TRUE
      AND supplier_normalised IS NOT NULL
      AND (
        lower(spend_category) LIKE '%asylum%'
        OR lower(spend_category) LIKE '%ip accommodation%'
        OR lower(spend_category) LIKE '%ukraine accommodation%'
        OR lower(spend_category) LIKE '%separated children%'
      )
    UNION ALL
    -- The DCEDIY legacy extract has NO public_display column (it predates the privacy
    -- classifier), so quarantine is applied by anti-join against the fact's own
    -- personal-data list: any normalised name the pipeline has flagged as
    -- review_personal_data anywhere in the fact is withheld here too. Residual: a
    -- personal name that appears ONLY in the legacy extract is not caught — closing
    -- that needs the upstream classifier run over the legacy extract (pipeline-owned).
    SELECT provider, stream, CAST(amount_eur AS DOUBLE) AS amount_eur, year
    FROM read_parquet('data/gold/parquet/dceidy_ipas_legacy_spend.parquet')
    WHERE year IN (2023, 2024) AND provider IS NOT NULL
      AND provider NOT IN (
          SELECT DISTINCT supplier_normalised
          FROM read_parquet('data/gold/parquet/procurement_payments_fact.parquet')
          WHERE privacy_status = 'review_personal_data'
            AND supplier_normalised IS NOT NULL
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
