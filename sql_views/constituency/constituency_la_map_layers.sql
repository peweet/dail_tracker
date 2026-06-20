-- v_la_map_layers — one row per local authority (all 31): the numeric layers behind
-- the NATIONAL CHOROPLETH on "Who runs your county" ("Every council, compared").
--
-- Each layer carries its raw value AND a precomputed NTILE(5) quintile bucket, so the
-- Streamlit page only maps bucket → colour. The quantile split is a modelling step and
-- therefore lives HERE, in the view — the page does no derivation (logic firewall).
--
-- All four layers are EXECUTIVE-function signals (the Chief Executive's administration,
-- not the elected councillors), reused verbatim from the dossier views so the map and
-- the per-council cards can never disagree:
--   commercial_rates_pct   ← v_la_collection_rates   (NOAC M2 2024)
--   derelict_outstanding_eur ← v_la_derelict_sites_levy (DHLGH 2024, cumulative)
--   planning_overturn_pct  ← v_la_planning_overturn   (An Bord Pleanála, 2016 on)
--   housing_vacancy_pct    ← v_la_housing_performance (NOAC H-series 2024)
--
-- The spine is v_la_chief_executives (all 31), LEFT JOINed so every council appears even
-- where a layer is absent (e.g. Cork County is missing from the appeals source). Quintiles
-- are computed ONLY over non-null values (the *_q CTEs filter NULLs before NTILE), so a
-- missing value stays NULL → the page renders it as a no-data fill, never a false "lowest".
--
-- ⚠️ Registration order: JOINs the four v_la_* views above, so it MUST register AFTER them
-- (handled in connections.CONSTITUENCY_FILES — _load_sql silently swallows a CatalogException).
CREATE OR REPLACE VIEW v_la_map_layers AS
WITH cr_q AS (
    SELECT local_authority, NTILE(5) OVER (ORDER BY commercial_rates_pct) AS q
    FROM v_la_collection_rates WHERE commercial_rates_pct IS NOT NULL
),
der_q AS (
    SELECT local_authority, NTILE(5) OVER (ORDER BY cumulative_outstanding_eur) AS q
    FROM v_la_derelict_sites_levy WHERE cumulative_outstanding_eur IS NOT NULL
),
pov_q AS (
    SELECT local_authority, NTILE(5) OVER (ORDER BY overturn_rate_pct) AS q
    FROM v_la_planning_overturn WHERE overturn_rate_pct IS NOT NULL
),
vac_q AS (
    SELECT local_authority, NTILE(5) OVER (ORDER BY vacancy_pct) AS q
    FROM v_la_housing_performance WHERE vacancy_pct IS NOT NULL
)
SELECT
    ce.local_authority,
    cr.commercial_rates_pct,
    der.cumulative_outstanding_eur AS derelict_outstanding_eur,
    pov.overturn_rate_pct          AS planning_overturn_pct,
    vac.vacancy_pct                AS housing_vacancy_pct,
    cr_q.q  AS q_commercial_rates,
    der_q.q AS q_derelict_outstanding,
    pov_q.q AS q_planning_overturn,
    vac_q.q AS q_housing_vacancy
FROM v_la_chief_executives ce
LEFT JOIN v_la_collection_rates   cr  ON cr.local_authority  = ce.local_authority
LEFT JOIN v_la_derelict_sites_levy der ON der.local_authority = ce.local_authority
LEFT JOIN v_la_planning_overturn  pov ON pov.local_authority = ce.local_authority
LEFT JOIN v_la_housing_performance vac ON vac.local_authority = ce.local_authority
LEFT JOIN cr_q  ON cr_q.local_authority  = ce.local_authority
LEFT JOIN der_q ON der_q.local_authority = ce.local_authority
LEFT JOIN pov_q ON pov_q.local_authority = ce.local_authority
LEFT JOIN vac_q ON vac_q.local_authority = ce.local_authority
ORDER BY ce.local_authority;
