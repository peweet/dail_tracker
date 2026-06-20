-- v_constituency_map_layers — one row per constituency (all 43): the numeric layers
-- behind the NATIONAL CHOROPLETH on the constituency index ("Compare every area").
--
-- Each layer carries its raw value AND a precomputed NTILE(5) quintile bucket, so the
-- Streamlit page only maps bucket → colour. The quantile split is a modelling step and
-- therefore lives HERE, in the view — the page does no derivation (logic firewall).
--
-- TRUE constituency-grain ONLY. Representation comes from v_constituency_registry and
-- the accountability/activity layers from v_constituency_house_work (this constituency's
-- CURRENT Dáil TDs, 34th Dáil since 2024-11-29). LA-grain housing facts (vacancy, prices,
-- SSHA, NOAC) are deliberately EXCLUDED: a constituency can span several local authorities,
-- so they have no single per-constituency value a choropleth fill could honestly show.
-- pct_landlord_tds uses ONLY the reliable landlord flag (see v_constituency_house_work).
--
-- Registers AFTER v_constituency_registry and v_constituency_house_work (dependency order
-- in connections.CONSTITUENCY_FILES — _load_sql silently swallows a CatalogException).
CREATE OR REPLACE VIEW v_constituency_map_layers AS
WITH base AS (
    SELECT
        r.constituency_name,
        r.population_2022,
        r.population_per_td,
        r.td_seats,
        r.n_tds_current,
        COALESCE(hw.n_questions, 0)  AS n_questions,
        COALESCE(hw.n_landlords, 0)  AS n_landlords,
        CASE WHEN r.n_tds_current > 0
             THEN 100.0 * COALESCE(hw.n_landlords, 0) / r.n_tds_current END AS pct_landlord_tds,
        CASE WHEN r.n_tds_current > 0
             THEN 1.0 * COALESCE(hw.n_questions, 0) / r.n_tds_current END  AS questions_per_td
    FROM v_constituency_registry r
    LEFT JOIN v_constituency_house_work hw
           ON hw.constituency_name = r.constituency_name
)
SELECT
    constituency_name,
    population_2022,
    population_per_td,
    td_seats,
    n_tds_current,
    n_questions,
    n_landlords,
    pct_landlord_tds,
    questions_per_td,
    -- quintile buckets (1 = lowest fifth … 5 = highest fifth). COALESCE keeps the
    -- null ordering deterministic for the rare degraded row (n_tds_current = 0).
    NTILE(5) OVER (ORDER BY population_2022)                      AS q_population,
    NTILE(5) OVER (ORDER BY population_per_td)                    AS q_population_per_td,
    NTILE(5) OVER (ORDER BY COALESCE(pct_landlord_tds, 0))       AS q_pct_landlord_tds,
    NTILE(5) OVER (ORDER BY COALESCE(questions_per_td, 0))       AS q_questions_per_td
FROM base
ORDER BY constituency_name;
