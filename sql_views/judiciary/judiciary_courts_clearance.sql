-- v_courts_clearance — annual case clearance by court, 2017–2024 (system health).
-- Source: data/gold/parquet/judiciary_courts_clearance.parquet
--   (Courts Service annual statistics, https://data.courts.ie, CC-BY 4.0;
--   promoted by extractors/judiciary_bench_extract.py at source grain
--   jurisdiction × area_of_law × category × year).
--
-- This view owns the metric (logic firewall): it aggregates the per-category source
-- rows to one row per court per year and computes
--     clearance_pct = resolved / incoming * 100.
-- clearance_pct LEGITIMATELY exceeds 100 when a court resolves more cases than it
-- received that year (i.e. it cut into its backlog) — that is a real signal and is
-- NEVER capped here or downstream. 0-incoming category rows fold into the SUM; a
-- court-year with no incoming cases at all yields NULL clearance_pct (NULLIF guard),
-- not a divide error. SCOPE: case throughput only — no judge is named or implied.
CREATE OR REPLACE VIEW v_courts_clearance AS
SELECT
    jurisdiction,
    year,
    SUM(incoming)                                          AS incoming,
    SUM(resolved)                                          AS resolved,
    ROUND(SUM(resolved) * 100.0 / NULLIF(SUM(incoming), 0), 1) AS clearance_pct,
    ANY_VALUE(source_name)                                 AS source_name,
    ANY_VALUE(source_url)                                  AS source_url
FROM read_parquet('data/gold/parquet/judiciary_courts_clearance.parquet')
GROUP BY jurisdiction, year
ORDER BY year, jurisdiction;
