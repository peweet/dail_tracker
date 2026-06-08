-- v_courts_clearance_by_area — case clearance one level finer than v_courts_clearance:
-- per court × area of law × year (Civil / Criminal / Family / …), 2017–2024.
-- Source: data/gold/parquet/judiciary_courts_clearance.parquet (Courts Service annual
-- statistics, https://data.courts.ie, CC-BY 4.0; promoted by
-- extractors/judiciary_bench_extract.py). Same metric as the court-level view, computed
-- here at area grain so the page can break a court down without aggregating in-app:
--     clearance_pct = resolved / incoming * 100
-- (legitimately >100 when a court/area cut into backlog; never capped). 0-incoming
-- area-years yield NULL clearance_pct via the NULLIF guard. SCOPE: throughput only —
-- no judge named.
CREATE OR REPLACE VIEW v_courts_clearance_by_area AS
SELECT
    jurisdiction,
    area_of_law,
    year,
    SUM(incoming)                                          AS incoming,
    SUM(resolved)                                          AS resolved,
    ROUND(SUM(resolved) * 100.0 / NULLIF(SUM(incoming), 0), 1) AS clearance_pct,
    ANY_VALUE(source_name)                                 AS source_name,
    ANY_VALUE(source_url)                                  AS source_url
FROM read_parquet('data/gold/parquet/judiciary_courts_clearance.parquet')
GROUP BY jurisdiction, area_of_law, year
ORDER BY jurisdiction, area_of_law, year;
