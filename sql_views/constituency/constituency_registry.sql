-- v_constituency_registry — one row per constituency: the dossier header spine.
-- Joins the Census-2022 demographics (population, per-TD ratio, statutory seats)
-- to a live count of the constituency's current Dáil TDs from the registry.
--
-- Demographics source: v_member_constituency_demographics (Electoral Commission
-- Constituency Review 2023, Appendix 2 — Census 2022 on the current 43 boundaries).
-- All 43 constituencies are present whether or not the registry has loaded; the
-- LEFT JOIN makes n_tds_current degrade to 0 rather than dropping the row.
CREATE OR REPLACE VIEW v_constituency_registry AS
SELECT
    d.constituency_name,
    d.population_2022,
    d.population_per_td,
    d.td_seats,                                   -- statutory seats (Electoral Commission)
    COUNT(r.unique_member_code)        AS n_tds_current,  -- TDs currently in the registry
    d.boundaries_label,
    d.source_key
FROM v_member_constituency_demographics d
LEFT JOIN v_member_registry r
       ON r.constituency = d.constituency_name
      AND r.house = 'Dáil'
GROUP BY ALL
ORDER BY d.constituency_name;
