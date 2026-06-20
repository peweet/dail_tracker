-- v_la_housing_performance — per-council social-housing MANAGEMENT performance
-- (NOAC H-series, 2024), each metric beside the national median. How well the
-- executive runs the council's own housing stock: how much sits empty, how long it
-- takes to re-let, upkeep spend per home, retrofit progress, and long-term
-- homelessness. All EXECUTIVE functions (the CE's administration), so this belongs
-- on the "Who runs your county" dossier next to collection / planning / dereliction.
--
-- DRY: collapses the already-built, already-tested
-- v_constituency_council_housing_performance (which repeats each council's figures
-- per serving constituency) to ONE row per council via DISTINCT. The metric + median
-- columns are council-grain there, so DISTINCT is exact.
--
-- ⚠️ Registration order: reads v_constituency_council_housing_performance, so it MUST
-- register AFTER it (handled in connections.CONSTITUENCY_FILES).
-- Source provenance: NOAC Performance Indicator Report 2024 H1-H7 gold parquets.
CREATE OR REPLACE VIEW v_la_housing_performance AS
SELECT DISTINCT
    local_authority,
    noac_period                          AS year,
    vacancy_pct,
    reletting_weeks,
    maintenance_eur_per_dwelling,
    retrofit_pct_of_stock,
    longterm_homeless_pct,
    nat_vacancy_pct,
    nat_reletting_weeks,
    nat_maintenance_eur_per_dwelling,
    nat_retrofit_pct_of_stock,
    nat_longterm_homeless_pct
FROM v_constituency_council_housing_performance
ORDER BY local_authority;
