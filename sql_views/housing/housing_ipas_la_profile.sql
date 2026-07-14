-- v_ipas_la_profile — international-protection applicants by LOCAL AUTHORITY, with
-- population and a real per-1,000 rate. The council-map contract.
--
-- Source: IPAS weekly accommodation & arrivals statistics (gov.ie), snapshot 2024-12-29.
-- The 31 LA values sum EXACTLY to the report's own Grand Total (32,702) — validated at
-- extraction. Population is CSO Census 2022 (CC-BY); `ip_per_1000_population` is the metric
-- the C&AG's own Figure 10.2 choropleth uses, which the C&AG published only as BANDS.
--
-- GRAIN: a point-in-time HEADCOUNT per LA, not a flow. value_safe_to_sum is FALSE — the
-- LA values do sum to the national total, but the column must never be summed with money
-- or unioned with any other fact. `cag_band` reproduces the C&AG's published banding so the
-- map can be read against the auditor's own.
--
-- UNKNOWNS ARE PRESERVED: where population could not be mapped, ip_per_1000_population is
-- NULL and population_unknown_reason says why. Never impute it.
CREATE OR REPLACE VIEW v_ipas_la_profile AS
SELECT
    local_authority,
    ip_applicants,
    population_2022,
    ip_per_1000_population,
    cag_band,
    snapshot_date,
    population_census_year,
    population_unknown_reason,
    source_url_ip_applicants,
    source_url_population,
    provenance_footer,
    value_safe_to_sum
FROM read_parquet('data/gold/parquet/ipas_la_profile.parquet')
ORDER BY ip_applicants DESC;
