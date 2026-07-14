-- v_ipas_property_rates — what a bed actually costs the State, per person per night.
--
-- Source: C&AG RoAPS 2024 Ch.10, Annex 10A — the auditor's sample of 20 properties, with the
-- contracted daily rate, the accommodation type, the county and the PROCUREMENT ROUTE.
-- Range: EUR 40 (a South Dublin dormitory) to EUR 170 (a Limerick hotel); median EUR 74.
-- Benchmark for context (IGEES, via C&AG 10.18): EUR 92/night privately provided vs EUR 34
-- State-owned.
--
-- The procurement_route column is the story: 14 of the 20 were DIRECT AWARDS; only 2 went to
-- tender.
--
-- rate_known=FALSE marks the 4 properties where the C&AG itself recorded the rate as "Unclear"
-- or the centre as Department-run. Those rows carry a NULL rate. Do not impute them.
--
-- GRAIN: one row per sampled property. value_safe_to_sum=FALSE — a per-night unit price is a
-- RATE, not a quantity; summing it is meaningless.
CREATE OR REPLACE VIEW v_ipas_property_rates AS
SELECT
    property_no,
    accommodation_type,
    county,
    procurement_route,
    contracted_rate_eur_per_person_night,
    rate_known,
    source_ref,
    source_url,
    value_safe_to_sum
FROM read_parquet('data/gold/parquet/ipas_property_rates.parquet')
ORDER BY contracted_rate_eur_per_person_night DESC NULLS LAST;
