-- v_la_noac_scorecard — five citizen-facing NOAC 2024 accountability indicators per
-- council, each beside the national median across the 31 councils. Powers the two new
-- "Who Runs Your County" dossier cards (How the council is run / Services to residents).
-- All five are EXECUTIVE responsibilities (Chief Executive's administration), shown as
-- published value vs benchmark — no composite score, no good/bad verdict.
--
-- Source: data/gold/parquet/noac_scorecard_wide.parquet — one row per LA (2024),
-- extracted from NOAC's PDF tables via PyMuPDF by extractors/noac_scorecard_extract.py.
--
-- Grain: one row per council (2024). la is mapped from NOAC's naming ("Carlow County",
-- "Limerick City and County") to the local_authority join key by the explicit 31-row
-- table — the SAME map used by v_la_collection_rates (a string-strip would mis-map the
-- City-and-County ones). National medians are window aggregates so the page renders
-- value-vs-benchmark with no derivation.
--
-- Derivation lives here (firewall): litter_problem_pct = moderately + significantly +
-- grossly polluted (NOAC's "unpolluted" grade is noisy — councils split it arbitrarily
-- against "slightly polluted"). Fire is service-NULLed for the four authorities with no
-- own brigade (Dublin Fire Brigade covers Dun Laoghaire-Rathdown / Fingal / South Dublin;
-- Galway City is covered by Galway County) so a 0 is never shown as worst-performer; the
-- median ignores those NULLs.
CREATE OR REPLACE VIEW v_la_noac_scorecard AS
WITH la_map(local_authority, noac_la) AS (
    VALUES
    ('Carlow', 'Carlow County'), ('Cavan', 'Cavan County'), ('Clare', 'Clare County'),
    ('Cork City', 'Cork City'), ('Cork County', 'Cork County'), ('Donegal', 'Donegal County'),
    ('Dublin City', 'Dublin City'), ('Dun Laoghaire-Rathdown', 'Dun Laoghaire-Rathdown'),
    ('Fingal', 'Fingal County'), ('Galway City', 'Galway City'), ('Galway County', 'Galway County'),
    ('Kerry', 'Kerry County'), ('Kildare', 'Kildare County'), ('Kilkenny', 'Kilkenny County'),
    ('Laois', 'Laois County'), ('Leitrim', 'Leitrim County'), ('Limerick', 'Limerick City and County'),
    ('Longford', 'Longford County'), ('Louth', 'Louth County'), ('Mayo', 'Mayo County'),
    ('Meath', 'Meath County'), ('Monaghan', 'Monaghan County'), ('Offaly', 'Offaly County'),
    ('Roscommon', 'Roscommon County'), ('Sligo', 'Sligo County'), ('South Dublin', 'South Dublin County'),
    ('Tipperary', 'Tipperary County'), ('Waterford', 'Waterford City and County'),
    ('Westmeath', 'Westmeath County'), ('Wexford', 'Wexford County'), ('Wicklow', 'Wicklow County')
),
raw AS (
    SELECT
        la,
        CAST(revenue_balance_pct        AS DOUBLE) AS revenue_balance_pct,
        CAST(m3_claims_per_capita_eur   AS DOUBLE) AS insurance_claims_per_capita_eur,
        CAST(m4_central_mgmt_charge_pct AS DOUBLE) AS mgmt_overhead_pct,
        CAST(m4_payroll_pct             AS DOUBLE) AS payroll_pct,
        CAST(sickness_certified_pct     AS DOUBLE) AS sickness_absence_pct,
        CAST(roads_poor_pct             AS DOUBLE) AS roads_poor_pct,
        CAST(fire_within_10min_pct      AS DOUBLE) AS fire_within_10min_pct,
        CAST(litter_moderate_pct AS DOUBLE)
            + CAST(litter_significant_pct AS DOUBLE)
            + CAST(litter_grossly_pct AS DOUBLE) AS litter_problem_pct
    FROM read_parquet('data/gold/parquet/noac_scorecard_wide.parquet')
    WHERE year = 2024
),
j AS (
    SELECT
        lm.local_authority,
        r.revenue_balance_pct,
        r.insurance_claims_per_capita_eur,
        r.mgmt_overhead_pct,
        r.payroll_pct,
        r.sickness_absence_pct,
        r.roads_poor_pct,
        r.litter_problem_pct,
        CASE WHEN lm.local_authority
                  IN ('Dun Laoghaire-Rathdown', 'Fingal', 'South Dublin', 'Galway City')
             THEN NULL ELSE r.fire_within_10min_pct END        AS fire_within_10min_pct
    FROM la_map lm
    JOIN raw r ON r.la = lm.noac_la
)
SELECT
    local_authority,
    2024                                                       AS year,
    revenue_balance_pct,
    insurance_claims_per_capita_eur,
    mgmt_overhead_pct,
    payroll_pct,
    sickness_absence_pct,
    roads_poor_pct,
    fire_within_10min_pct,
    litter_problem_pct,
    ROUND(MEDIAN(revenue_balance_pct)  OVER (), 1)             AS nat_revenue_balance_pct,
    ROUND(MEDIAN(insurance_claims_per_capita_eur) OVER (), 2)  AS nat_insurance_claims_per_capita_eur,
    ROUND(MEDIAN(mgmt_overhead_pct)    OVER (), 1)             AS nat_mgmt_overhead_pct,
    ROUND(MEDIAN(payroll_pct)          OVER (), 1)             AS nat_payroll_pct,
    ROUND(MEDIAN(sickness_absence_pct) OVER (), 1)             AS nat_sickness_absence_pct,
    ROUND(MEDIAN(roads_poor_pct)       OVER (), 1)             AS nat_roads_poor_pct,
    ROUND(MEDIAN(fire_within_10min_pct) OVER (), 1)           AS nat_fire_within_10min_pct,
    ROUND(MEDIAN(litter_problem_pct)   OVER (), 1)             AS nat_litter_problem_pct
FROM j
ORDER BY local_authority;
