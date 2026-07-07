-- v_procurement_payments_real — EXPERIMENTAL real-terms lens over public-body PAYMENTS, using the
-- GOVERNMENT-CONSUMPTION deflator (the agency-standard index for public money — see
-- v_govt_consumption_deflator; NOT CPI). ADDITIVE: nominal amount_eur is untouched. Built on
-- v_procurement_payments so it inherits the privacy/extraction filters and the SPENT/COMMITTED
-- tiering. Consumption must be gated behind DAIL_EXPERIMENTAL.
-- Design: doc/PROCUREMENT_INFLATION_BENCHMARKING_DESIGN.md.
--
-- real_caveat (amount_eur_real is non-NULL IFF real_caveat = 'OK'):
--   NO_VALUE     — no euro to adjust
--   IMPLAUSIBLE  — amount outside [€100, €500m] (parse artefact / not a single plausible line)
--   YEAR_MISSING — payment year outside the deflator (currently 2025+): real = NULL, never x1.0
--   OK           — adjustable
-- ⚠️ Real-terms re-expresses purchasing power in the deflator's base year (currently 2024);
-- it is NOT a cost today. NEVER sum across realisation_tier (SPENT vs COMMITTED) or vat_status.
CREATE OR REPLACE VIEW v_procurement_payments_real AS
SELECT
    p.*,
    g.base_year                       AS real_base_year,
    'CSO_GOV_CONSUMPTION'             AS deflator_index,
    g.deflator_to_base                AS deflator_factor,
    CASE
        WHEN p.amount_eur IS NULL                                     THEN NULL
        WHEN NOT (p.amount_eur >= 100 AND p.amount_eur <= 500000000)  THEN NULL
        WHEN g.deflator_to_base IS NULL                               THEN NULL
        ELSE p.amount_eur * g.deflator_to_base
    END                               AS amount_eur_real,
    CASE
        WHEN p.amount_eur IS NULL                                     THEN 'NO_VALUE'
        WHEN NOT (p.amount_eur >= 100 AND p.amount_eur <= 500000000)  THEN 'IMPLAUSIBLE'
        WHEN g.deflator_to_base IS NULL                               THEN 'YEAR_MISSING'
        ELSE 'OK'
    END                               AS real_caveat
FROM v_procurement_payments p
LEFT JOIN v_govt_consumption_deflator g ON p.year = g.year;
