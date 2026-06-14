-- v_procurement_expiring_contracts_etenders — NATIONAL (eTenders) contracts projected to an
-- estimated end date: the "expiring contracts / re-tender pipeline" signal Tussell & Stotles sell,
-- reconstructed from data we already hold. Companion to v_procurement_expiring_contracts (which does
-- the same for TED EU-journal notices); this covers the much larger national eTenders award set.
--
-- ESTIMATED END = award/created date + "Contract Duration (Months)". This is an ADVERTISED TERM,
-- NOT A VERIFIED EVENT — a contract may end early, or run longer via renewals (renewals are NOT
-- folded into the estimate, deliberately). Coverage ~43% of awards (duration fill rate). Framework /
-- DPS notices are EXCLUDED: their "duration" is a framework ceiling window, not a single contract.
-- ⚠️ award value is AWARD/CEILING grade (value_kind) — DISPLAY-ONLY, never summed, never added to
-- payments or TED. Privacy: a sole_trader_or_individual winner name is withheld; the contract itself
-- (buyer + value + dates) stays listed — it is public record.
--
-- months_to_expiry is computed against a fixed reference the consuming page passes in (the registrar
-- substitutes CURRENT_DATE); kept as the raw est_end here so the view stays deterministic for tests.
CREATE OR REPLACE VIEW v_procurement_expiring_contracts_etenders AS
WITH base AS (
    SELECT
        TRY_STRPTIME("Notice Published Date/Contract Created Date", '%d/%m/%Y')::DATE   AS award_date,
        TRY_CAST(regexp_extract("Contract Duration (Months)", '[0-9]+') AS INTEGER)     AS duration_months,
        "Contracting Authority"                                                         AS buyer_name,
        "Tender/Contract Name"                                                          AS contract_name,
        "Main Cpv Code"                                                                 AS cpv_code,
        "Spend Category"                                                                AS spend_category,
        supplier                                                                        AS winner_raw,
        supplier_norm,
        supplier_class,
        value_eur,
        value_kind,
        is_framework_or_dps
    FROM read_parquet('data/gold/parquet/procurement_awards.parquet')
)
SELECT
    buyer_name,
    contract_name,
    cpv_code,
    spend_category,
    -- withhold likely-personal winner names (same quarantine as the supplier rankings); contract stays listed
    CASE WHEN supplier_class = 'sole_trader_or_individual' THEN NULL ELSE winner_raw END AS winner_display,
    supplier_norm,
    supplier_class,
    award_date,
    duration_months,
    (award_date + (duration_months || ' months')::INTERVAL)::DATE                        AS est_end_date,
    'award_date + advertised duration (term, not verified; renewals not folded)'          AS est_end_basis,
    value_eur                                                                            AS award_value_eur,
    value_kind
FROM base
WHERE award_date IS NOT NULL
  AND duration_months BETWEEN 1 AND 240          -- guard parse junk; 20yr ceiling
  AND NOT is_framework_or_dps                    -- a framework's "end" is a ceiling window, not a contract
ORDER BY est_end_date;
