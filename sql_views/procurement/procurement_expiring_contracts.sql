-- v_procurement_expiring_contracts — TED-advertised contract terms projected to an
-- estimated end date (the "expiring contracts" signal both Tussell and Stotles sell).
--
-- Source: data/silver/parquet/ted_ie_awards.parquet (extractors/ted_ireland_extract.py).
-- The END DATE IS AN ADVERTISED TERM, NOT A VERIFIED EVENT: contract_end_basis records
-- the derivation (explicit_end_date BT-537 > start BT-536 + duration BT-36 >
-- conclusion BT-145 + duration). Renewal options (renewal_max) are surfaced but NEVER
-- folded into the estimate — a contract may run longer via renewals or end early.
-- Coverage ~36% of award rows (eForms fill rates; early-2024 transition notices sparser).
--
-- Grain: one row per NOTICE (winners aggregated for display). Privacy: winner names of
-- sole_trader_or_individual class are withheld from the aggregate (same quarantine as the
-- supplier rankings); the notice itself stays listed (the buyer/contract is public record).
-- Pan-EU mega-frameworks (is_pan_eu_outlier) are excluded — Ireland is one of dozens of
-- participants and the ceiling value misleads.
-- ⚠️ award_value_eur is AWARD/CEILING grade (value_kind) — display-only, never summed.
CREATE OR REPLACE VIEW v_procurement_expiring_contracts AS
SELECT
    publication_number,
    ANY_VALUE(notice_url)                                        AS notice_url,
    -- same buyer-name cleanup as v_procurement_competition (eForms _NNN / (ID N) artefacts)
    trim(regexp_replace(regexp_replace(ANY_VALUE(buyer_name), '_[0-9]+$', ''),
                        '\s*\(ID\s*[0-9]+\)$', ''))              AS buyer_name,
    STRING_AGG(DISTINCT regexp_replace(winner_name, '_[0-9]+$', ''), '; ')
        FILTER (WHERE winner_name IS NOT NULL
                  AND supplier_class != 'sole_trader_or_individual')  AS winners_display,
    ANY_VALUE(n_winners)                                         AS n_winners,
    ANY_VALUE(cpv_code)                                          AS cpv_code,
    ANY_VALUE(cpv_division)                                      AS cpv_division,
    ANY_VALUE(award_value_eur)                                   AS award_value_eur,
    ANY_VALUE(value_kind)                                        AS value_kind,
    ANY_VALUE(is_multi_supplier_framework)                       AS is_multi_supplier_framework,
    TRY_CAST(ANY_VALUE(contract_conclusion_date) AS DATE)        AS contract_conclusion_date,
    ANY_VALUE(contract_duration_months)                          AS contract_duration_months,
    ANY_VALUE(renewal_max)                                       AS renewal_max,
    TRY_CAST(ANY_VALUE(contract_end_date_est) AS DATE)           AS contract_end_date_est,
    ANY_VALUE(contract_end_basis)                                AS contract_end_basis,
    TRY_CAST(ANY_VALUE(dispatch_date) AS DATE)                   AS dispatch_date,
    ANY_VALUE(year)                                              AS year
FROM read_parquet('data/silver/parquet/ted_ie_awards.parquet')
WHERE contract_end_date_est IS NOT NULL
  AND NOT is_pan_eu_outlier
GROUP BY publication_number
ORDER BY contract_end_date_est ASC;
