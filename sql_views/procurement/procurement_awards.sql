-- v_procurement_awards — one row per award×supplier from the eTenders open data.
-- Source: data/gold/parquet/procurement_awards.parquet (procurement chain,
--   extractors/procurement_etenders_extract.py).
--
-- This is the raw feed. Ugly source headers are renamed to snake_case; the date
-- is parsed from DD/MM/YYYY. VALUE IS NOT SPEND: value_eur is the awarded/estimated
-- contract value — framework & DPS notices are multi-year CEILINGS and a
-- multi-supplier framework repeats one ceiling across every supplier row. Only
-- value_safe_to_sum rows may be summed, and even then it is "awarded value, not
-- actual expenditure". name_truncated flags an OGP source defect (dropped leading
-- capital) — kept here, excluded from rankings/CRO matching downstream.
CREATE OR REPLACE VIEW v_procurement_awards AS
SELECT
    "Tender ID"                                  AS tender_id,
    supplier,
    supplier_norm,
    supplier_class,
    name_truncated,
    "Contracting Authority"                      AS contracting_authority,
    "Main Cpv Code"                              AS cpv_code,
    "Main Cpv Code Description"                  AS cpv_description,
    -- Detail fields (2026-06-12). tender_title is the actual contract name (100% filled
    -- on award rows) — before this a line item's only description was its CPV label.
    -- category_label is the display fallback: Main Cpv Code is filled on only ~30% of
    -- award rows, but ~69% of CPV-less rows carry an OGP Spend Category. The fallback is
    -- LABELLED-AS-IS display copy (two different taxonomies, never grouped together) —
    -- per-CPV rollups keep using cpv_code/cpv_description only.
    "Tender/Contract Name"                       AS tender_title,
    "Spend Category"                             AS spend_category,
    COALESCE("Main Cpv Code Description", "Spend Category") AS category_label,
    "Contract Type"                              AS contract_type,
    "Procedure"                                  AS procedure_type,
    TRY_CAST("Contract Duration (Months)" AS INTEGER)  AS contract_duration_months,
    TRY_CAST("No of Bids Received" AS INTEGER)         AS n_bids_received,
    TRY_CAST("No of SMEs Bids Received" AS INTEGER)    AS n_sme_bids_received,
    TRY_CAST("No of Awarded SMEs" AS INTEGER)          AS n_awarded_smes,
    -- Pre-award ESTIMATE from the notice header (~27% filled): display-only context,
    -- never summed, never a substitute for value_eur.
    estimated_value_eur,
    "Additional CPV Codes on CFT"                AS additional_cpv_codes,
    -- Deep links to the EU Official Journal notice (above-EU-threshold subset, ~25%).
    "TED Notice Link"                            AS ted_notice_link,
    "TED CAN Link"                               AS ted_can_link,
    -- Deep link to the AUTHORITATIVE national notice on eTenders. The OGP "Tender ID" is the
    -- eTenders (European Dynamics EPPS) resource id — confirmed: it templates straight into the
    -- public notice URL the live-tender scraper also captures (.../prepareViewCfTWS.do?resourceId=<id>),
    -- and resolves to the real notice (a garbage id falls back to the CAS login page). This gives
    -- the sub-EU-threshold mass (the ~75% with no TED link) a path to its source notice. NULL where
    -- the source dropped the Tender ID (~7%), so the page links only when a real notice exists.
    CASE WHEN "Tender ID" IS NOT NULL AND TRIM("Tender ID") NOT IN ('', 'NULL')
         THEN 'https://www.etenders.gov.ie/epps/cft/prepareViewCfTWS.do?resourceId=' || TRIM("Tender ID")
    END                                          AS etenders_notice_url,
    "Competition Type"                           AS competition_type,
    TRY_STRPTIME("Notice Published Date/Contract Created Date", '%d/%m/%Y')::DATE AS award_date,
    value_eur,
    value_kind,
    is_framework_or_dps,
    value_shared_across_suppliers,
    value_safe_to_sum,
    -- Framework nesting, made visible: a call-off is a drawdown under a framework/DPS
    -- (its parent agreement). Carried so award histories can label call-off rows; the
    -- parent-resolution join lives in v_procurement_call_off_links.
    is_call_off,
    "Parent Agreement ID"                        AS parent_agreement_id
FROM read_parquet('data/gold/parquet/procurement_awards.parquet');
