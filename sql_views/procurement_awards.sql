-- v_procurement_awards — one row per award×supplier from the eTenders open data.
-- Source: data/gold/parquet/procurement_awards.parquet (procurement chain,
--   pipeline_sandbox/procurement_etenders_extract.py).
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
    "Competition Type"                           AS competition_type,
    TRY_STRPTIME("Notice Published Date/Contract Created Date", '%d/%m/%Y')::DATE AS award_date,
    value_eur,
    value_kind,
    is_framework_or_dps,
    value_shared_across_suppliers,
    value_safe_to_sum
FROM read_parquet('data/gold/parquet/procurement_awards.parquet');
