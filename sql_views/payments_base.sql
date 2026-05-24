-- v_payments_base — reads the full PSA gold Parquet produced by payments_full_psa_etl.py
--
-- Grain: one row per payment transaction (member_name, date_paid, narrative).
-- Covers all payment kinds: TAA, PSA_DUBLIN, PRA, PRA_MIN, PRA_FLAG_ONLY — both the
-- TAA-banded travel allowance and the previously-quarantined PRA-side rows.
--
-- Column aliases preserve the names expected by downstream views:
--   amount      → amount_num   (matches payments_summary, payments_member_detail, etc.)
--   date_paid   → synthesises payment_year
--
-- TODO_PIPELINE_VIEW_REQUIRED: payments_member_enrichment.py is not yet built, so the
-- parquet currently lacks unique_member_code / party_name / constituency. They are
-- projected as NULL here so downstream views (member_detail, yearly_evolution) compile.
-- Once enrichment lands, drop these NULL casts and let the parquet supply the values.

CREATE OR REPLACE VIEW v_payments_base AS
SELECT
    member_name,
    position,
    payment_kind,
    taa_band_raw,
    taa_band_label,
    date_paid,
    narrative,
    amount                                AS amount_num,
    EXTRACT(YEAR FROM date_paid)::INTEGER AS payment_year,
    source_pdf,
    schema,
    NULL::VARCHAR AS unique_member_code,
    NULL::VARCHAR AS party_name,
    NULL::VARCHAR AS constituency
FROM read_parquet('data/gold/parquet/payments_full_psa.parquet');
