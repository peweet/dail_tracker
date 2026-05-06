-- ============================================================================
--  EXPERIMENTAL — DELETE ON INTEGRATION
-- ============================================================================
--  Sandbox preview of the full PSA (TAA + PRA) re-parse. Reads from
--  data/gold/parquet/payments_full_psa.parquet — produced by
--  pipeline_sandbox/payments_full_psa_etl.py and SEPARATE from the production
--  payments_fact.parquet (which is TAA-only and used by v_payments_base).
--
--  The new file has columns:
--    member_name, position, payment_kind, taa_band_raw, taa_band_label,
--    date_paid, narrative, amount, source_pdf, schema
--
--  Differences vs production payments_fact.parquet:
--    - column `amount`        (was `amount_num` in production)
--    - new column `payment_kind` ∈ {TAA, PSA_DUBLIN, PRA, PRA_MIN, PRA_FLAG_ONLY}
--    - new columns `source_pdf`, `schema`
--    - no `payment_year`     — synthesised here from date_paid
--
--  REMOVAL CHECKLIST when graduating to production:
--    1. Replace the parser stage in payments.py (top-level, not pages_code)
--       with the schema-aware logic in pipeline_sandbox/payments_full_psa_etl.py.
--    2. Update payments_fact.parquet to carry `amount_num` (rename `amount`)
--       and add `payment_year`, `payment_kind`, `source_pdf`, `schema`.
--    3. Update sql_views/payments_base.sql to either pass through the new
--       columns or filter on payment_kind = 'TAA' if you want to preserve
--       the current TAA-only totals.
--    4. Delete this file.
--    5. Remove the experimental preview page from utility/app.py and
--       delete utility/pages_code/experimental_preview.py +
--       utility/data_access/experimental_data.py.
--    6. (Optional) Run pipeline_sandbox/payments_2019_backfill_probe.py
--       --download to fetch the 12 missing 2019 PDFs into bronze, then
--       re-run the parser to capture the full pre-2020 corpus.
--
--  This file matches `experimental_*.sql` and is NOT loaded by the production
--  payments_data.get_payments_conn() loader (which globs `payments_*.sql`).
-- ============================================================================

CREATE OR REPLACE VIEW v_experimental_payments_full_psa AS
SELECT
    member_name,
    position,
    payment_kind,
    taa_band_raw,
    taa_band_label,
    date_paid,
    narrative,
    amount,
    source_pdf,
    schema,
    EXTRACT(YEAR FROM date_paid)::INTEGER AS payment_year
FROM read_parquet('data/gold/parquet/payments_full_psa.parquet');
