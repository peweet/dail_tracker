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
-- unique_member_code / party_name / constituency populated by
-- payments_member_enrichment.py after the PSA ETL runs.
-- Coverage: 172 of 176 current TDs (97.7%). The four unmatched (Daniel Ennis,
-- Frankie Feighan, Paul Nicholas Gogarty, Conor D McGuinness) are upstream
-- name-shape mismatches between the Oireachtas members API and the PSA
-- payment publications; they retain NULL and fall back to the "Not on file"
-- empty-state in the UI.

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
    unique_member_code,
    party_name,
    constituency
FROM read_parquet('data/gold/parquet/payments_full_psa.parquet');
