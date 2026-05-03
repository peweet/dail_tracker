-- v_payments_base — reads the clean gold Parquet produced by payments_gold_etl.py
--
-- Grain: one row per payment transaction (member_name, date_paid, narrative).
-- Two payments CAN share the same date_paid but cover different periods.
--
-- All name normalisation, amount parsing, TAA band mapping, and quarantine logic
-- live in pipeline_sandbox/payments_gold_etl.py (Polars). SQL does aggregation only.
--
-- TODO_PIPELINE_VIEW_REQUIRED: integrate pipeline_sandbox/payments_gold_etl.py into
-- the main pipeline so payments_fact.parquet is regenerated automatically on each run.
--
-- TODO_PIPELINE_VIEW_REQUIRED: payments_member_enrichment.py is not yet built, so the
-- parquet currently lacks unique_member_code / party_name / constituency. They are
-- projected as NULL here so downstream views (member_detail, yearly_evolution) compile.
-- Once enrichment lands, drop these NULL casts and let the parquet supply the values.

CREATE OR REPLACE VIEW v_payments_base AS
SELECT
    *,
    NULL::VARCHAR AS unique_member_code,
    NULL::VARCHAR AS party_name,
    NULL::VARCHAR AS constituency
FROM read_parquet('data/gold/parquet/payments_fact.parquet');
