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

CREATE OR REPLACE VIEW v_payments_base AS
SELECT * FROM read_parquet('data/gold/parquet/payments_fact.parquet');
