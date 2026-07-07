-- v_cpi_deflator — the CSO CPA07 CPI deflator gold table exposed as a view, so SQL can
-- adjust any euro for general consumer-price inflation through ONE shared index. Mirrors
-- services/deflator.py:Deflator (a parity test pins SQL deflation == that function).
--
-- Source: data/gold/parquet/cso_cpi_deflator.parquet (extractors/cso_pxstat_extract.py:
--   build_cpi_deflator — chain-linked CPA07, base year 2025). deflator_to_base =
--   index[base_year]/index[year], so value_eur * deflator_to_base = value in base-year euro;
--   it is 1.0 at the base year. A year ABSENT from this table has no row, so a LEFT JOIN to
--   it yields NULL (never a silent x1.0) for that award/payment.
--
-- ⚠️ This is GENERAL consumer-price inflation. It is NOT construction inflation, building-
-- materials inflation, labour-rate inflation or tender-price inflation — those move at very
-- different rates (the SCSI Tender Price Index in dail_tracker_core/qs_valuation.py is the
-- construction lens). See doc/PROCUREMENT_INFLATION_BENCHMARKING_DESIGN.md.
--
-- Filename is procurement_aa_* on purpose: the view loader registers a domain's files in
-- ALPHABETICAL order (dail_tracker_core/db.py), and v_procurement_awards_real LEFT JOINs this
-- view, so this must register first.
CREATE OR REPLACE VIEW v_cpi_deflator AS
SELECT
    year,
    cpi_index_chained,
    deflator_to_base,
    base_year,
    'CSO_CPA07_CPI' AS index_code
FROM read_parquet('data/gold/parquet/cso_cpi_deflator.parquet');
