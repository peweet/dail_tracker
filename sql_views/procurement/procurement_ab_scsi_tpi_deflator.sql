-- v_scsi_tpi_deflator — SCSI Tender Price Index deflator exposed as a view, so construction
-- awards can be adjusted by CONSTRUCTION tender-price inflation (the right index for "what would
-- this work cost to procure today") instead of general CPI. Built by
-- extractors/cso_pxstat_extract.py:build_scsi_tpi_deflator from data/_meta/scsi_tender_price_index.csv.
-- deflator_to_base = tpi_index[base]/tpi_index[year] (1.0 at base year). Named procurement_ab_*
-- so the alphabetical loader registers it after v_cpi_deflator (aa_) and before
-- v_procurement_awards_real (which LEFT JOINs both).
--
-- ⚠️ Tender prices INCLUDE contractor margins and reflect construction-market conditions — they
-- moved far faster than CPI (e.g. ~2× since 2014). This is the QS lens, not a general price index.
CREATE OR REPLACE VIEW v_scsi_tpi_deflator AS
SELECT
    year,
    tpi_index,
    deflator_to_base,
    base_year,
    'SCSI_TPI_CONSTRUCTION' AS index_code
FROM read_parquet('data/gold/parquet/scsi_tpi_deflator.parquet');
