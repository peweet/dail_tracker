-- v_nphdb_bam_disclosures — BAM's own PQ-disclosed National Children's Hospital
-- schedule-slippage ledger + 2017/2018 project-cost estimate table (gold parquet
-- materialised by extractors/nphdb_bam_disclosures_extract.py). A hand-verified
-- static disclosure, not a scraped corpus — one row per ledger/cost line item,
-- self-contained (no inter-view deps).
CREATE OR REPLACE VIEW v_nphdb_bam_disclosures AS
SELECT
    disclosure_type,
    sort_order,
    row_label,
    forecast_completion,
    delay_from_original,
    cost_2017_eur_m,
    cost_2018_eur_m,
    source_date,
    source_pq_ref,
    source_url,
    notes
FROM read_parquet('data/gold/parquet/nphdb_bam_disclosures.parquet');
