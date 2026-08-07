-- Grain: one deterministic pre-tender lead x work-package classification.
-- Money boundary: evidence phrases are descriptive and never aggregable.
-- Status boundary: package classification does not verify a live tender.
CREATE OR REPLACE VIEW v_procurement_pre_tender_work_packages AS
SELECT
    package_row_id,
    lead_id,
    source_corpus,
    source_record_id,
    CAST(source_date AS DATE) AS source_date,
    reporting_area,
    project_name,
    package_code,
    package_label,
    package_group,
    evidence_phrase,
    matched_field,
    classification_basis,
    source_url,
    source_review_required,
    current_status_verified,
    amount_is_not_aggregable,
    classification_schema
FROM read_parquet('data/gold/parquet/pre_tender_work_packages.parquet');
