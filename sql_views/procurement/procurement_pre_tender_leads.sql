-- Grain: one dated source observation about one named project or procurement package.
-- This is a pre-notice lead lane, not a tender, award, payment, or budget fact.
-- amount_text is source wording only and must never be parsed or summed.
CREATE OR REPLACE VIEW v_procurement_pre_tender_leads AS
SELECT
    lead_id,
    source_corpus,
    source_record_id,
    TRY_CAST(source_date AS DATE) AS source_date,
    date_precision,
    buyer_or_sponsor,
    reporting_area,
    area_basis,
    project_name,
    sector,
    likely_work_package,
    published_stage,
    normalized_stage,
    stage_display_order,
    amount_text,
    amount_is_not_aggregable,
    source_url,
    source_review_required,
    current_status_verified,
    tender_notice_status,
    report_as_of,
    classification_schema,
    school_roll_number,
    snapshot_freshness
FROM read_parquet('data/gold/parquet/pre_tender_leads.parquet');
