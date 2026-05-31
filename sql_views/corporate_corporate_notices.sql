-- v_corporate_notices — corporate distress / register notices from Iris Oifigiúil.
-- Source: data/gold/parquet/corporate_notices.parquet (produced by
-- pipeline_sandbox/corporate_notices_enrichment.py). Personal insolvency
-- (individual bankruptcies) is excluded by policy at the enrichment level —
-- see [[feedback_personal_insolvency_privacy]].
--
-- Grain: one row per corporate notice across:
--   corporate_insolvency / corporate_notice / corporate_rescue /
--   investment_vehicle_register_notice.
--
-- brand_mentions + parent_fund_mentions are list columns tagged from the
-- curated data/_meta/loan_book_fund_aliases.csv (~25 starter entries).
-- The Corporate page does its display-only aggregations on the loaded frame.
CREATE OR REPLACE VIEW v_corporate_notices AS
SELECT
    notice_ref,
    issue_date,
    issue_number,
    notice_category,
    notice_subtype,
    entity_name,
    display_title,
    title,
    raw_text,
    brand_mentions,
    parent_fund_mentions,
    fund_type_mentions,
    iris_source_pdf
FROM read_parquet('data/gold/parquet/corporate_notices.parquet')
ORDER BY issue_date DESC NULLS LAST;
