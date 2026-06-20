-- v_corporate_cbi_enforcement — Central Bank of Ireland enforcement actions (settlement
--   public statements), one row per action. The public regulatory record of a FIRM.
--
-- Source: data/gold/parquet/cbi_enforcement_actions.parquet
--   produced by extractors/enrichment_promote_to_gold.py, the privacy-filtered promotion of the
--   vetted sandbox scrape pipeline_sandbox/cbi_enforcement_extract.py (the centralbank.ie
--   enforcement-actions hub + per-action statement PDFs).
--
-- PRIVACY: suspected natural persons (ex-officers sanctioned in a professional capacity) were
--   DROPPED at promotion — this view carries firms only. Prohibition notices / fitness-and-probity
--   adverse assessments are never ingested. (feedback_personal_insolvency_privacy)
--
-- VALUE SEMANTICS: fine_amount_eur is a real monetary penalty but parse confidence varies
--   (older statements are scans with no text layer → has_text_layer FALSE → fine is NULL, NOT zero;
--   n_euro_mentions flags statements with several figures for manual QC). value_safe_to_sum is FALSE
--   on every row — NEVER SUM fine_amount_eur and never union it with payment/award facts.
CREATE OR REPLACE VIEW v_corporate_cbi_enforcement AS
SELECT
    notice_date,
    title,
    party_name,
    doc_type,
    fine_amount_eur,
    has_text_layer,
    n_euro_mentions,
    pdf_url,
    value_kind,
    value_safe_to_sum,
    source_url,
    ingested_date
FROM read_parquet('data/gold/parquet/cbi_enforcement_actions.parquet');
