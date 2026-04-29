-- v_payments_member_detail — payment transaction fact table (one row per transaction)
-- Depends on: v_payments_base
--
-- Grain: (member_name, date_paid, narrative)
-- This is the audit trail view: every individual payment that makes up a member's total.
-- Do not aggregate here — aggregation lives in v_payments_yearly_evolution.
--
-- party_name and constituency enriched by pipeline_sandbox/payments_member_enrichment.py
-- TODO_PIPELINE_VIEW_REQUIRED: canonical member_id for cross-page linking
-- TODO_PIPELINE_VIEW_REQUIRED: per-year official source PDF URL

CREATE OR REPLACE VIEW v_payments_member_detail AS
SELECT
    COALESCE(unique_member_code, '') AS unique_member_code,
    member_name,
    position,
    COALESCE(party_name,   '') AS party_name,
    COALESCE(constituency, '') AS constituency,
    taa_band_raw,
    taa_band_label,
    date_paid,
    narrative,
    amount_num,
    payment_year
FROM v_payments_base
ORDER BY member_name, date_paid ASC, narrative ASC;
