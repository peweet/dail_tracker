-- v_payments_member_detail — payment transaction fact table (one row per transaction)
-- Depends on: v_payments_base
--
-- Grain: (member_name, date_paid, narrative)
-- This is the audit trail view: every individual payment that makes up a member's total.
-- Do not aggregate here — aggregation lives in v_payments_yearly_evolution.
--
-- TODO_PIPELINE_VIEW_REQUIRED: canonical member_id for cross-page linking
-- TODO_PIPELINE_VIEW_REQUIRED: party_name and constituency (not in payments source CSV)
-- TODO_PIPELINE_VIEW_REQUIRED: per-year official source PDF URL

CREATE OR REPLACE VIEW v_payments_member_detail AS
SELECT
    member_name,
    position,
    taa_band_raw,
    taa_band_label,
    date_paid,
    narrative,
    amount_num,
    payment_year
FROM v_payments_base
ORDER BY member_name, date_paid ASC, narrative ASC;
