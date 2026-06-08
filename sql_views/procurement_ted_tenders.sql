-- v_procurement_ted_tenders — TED (EU Official Journal) Irish COMPETITION / TENDER notices
-- (cn-standard): the PRE-AWARD pipeline — what Irish public bodies are putting out to tender,
-- under which procedure, by when. Reads the silver parquet directly (same pattern as the
-- award + lobbying-overlap views).
--
-- ⚠️ A THIRD GRAIN, never summed with awards (eTenders / TED CAN) or payments. estimated_value_eur
-- is a BUYER ESTIMATE recorded before any award — not money awarded and not money paid, so
-- value_safe_to_sum is always FALSE. A tender notice is a procurement OPPORTUNITY, not a contract.
CREATE OR REPLACE VIEW v_procurement_ted_tenders AS
SELECT
    publication_number,
    notice_url,
    buyer_name,
    cpv_code,
    cpv_division,
    procedure_type,
    is_uncompetitive_procedure,
    submission_deadline,
    -- a tender is "still open" if its deadline has not yet passed (display convenience only)
    (TRY_CAST(submission_deadline AS DATE) >= current_date) AS is_still_open,
    estimated_value_eur,
    currency,
    value_kind,
    value_safe_to_sum,   -- always FALSE here (pre-award estimate) — never sum across grains
    dispatch_date,
    year
FROM read_parquet('data/silver/parquet/ted_ie_tenders.parquet')
ORDER BY dispatch_date DESC;
