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
    -- notice-title from the TED API (eng preferred); NULL when the source omits it —
    -- consumers fall back visibly (e.g. to the publication number), never invent one.
    title,
    buyer_name,
    cpv_code,
    cpv_division,
    procedure_type,
    is_uncompetitive_procedure,
    submission_deadline,
    -- NUTS3 place-of-performance as stated on the notice; NULL when only a country code given
    nuts3_code,
    region,
    -- a tender is "still open" if its deadline has not yet passed (display convenience only)
    (TRY_CAST(submission_deadline AS DATE) >= current_date) AS is_still_open,
    estimated_value_eur,
    currency,
    value_kind,
    value_safe_to_sum,   -- always FALSE here (pre-award estimate) — never sum across grains
    dispatch_date,
    year,
    retrieved_utc
FROM read_parquet('data/silver/parquet/ted_ie_tenders.parquet')
ORDER BY dispatch_date DESC;
