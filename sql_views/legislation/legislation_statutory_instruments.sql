-- v_bill_statutory_instruments - SIs joined to their enabling bill.
-- Source: data/gold/parquet/bill_statutory_instruments.parquet (produced by
-- iris_si_bill_enrichment.py - lifts the matcher from the statutory_instruments
-- page into the pipeline).
--
-- Grain: one row per matched (bill, SI). SIs without a bill match are
-- written to data/silver/_meta/ for the coverage gate and not exposed here.
--
-- The view file name starts with 'legislation_' so it is picked up by
-- legislation_data.py's get_legislation_conn() glob.

CREATE OR REPLACE VIEW v_bill_statutory_instruments AS
SELECT
    bill_id,
    bill_short_title,
    sponsor_unique_member_code,
    si_year,
    si_number,
    si_id,
    si_title,
    si_signed_date,
    si_minister,              -- role string, e.g. 'The Minister for Finance'
    si_minister_named,        -- person name extracted from raw_text, nullable
    si_policy_domain,
    si_policy_domains_all,
    si_operation,
    si_operation_flags,
    si_form,
    si_eu_relationship,
    si_is_eu,
    eisb_url,
    iris_source_pdf,
    match_score
FROM read_parquet('data/gold/parquet/bill_statutory_instruments.parquet')
-- Pipeline (iris_si_bill_enrichment.py) already drops rows with NULL
-- matched_bill_id before writing the parquet, so no defensive WHERE filter
-- is needed here. Keep gold guarantees enforced upstream.
ORDER BY si_signed_date DESC NULLS LAST, si_number;
