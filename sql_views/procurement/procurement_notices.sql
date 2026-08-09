-- v_procurement_notices — one row per eTenders notice, AWARDED OR NOT. Source:
-- data/gold/parquet/procurement_notices.parquet (procurement chain,
-- extractors/procurement_etenders_extract.py, 2026-08-09).
--
-- The award fact (v_procurement_awards) sees only notices with a recorded supplier —
-- which is roughly HALF of all notices (2026-08 export: 84,480 rows, 43,255 with a
-- supplier; only 4 tender IDs ever appear both ways, so the rest are genuinely
-- unawarded, not notice-stage duplicates). This view is the honest denominator:
-- award_status ∈ awarded | cancelled | award_published_no_supplier | no_award_published.
--
-- NO supplier names here (privacy lives in the award fact). estimated_value_eur is a
-- PRE-AWARD ESTIMATE — its own money class, never summed with awards/payments/budget/TED;
-- the awarded amount stays exclusively in v_procurement_awards with its safety flags.
CREATE OR REPLACE VIEW v_procurement_notices AS
SELECT
    tender_id,
    contracting_authority,
    client_authority,
    COALESCE(client_authority, contracting_authority) AS buyer_authority,
    agreement_owner,
    tender_title,
    competition_type,
    procedure_type,
    contract_type,
    spend_category,
    cpv_code,
    cpv_description,
    COALESCE(cpv_description, spend_category) AS category_label,
    threshold_level,
    directive,
    evaluation_type,
    platform,
    parent_agreement_id,
    published_date,
    EXTRACT(year FROM published_date)::INT AS year,
    submission_deadline,
    cancelled_date,
    award_published_date,
    award_status,
    has_awarded_supplier,
    awarded_value_present,
    estimated_value_eur,
    n_bids_received,
    -- Same deep-link construction as v_procurement_awards: the OGP Tender ID templates
    -- into the public eTenders notice URL; NULL where the source dropped the id.
    CASE WHEN tender_id IS NOT NULL AND TRIM(tender_id) NOT IN ('', 'NULL')
         THEN 'https://www.etenders.gov.ie/epps/cft/prepareViewCfTWS.do?resourceId=' || TRIM(tender_id)
    END AS etenders_notice_url
FROM read_parquet('data/gold/parquet/procurement_notices.parquet');
