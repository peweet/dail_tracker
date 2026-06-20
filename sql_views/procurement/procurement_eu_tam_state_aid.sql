-- v_procurement_eu_tam_state_aid — EU State-Aid Transparency (TAM) awards granted by Irish
--   authorities, one row per published award (>€100k; lower for agri/fisheries). Named
--   beneficiary grant aid (IDA / Enterprise Ireland / DAFM-style), the structured "grant register".
--
-- Source: data/gold/parquet/eu_tam_state_aid.parquet
--   produced by extractors/enrichment_promote_to_gold.py, the privacy-filtered promotion of the
--   vetted sandbox crawl pipeline_sandbox/eu_tam_ireland_extract.py (EC transparency register).
--
-- PRIVACY: suspected natural persons (agri sole-traders) were DROPPED at promotion and the raw
--   national_id column was excluded — this view carries companies / public bodies only. The parsed
--   cro_company_num (six-digit CRO number, ~62% of rows) is the clean join key to the supplier
--   backbone. (feedback_personal_insolvency_privacy)
--
-- VALUE SEMANTICS: nominal_amount_value is the AWARDED face value, aid_element_value the subsidy
--   equivalent for guarantees/loans; small-band disclosures publish a RANGE kept as text in
--   *_raw (then *_value is NULL). value_safe_to_sum is FALSE on every row — NEVER SUM these and
--   never union with payment facts (these are AWARDS, not drawdowns).
CREATE OR REPLACE VIEW v_procurement_eu_tam_state_aid AS
SELECT
    ref_no,
    sa_number,
    aid_measure_title,
    beneficiary_name,
    beneficiary_type,
    cro_company_num,
    region,
    sector_nace,
    aid_instrument,
    objective,
    nominal_amount_raw,
    nominal_amount_value,
    nominal_amount_currency,
    aid_element_raw,
    aid_element_value,
    aid_element_currency,
    date_granted,
    granting_authority,
    entrusted_entity,
    financial_intermediary,
    published_date,
    award_detail_url,
    value_kind,
    realisation_tier,
    value_safe_to_sum,
    ingested_date
FROM read_parquet('data/gold/parquet/eu_tam_state_aid.parquet')
WHERE beneficiary_name IS NOT NULL;
