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
-- SCHEME-TOTAL CONTAMINATION (2026-06-27): the EC register sometimes records a SCHEME's whole
--   budget against ONE named beneficiary. Example: under SA.105798 ("Scheme of Investment Aid for
--   Horticulture", 139 awards, median €38,950) one row reads €2,767,727,677 — the scheme total, not
--   that firm's aid (the same firm also appears correctly at €10,628). This is a SOURCE artifact, not
--   a parse error (aid_element_raw literally carries the figure). We can't correct the number, so we
--   FLAG it: aid_element_suspect_scheme_total is TRUE for a row that, within its scheme (sa_number),
--   is the single largest by >100x over the next award AND is ≥€100m AND the scheme has ≥2 awards.
--   A genuine large single-award scheme (e.g. SA.54472, National Broadband, the only award in its
--   scheme at €2.977bn) is NOT flagged. The page sets flagged rows aside from the ranked register.
CREATE OR REPLACE VIEW v_procurement_eu_tam_state_aid AS
WITH _src AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY sa_number ORDER BY aid_element_value DESC NULLS LAST) AS _aid_rank,
        COUNT(aid_element_value) OVER (PARTITION BY sa_number)                                AS _scheme_n_awards
    FROM read_parquet('data/gold/parquet/eu_tam_state_aid.parquet')
    WHERE beneficiary_name IS NOT NULL
),
_flagged AS (
    SELECT *,
        MAX(CASE WHEN _aid_rank >= 2 THEN aid_element_value END) OVER (PARTITION BY sa_number) AS _scheme_second_val
    FROM _src
)
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
    ingested_date,
    (
        _aid_rank = 1
        AND sa_number IS NOT NULL
        AND _scheme_n_awards >= 2
        AND aid_element_value >= 100000000
        AND _scheme_second_val IS NOT NULL
        AND aid_element_value > 100 * _scheme_second_val
    ) AS aid_element_suspect_scheme_total
FROM _flagged;
