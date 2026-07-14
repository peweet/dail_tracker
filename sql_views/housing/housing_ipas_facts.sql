-- v_ipas_facts — the CITATION BACKING STORE for the international-protection corpus.
--
-- Every figure the app publishes about asylum accommodation must trace to a row here: the
-- document it came from, the page, the paragraph/figure reference, and the source URL. This
-- is what makes the provenance footer real rather than decorative, and what lets a later
-- extraction pass find what an earlier one missed.
--
-- 5,330 facts from 10 extractors across 16 source documents (C&AG 2024 + 2015, HIQA's overview
-- and its 101 individual inspection reports, IGEES, the Accommodation Strategy, the National
-- Standards, the Project Initiation Document, the IPAS weekly statistics).
--
-- ⚠️ THIS IS AN ARCHIVE, NOT A SERVING TABLE. It has no county, operator or centre key, its
-- `unit` is long-tailed and its `period` is free text. Do NOT aggregate it for a page — read
-- the purpose-built contracts (v_ipas_la_profile, v_ipas_operators, v_ipas_centre_compliance,
-- v_ipas_property_rates, v_ipas_entitlements) and use this only to cite them.
--
-- UNKNOWNS ARE FIRST-CLASS: is_unknown=TRUE rows carry a NULL value and an unknown_reason.
-- They are kept deliberately — where the State does not publish a number, that IS the finding.
-- Never filter them out silently; never impute them.
--
-- GRAIN: one row per extracted fact. value_safe_to_sum=FALSE, always. These are figures QUOTED
-- by auditors and regulators — they must never be summed, nor unioned with
-- procurement_payments_fact, procurement_awards, or any grant fact.
CREATE OR REPLACE VIEW v_ipas_facts AS
SELECT
    fact_id,
    doc_key,
    doc_title,
    category,
    subject,
    metric,
    value_numeric,
    value_text,
    unit,
    qualifier,
    period,
    scope,
    is_unknown,
    unknown_reason,
    page,
    ref,
    source_url,
    confidence,
    value_safe_to_sum
FROM read_parquet('data/gold/parquet/ipas_facts.parquet');
