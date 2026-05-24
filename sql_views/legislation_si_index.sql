-- v_statutory_instruments — the Statutory Instrument as a first-class entity,
-- one row per SI. Source: data/gold/parquet/statutory_instruments.parquet
-- (pipeline_sandbox/si_entity_enrichment.py).
--
-- Sourced from the Iris Oifigiúil SI taxonomy directly — NOT gated on a bill
-- match. The enabling-bill link (bill_id / bill_short_title) is one optional
-- attribute, present on ~30% of rows; the browsable SI page does not depend
-- on it. Distinct from v_bill_statutory_instruments, which is the bill-gated
-- view that backs the "SIs under this Act" section on bill detail.
--
-- The file name starts with 'legislation_' so legislation_data.py's
-- get_legislation_conn() glob picks it up.

CREATE OR REPLACE VIEW v_statutory_instruments AS
SELECT
    si_id,
    si_year,
    si_number,
    si_title,
    CAST(si_signed_date AS DATE)            AS si_signed_date,
    si_operation,
    si_operation_flags,
    si_form,
    si_eu_relationship,
    si_is_eu,
    si_policy_domain,
    si_policy_domains_all,
    si_responsible_actor,
    si_department,
    si_department_label,
    si_minister_member_code,
    si_minister_name,
    si_parent_legislation,
    bill_id,
    bill_short_title,
    eisb_url,
    iris_source_pdf,
    si_taxonomy_confidence
FROM read_parquet('data/gold/parquet/statutory_instruments.parquet')
ORDER BY si_signed_date DESC NULLS LAST, si_year DESC, si_number DESC;
