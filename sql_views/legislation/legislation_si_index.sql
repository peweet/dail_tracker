-- v_statutory_instruments — the Statutory Instrument as a first-class entity,
-- one row per SI. Source: data/gold/parquet/statutory_instruments.parquet
-- (si_entity_enrichment.py).
--
-- Sourced from the Iris Oifigiúil SI taxonomy directly — NOT gated on a bill
-- match. The enabling-bill link (bill_id / bill_short_title) is one optional
-- attribute, present on ~30% of rows; the browsable SI page does not depend
-- on it. Distinct from v_bill_statutory_instruments, which is the bill-gated
-- view that backs the "SIs under this Act" section on bill detail.
--
-- The file name starts with 'legislation_' so legislation_data.py's
-- get_legislation_conn() glob picks it up.
--
-- Legal-state (revoked / amended) is LEFT-JOINed from v_si_current_state
-- (legislation_si_current_state.sql, eISB Legislation Directory). LEFT, not
-- inner: an SI absent from the directory crawl keeps every base column and
-- gets current_state = NULL, which the page renders as "status not checked" —
-- never as "in force". The join is on si_id (identical {year}-{number:03d}
-- format on both sides); it is one-row-per-SI on both sides, so it cannot
-- inflate the row count. The page stays display-only (SELECT *).

CREATE OR REPLACE VIEW v_statutory_instruments AS
SELECT
    si.si_id,
    si.si_year,
    si.si_number,
    si.si_title,
    CAST(si.si_signed_date AS DATE)         AS si_signed_date,
    si.si_operation,
    si.si_operation_flags,
    si.si_form,
    si.si_eu_relationship,
    si.si_is_eu,
    si.si_policy_domain,
    si.si_policy_domains_all,
    si.si_responsible_actor,
    si.si_signatory_name,
    si.si_department,
    si.si_department_label,
    si.si_minister_member_code,
    si.si_minister_name,
    si.si_parent_legislation,
    si.bill_id,
    si.bill_short_title,
    si.eisb_url,
    si.iris_source_pdf,
    si.si_taxonomy_confidence,
    -- legal-state columns (NULL when the directory crawl did not cover this SI)
    cs.current_state,
    cs.affecting_sis,
    cs.this_si_eli_url,
    cs.how_affected_raw,
    cs.state_source,
    cs.state_source_url,
    cs.directory_updated_to,
    cs.confidence                           AS state_confidence
FROM read_parquet('data/gold/parquet/statutory_instruments.parquet') si
LEFT JOIN v_si_current_state cs ON cs.si_id = si.si_id
ORDER BY si_signed_date DESC NULLS LAST, si.si_year DESC, si.si_number DESC;
