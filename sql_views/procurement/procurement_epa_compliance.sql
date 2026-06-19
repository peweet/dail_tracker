-- v_procurement_epa_compliance — per-company EPA environmental-licence + enforcement record,
--   keyed on the CRO company_num so the company dossier (/company?supplier=…) can annotate a firm
--   with its EPA footprint. One row per CRO-matched company.
--
-- Source: data/gold/parquet/epa_supplier_compliance.parquet
--   produced by extractors/epa_promote_to_gold.py (the PROMOTED output, committed gold), which is the
--   CRO-matched, sole-trader-dropped projection of the sandbox accountability view
--   (pipeline_sandbox/epa_accountability_view.py: EPA WFS licences + EPA LEAP enforcement → CRO firm).
--
-- Display-only annotation, no civic claim beyond the public EPA record:
--   * licence portfolio  — n_licences, the licence classes/statuses held, whether any is active.
--   * enforcement record — counts of EPA records logged against those licences, BY TYPE
--     (incident / complaint / non-compliance) plus how many are still open. These are regulatory
--     activity, NOT findings of wrongdoing.
--   * enforcement_crawled — TRUE only if the firm's licences were in the enforcement crawl scope
--     (waste sector + public-money firms). When FALSE, the event counts are 0 because the record was
--     NOT assessed — the page must say "not assessed", never "clean".
--   No money columns: public-money figures live in the award/payment panels and are never juxtaposed
--   with enforcement counts (would imply causation the data does not support).
CREATE OR REPLACE VIEW v_procurement_epa_compliance AS
SELECT
    company_num,
    n_licences,
    licence_classes,
    licence_statuses,
    any_active_licence,
    is_public_body,
    uww_priority_site,
    enforcement_crawled,
    n_enforcement_events,
    n_incident,
    n_complaint,
    n_non_compliance,
    n_open,
    last_record_date
FROM read_parquet('data/gold/parquet/epa_supplier_compliance.parquet')
WHERE company_num IS NOT NULL;
