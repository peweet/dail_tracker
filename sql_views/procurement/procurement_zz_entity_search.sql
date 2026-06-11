-- v_procurement_entity_search — unified typeahead corpus for the procurement page's
-- search-first hero: suppliers, contracting authorities and CPV categories in ONE list,
-- so a user types a name without needing to know which register answers their question.
--
-- zz_ filename prefix: this view JOINs other procurement views, and registration is
-- alphabetical within the procurement_*.sql glob — zz_ sorts it last so its
-- dependencies exist first (same convention as member_zz_*.sql; see
-- feedback_sql_view_dependency_order).
--
-- ⚠️ THE TWO MONEY HINT COLUMNS ARE DIFFERENT GRAINS: awarded_value_safe_eur is an
-- award-ceiling sum (eTenders), paid_safe_eur is realised SPENT payments — they are
-- carried so a result row can show each with its own label, NEVER added or compared
-- as totals. Suppliers' paid figures join via hard CRO company_num only.
--
-- Grain: one row per entity. entity_kind ∈ ('supplier','authority','cpv'); url_key is
-- the existing deep-link query-param value for that kind (?supplier= / ?authority= /
-- ?cpv=). n_records = the kind's own trustworthy count (awards for all three kinds).
CREATE OR REPLACE VIEW v_procurement_entity_search AS
SELECT
    'supplier'                                   AS entity_kind,
    s.supplier                                   AS display_name,
    s.supplier_norm                              AS url_key,
    s.n_awards                                   AS n_records,
    s.n_authorities                              AS n_counterparties,
    s.awarded_value_safe_eur,
    c.paid_safe_eur,
    s.company_num                                AS cro_company_num,
    s.on_lobbying_register
FROM v_procurement_supplier_summary s
LEFT JOIN v_procurement_entity_chain c ON s.company_num = c.company_num
UNION ALL
SELECT
    'authority'                                  AS entity_kind,
    a.contracting_authority                      AS display_name,
    a.contracting_authority                      AS url_key,
    a.n_awards                                   AS n_records,
    a.n_suppliers                                AS n_counterparties,
    a.awarded_value_safe_eur,
    NULL::DOUBLE                                 AS paid_safe_eur,
    NULL::BIGINT                                 AS cro_company_num,
    FALSE                                        AS on_lobbying_register
FROM v_procurement_authority_summary a
UNION ALL
SELECT
    'cpv'                                        AS entity_kind,
    p.cpv_description                            AS display_name,
    p.cpv_code                                   AS url_key,
    p.n_awards                                   AS n_records,
    p.n_suppliers                                AS n_counterparties,
    p.awarded_value_safe_eur,
    NULL::DOUBLE                                 AS paid_safe_eur,
    NULL::BIGINT                                 AS cro_company_num,
    FALSE                                        AS on_lobbying_register
FROM v_procurement_cpv_summary p
ORDER BY n_records DESC;
