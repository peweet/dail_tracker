-- v_procurement_entity_search — unified typeahead corpus for the procurement page's
-- search-first hero: suppliers, contracting authorities and CPV categories in ONE list,
-- so a user types a name without needing to know which register answers their question.
--
-- zz_ filename prefix: this view JOINs other procurement views, and registration is
-- alphabetical within the procurement_*.sql glob — zz_ sorts it last so its
-- dependencies exist first (same convention as member_zz_*.sql; see
-- feedback_sql_view_dependency_order).
--
-- TWO REGISTERS, NEVER CONFLATED. The corpus spans the AWARDS register (eTenders/TED:
-- entity_kind 'supplier'/'authority'/'cpv') AND the realised-PAYMENTS register (public
-- bodies' own >€20k lists: entity_kind 'paid_supplier'/'paid_body'). A firm paid by the
-- State but never an eTenders award-winner (e.g. a council/OPW contractor) lived ONLY in
-- the awards-side corpus before and so was unsearchable; the paid_* branches fix that.
-- The two registers use DIFFERENT published name forms and are linked by NO key here, so a
-- firm can legitimately appear under both an awards card and a paid card — that is correct,
-- not a duplicate: they are different lifecycle stages of public money.
--
-- ⚠️ THE TWO MONEY HINT COLUMNS ARE DIFFERENT GRAINS: awarded_value_safe_eur is an
-- award-ceiling sum (eTenders), paid_safe_eur is realised SPENT/COMMITTED payments — they are
-- carried so a result row can show each with its own label, NEVER added or compared
-- as totals. On 'supplier' rows the paid figure joins via hard CRO company_num only; on
-- 'paid_supplier'/'paid_body' rows it is the row's own payments total.
--
-- PRIVACY: the paid_supplier branch is restricted to supplier_class='company' — the same gate
-- that makes a paid supplier's card CLICKABLE in the payments tab. Sole traders / individuals /
-- bare id-codes are NEVER added to the search corpus (no person typeahead / profile-building),
-- mirroring the quarantine the rest of the payments drill-down enforces.
--
-- Grain: one row per entity (paid_* are one row per entity × lifecycle tier). url_key is the
-- existing deep-link query-param value for that kind (?supplier= / ?authority= / ?cpv= /
-- ?paid_supplier= / ?paid_publisher=); paid_tier carries SPENT/COMMITTED for the paid_* kinds
-- so the page can build the tier-correct paid-dossier link (NULL for the award kinds).
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
    s.on_lobbying_register,
    NULL::VARCHAR                                AS paid_tier
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
    FALSE                                        AS on_lobbying_register,
    NULL::VARCHAR                                AS paid_tier
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
    FALSE                                        AS on_lobbying_register,
    NULL::VARCHAR                                AS paid_tier
FROM v_procurement_cpv_summary p
UNION ALL
-- Realised-PAYMENTS suppliers (the contractors the State actually PAID / ORDERED FROM). One
-- row per (company × tier). Company-class only (privacy gate above). n_records = the firm's
-- published payment lines; the money hint is the realised paid/ordered total, never an award.
SELECT
    'paid_supplier'                              AS entity_kind,
    ps.supplier                                  AS display_name,
    ps.supplier_normalised                       AS url_key,
    ps.n_payments                                AS n_records,
    ps.n_publishers                              AS n_counterparties,
    NULL::DOUBLE                                 AS awarded_value_safe_eur,
    ps.total_safe_eur                            AS paid_safe_eur,
    NULL::BIGINT                                 AS cro_company_num,
    FALSE                                        AS on_lobbying_register,
    ps.realisation_tier                          AS paid_tier
FROM v_procurement_payments_supplier_summary ps
WHERE ps.supplier_class = 'company' AND ps.total_safe_eur > 0
UNION ALL
-- Realised-PAYMENTS public bodies (the councils / state bodies that publish >€20k lists). One
-- row per (body × tier) so a council reachable only through the payments register — not the
-- eTenders contracting-authority list — is searchable by name.
SELECT
    'paid_body'                                  AS entity_kind,
    pb.publisher_name                            AS display_name,
    pb.publisher_name                            AS url_key,
    pb.n_payments                                AS n_records,
    pb.n_suppliers                               AS n_counterparties,
    NULL::DOUBLE                                 AS awarded_value_safe_eur,
    pb.total_safe_eur                            AS paid_safe_eur,
    NULL::BIGINT                                 AS cro_company_num,
    FALSE                                        AS on_lobbying_register,
    pb.realisation_tier                          AS paid_tier
FROM v_procurement_payments_publisher_summary pb
WHERE pb.total_safe_eur > 0
ORDER BY n_records DESC;
