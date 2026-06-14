-- v_procurement_council_summary — one row per PUBLISHING local authority, for the
-- "Your council" index tab (utility/pages_code/procurement.py, _render_councils).
--
-- Reads data/gold/parquet/procurement_payments_fact.parquet DIRECTLY (not via
-- v_procurement_payments) on purpose: the procurement_*.sql glob loads alphabetically with
-- swallow_errors=True, and this file sorts BEFORE procurement_payments.sql, so a dependency
-- on v_procurement_payments would register against a not-yet-existing view and be silently
-- dropped (see memory feedback_sql_view_dependency_order). The WHERE clause is a byte-for-byte
-- copy of v_procurement_payments' filter, so per-council totals match the existing
-- "Local authorities only" toggle on the payments tab exactly.
--
-- ⚠️ NEVER-SUM. ordered_safe_eur (realisation_tier='COMMITTED' / purchase orders, "ordered €X")
-- and paid_safe_eur ('SPENT' / actual payments, "paid €X") are DIFFERENT lifecycle stages of
-- public money — surfaced as two columns and NEVER added. In this corpus only Meath and Offaly
-- publish 'SPENT'; the other 19 LAs publish 'COMMITTED' only, so each card shows ONE pill. Only
-- value_safe_to_sum rows contribute to either total.
--
-- province / province_order are static Irish geography (the 4 historic provinces) used to group
-- the index North->South (1=Ulster .. 4=Munster). South Dublin is the only Dublin LA publishing,
-- so it sits in Leinster (Dublin is in Leinster). This is fixed geography, not data inference.
CREATE OR REPLACE VIEW v_procurement_council_summary AS
SELECT
    publisher_name AS council,
    CASE
        WHEN publisher_name IN ('Donegal', 'Monaghan')                                      THEN 'Ulster'
        WHEN publisher_name IN ('Galway City', 'Galway County', 'Leitrim', 'Mayo', 'Sligo') THEN 'Connacht'
        WHEN publisher_name IN ('Clare', 'Cork City', 'Cork County', 'Limerick',
                                'Waterford')                                                THEN 'Munster'
        ELSE 'Leinster'  -- Kildare, Kilkenny, Longford, Meath, Offaly, South Dublin, Westmeath, Wexford, Wicklow
    END AS province,
    CASE
        WHEN publisher_name IN ('Donegal', 'Monaghan')                                      THEN 1
        WHEN publisher_name IN ('Galway City', 'Galway County', 'Leitrim', 'Mayo', 'Sligo') THEN 2
        WHEN publisher_name IN ('Clare', 'Cork City', 'Cork County', 'Limerick',
                                'Waterford')                                                THEN 4
        ELSE 3  -- Leinster
    END AS province_order,
    COUNT(DISTINCT supplier_normalised)                                                  AS n_suppliers,
    MIN(year)::INT                                                                       AS min_year,
    MAX(year)::INT                                                                       AS max_year,
    COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum AND realisation_tier = 'COMMITTED'), 0)
                                                                                         AS ordered_safe_eur,
    COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum AND realisation_tier = 'SPENT'), 0)
                                                                                         AS paid_safe_eur,
    COUNT(*) FILTER (WHERE realisation_tier = 'COMMITTED')                               AS n_ordered,
    COUNT(*) FILTER (WHERE realisation_tier = 'SPENT')                                   AS n_paid
FROM read_parquet('data/gold/parquet/procurement_payments_fact.parquet')
WHERE publisher_type = 'local_authority'
  AND extraction_status = 'extracted'
  AND supplier_normalised IS NOT NULL
  AND length(supplier_normalised) >= 2
GROUP BY publisher_name
ORDER BY province_order, GREATEST(ordered_safe_eur, paid_safe_eur) DESC;
