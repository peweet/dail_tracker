-- v_procurement_supplier_sector_breadth — how WIDELY a paid supplier reaches across
-- the public service: distinct publisher SECTORS (health, local_government, justice,
-- ...) rather than raw publisher counts, which a single sector (31 councils) can
-- inflate. Validated 2026-06-11: PFH Technology spans 10 of the corpus's 15 sectors
-- (€149.9m safe), Deloitte LLP 11 sectors (€97.3m), Vodafone 11 (€40.8m).
--
-- ⚠️ Breadth is a REACH fact, not an influence claim. total_safe_eur is the usual
-- indicative floor (only value_safe_to_sum lines; VAT treatment varies by publisher —
-- vat_mixed mirrors v_procurement_payments_supplier_summary). SPENT and COMMITTED
-- lines both count toward breadth (presence), but the safe total is split per tier so
-- the two are never silently added.
--
-- ⚠️ NAME-COLLISION RISK: supplier_normalised can collapse DIFFERENT firms into one
-- bucket (a generic token like "ELECTRIC" aggregates many "X Electric" companies and
-- tops the breadth ranking spuriously). n_raw_variants counts the distinct raw names
-- behind each bucket — a consuming UI should treat a high variant count on a short
-- generic name as a collision and exclude or badge it, not present it as one firm.
--
-- Grain: one row per supplier_normalised (company-class, public-display lines only).
CREATE OR REPLACE VIEW v_procurement_supplier_sector_breadth AS
SELECT
    supplier_normalised,
    COUNT(DISTINCT supplier_raw)                              AS n_raw_variants,
    COUNT(DISTINCT sector)                                    AS n_sectors,
    COUNT(DISTINCT publisher_id)                              AS n_publishers,
    COUNT(*)                                                  AS n_lines,
    SUM(amount_eur) FILTER (WHERE value_safe_to_sum AND realisation_tier = 'SPENT')
                                                              AS paid_safe_eur,
    SUM(amount_eur) FILTER (WHERE value_safe_to_sum AND realisation_tier = 'COMMITTED')
                                                              AS committed_safe_eur,
    COUNT(DISTINCT vat_status) > 1                            AS vat_mixed,
    list_sort(list(DISTINCT sector))                          AS sectors,
    MIN(year)                                                 AS first_year,
    MAX(year)                                                 AS last_year,
    ANY_VALUE(cro_company_num)                                AS cro_company_num
FROM read_parquet('data/gold/parquet/procurement_payments_fact.parquet')
WHERE public_display
  AND supplier_class = 'company'
  AND supplier_normalised IS NOT NULL
  AND length(trim(supplier_normalised)) >= 6
GROUP BY supplier_normalised
ORDER BY n_sectors DESC, paid_safe_eur DESC NULLS LAST;
