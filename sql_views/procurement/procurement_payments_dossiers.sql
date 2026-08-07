-- v_procurement_payments_* dossier views -- reusable payment aggregations.
--
-- All values are constrained by v_procurement_payments: SPENT and COMMITTED
-- stay separate, and amount_*_safe_eur is NULL outside its own tier.  Query
-- functions filter these views; they do not create alternate rollups.

CREATE OR REPLACE VIEW v_procurement_payments_corpus_stats AS
SELECT
    COUNT(*) AS n_payments,
    COUNT(DISTINCT publisher_name) AS n_publishers,
    COUNT(DISTINCT supplier_normalised) AS n_suppliers,
    MIN(year)::INT AS min_year,
    MAX(year)::INT AS max_year,
    COALESCE(SUM(amount_spent_safe_eur), 0) AS spent_safe_eur,
    COALESCE(SUM(amount_committed_safe_eur), 0) AS committed_safe_eur
FROM v_procurement_payments;

CREATE OR REPLACE VIEW v_procurement_payments_supplier_by_year AS
SELECT
    supplier_normalised,
    year,
    COALESCE(SUM(amount_spent_safe_eur), 0) AS paid_safe_eur,
    COALESCE(SUM(amount_committed_safe_eur), 0) AS ordered_safe_eur,
    COUNT(*) FILTER (WHERE realisation_tier = 'SPENT') AS n_paid_lines,
    COUNT(*) FILTER (WHERE realisation_tier = 'COMMITTED') AS n_ordered_lines
FROM v_procurement_payments
WHERE year IS NOT NULL
GROUP BY supplier_normalised, year;

CREATE OR REPLACE VIEW v_procurement_payments_publisher_suppliers AS
SELECT
    publisher_name,
    realisation_tier,
    mode(supplier) AS supplier,
    supplier_normalised,
    mode(supplier_class) AS supplier_class,
    COUNT(*) AS n_payments,
    MIN(year)::INT AS min_year,
    MAX(year)::INT AS max_year,
    COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum), 0) AS total_safe_eur,
    mode(cro_company_num) AS cro_company_num
FROM v_procurement_payments
GROUP BY publisher_name, realisation_tier, supplier_normalised;

CREATE OR REPLACE VIEW v_procurement_payments_publisher_by_year AS
SELECT
    publisher_name,
    realisation_tier,
    year,
    COUNT(*) AS n_payments,
    COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum), 0) AS total_safe_eur
FROM v_procurement_payments
WHERE year IS NOT NULL
GROUP BY publisher_name, realisation_tier, year;

CREATE OR REPLACE VIEW v_procurement_payments_publisher_profile AS
SELECT
    publisher_name,
    mode(publisher_type) AS publisher_type,
    mode(sector) AS sector,
    COUNT(DISTINCT supplier_normalised) AS n_suppliers,
    MIN(year)::INT AS min_year,
    MAX(year)::INT AS max_year,
    COUNT(*) FILTER (WHERE realisation_tier = 'SPENT') AS n_paid_lines,
    COUNT(*) FILTER (WHERE realisation_tier = 'COMMITTED') AS n_ordered_lines,
    COALESCE(SUM(amount_spent_safe_eur), 0) AS paid_safe_eur,
    COALESCE(SUM(amount_committed_safe_eur), 0) AS ordered_safe_eur
FROM v_procurement_payments
GROUP BY publisher_name;

CREATE OR REPLACE VIEW v_procurement_payments_supplier_profile_by_tier AS
SELECT
    supplier_normalised,
    realisation_tier,
    COUNT(*) AS n_payments,
    COUNT(DISTINCT publisher_name) AS n_publishers,
    MIN(year)::INT AS min_year,
    MAX(year)::INT AS max_year,
    COUNT(DISTINCT vat_status) > 1 AS vat_mixed,
    COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum), 0) AS total_safe_eur
FROM v_procurement_payments
GROUP BY supplier_normalised, realisation_tier;

CREATE OR REPLACE VIEW v_procurement_payments_supplier_header AS
SELECT
    supplier_normalised,
    mode(supplier) AS supplier,
    mode(supplier_class) AS supplier_class,
    COUNT(DISTINCT publisher_name) AS n_publishers,
    MIN(year)::INT AS min_year,
    MAX(year)::INT AS max_year,
    COUNT(*) FILTER (WHERE realisation_tier = 'SPENT') AS n_paid_lines,
    COUNT(*) FILTER (WHERE realisation_tier = 'COMMITTED') AS n_ordered_lines,
    COALESCE(SUM(amount_spent_safe_eur), 0) AS paid_safe_eur,
    COALESCE(SUM(amount_committed_safe_eur), 0) AS ordered_safe_eur,
    COUNT(DISTINCT vat_status) > 1 AS vat_mixed,
    mode(cro_company_num) AS cro_company_num,
    mode(cro_company_status) AS cro_company_status
FROM v_procurement_payments
GROUP BY supplier_normalised;

CREATE OR REPLACE VIEW v_procurement_payments_supplier_publishers AS
SELECT
    supplier_normalised,
    realisation_tier,
    publisher_name,
    mode(publisher_type) AS publisher_type,
    mode(sector) AS sector,
    COUNT(*) AS n_payments,
    MIN(year)::INT AS min_year,
    MAX(year)::INT AS max_year,
    COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum), 0) AS total_safe_eur
FROM v_procurement_payments
GROUP BY supplier_normalised, realisation_tier, publisher_name;
