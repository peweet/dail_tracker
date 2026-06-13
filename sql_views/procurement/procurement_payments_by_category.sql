-- v_payments_by_category / _by_category_publisher — the "where money goes, BY WHAT IT WAS FOR" lens
-- over data/gold/parquet/procurement_payments_fact.parquet.
--
-- spend_category is SOURCE-GROUNDED: it is the publisher's OWN published purpose text (the source
-- `description`), canonicalised in the pipeline ONLY for truncation + casing — never an invented
-- taxonomy (owner decision 2026-06-13, "department's exact words"). So a category like
-- "IP Accommodation" / "School Building Projects" / "Passport Booklets" IS the department's published
-- label, merely de-noised — verifiable, inference-free. Derived once in
-- extractors/procurement_payments_consolidate.py (canon_spend_category); the UI must NOT re-derive it.
--
-- RAILS (identical to v_public_payments):
--   • PRIVACY GATE — public_display = TRUE only (personal/sole-trader rows quarantined upstream).
--   • SUM-SAFE — only value_safe_to_sum rows contribute (excludes ceilings, blank-supplier, and
--     public_body intergovernmental transfers).
--   • ONE TIER PER ROW — GROUP BY realisation_tier so SPENT (paid) and COMMITTED (ordered) are NEVER
--     blended into a single figure; render the verb ("paid €X" / "ordered €X"), never a bare €.
--   • Rows with no published description surface as 'Uncategorised' (≈15%), shown honestly, never hidden.

CREATE OR REPLACE VIEW v_payments_by_category AS
SELECT
    COALESCE(spend_category, 'Uncategorised')        AS spend_category,
    realisation_tier,
    COUNT(*)                                         AS n_lines,
    COUNT(DISTINCT publisher_id)                     AS n_bodies,
    COUNT(DISTINCT supplier_normalised)              AS n_suppliers,
    MIN(year)::INT                                   AS first_year,
    MAX(year)::INT                                   AS last_year,
    SUM(amount_eur)                                  AS total_safe_eur
FROM read_parquet('data/gold/parquet/procurement_payments_fact.parquet')
WHERE value_safe_to_sum
  AND public_display
GROUP BY 1, 2
ORDER BY total_safe_eur DESC;

-- Per-body drill-down: "what did THIS department/council spend on?" (e.g. Justice → IP Accommodation
-- €580m). Same rails; one row per (publisher × category × tier).
CREATE OR REPLACE VIEW v_payments_by_category_publisher AS
SELECT
    publisher_id,
    MODE(publisher_name)                             AS publisher_name,
    MODE(publisher_type)                             AS publisher_type,
    COALESCE(spend_category, 'Uncategorised')        AS spend_category,
    realisation_tier,
    COUNT(*)                                         AS n_lines,
    COUNT(DISTINCT supplier_normalised)              AS n_suppliers,
    MIN(year)::INT                                   AS first_year,
    MAX(year)::INT                                   AS last_year,
    SUM(amount_eur)                                  AS total_safe_eur
FROM read_parquet('data/gold/parquet/procurement_payments_fact.parquet')
WHERE value_safe_to_sum
  AND public_display
GROUP BY publisher_id, 4, realisation_tier
ORDER BY total_safe_eur DESC;

-- FULL TRANSPARENCY DRILL: "this category → these named vendors → these amounts" for EVERY category.
-- Grain = one row per (category × published supplier name × tier). This is the most granular,
-- fully-traceable view: every row maps back to the publisher's own over-€20k disclosure.
--
-- VENDOR IDENTITY IS NOT MERGED. The published supplier name is the grain; cro_company_num is SURFACED
-- (not grouped on) so a consumer can roll up to a legal entity WHERE a CRO match exists — but the view
-- never fabricates an operator-merge. This matters: the data shows e.g. "Bridgestock" (CRO 342894) and
-- "Bridgestock Care" (CRO 587776) as DIFFERENT registrations, and "Mosney" (CRO 11917) vs "Mosney
-- Holidays" (no CRO) — merging them would be an unverifiable guess, so each published name stays distinct.
-- Same rails as above: public_display + value_safe_to_sum, one tier per row, never blended.
CREATE OR REPLACE VIEW v_payments_category_suppliers AS
SELECT
    COALESCE(spend_category, 'Uncategorised')        AS spend_category,
    MODE(supplier_raw)                               AS supplier,
    supplier_normalised,
    MODE(cro_company_num)                            AS cro_company_num,   -- surfaced for optional roll-up; NULL if unmatched
    realisation_tier,
    COUNT(*)                                         AS n_lines,
    COUNT(DISTINCT publisher_id)                     AS n_bodies,
    MIN(year)::INT                                   AS first_year,
    MAX(year)::INT                                   AS last_year,
    SUM(amount_eur)                                  AS total_safe_eur
FROM read_parquet('data/gold/parquet/procurement_payments_fact.parquet')
WHERE value_safe_to_sum
  AND public_display
  AND supplier_normalised IS NOT NULL
  AND TRIM(supplier_normalised) <> ''
GROUP BY 1, supplier_normalised, realisation_tier
ORDER BY total_safe_eur DESC;
