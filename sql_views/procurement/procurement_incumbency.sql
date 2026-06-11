-- v_procurement_incumbency — repeat-winner relationships: one row per supplier ×
-- contracting-authority pair, with how many DISTINCT YEARS the pair produced an award.
-- Validated 2026-06-11: 666 pairs span ≥4 years, 207 span ≥6, and the longest span 12
-- of the corpus's 13 years (e.g. Starrus Eco Holdings ↔ OGP, 70 awards / 12 years;
-- Deloitte Ireland ↔ OGP, 202 awards / 11 years).
--
-- ⚠️ A long streak is a STRUCTURE FACT, not a finding of wrongdoing — durable
-- incumbency is often the procurement system working (framework renewals, specialist
-- capability). Never rank with evaluative copy.
--
-- ⚠️ CENTRAL PURCHASING BODIES: The Office of Government Procurement (and the Education
-- Procurement Service) buy ON BEHALF OF the whole public service, so a "streak" with
-- them means repeated central-framework success, not a bilateral buyer relationship.
-- authority_is_central_purchasing flags this so a consuming UI can badge or segment it
-- (name-pattern flag; the proper fix is a curated central-purchasing-body list in
-- data/_meta if more bodies emerge).
--
-- Grain: one row per (supplier_norm, contracting_authority) with ≥2 awards. Company-
-- class population, dated rows only. Ceiling-aware: awarded_value_safe_eur sums only
-- value_safe_to_sum rows and remains "awarded, not paid".
CREATE OR REPLACE VIEW v_procurement_incumbency AS
SELECT
    supplier_norm,
    ANY_VALUE(supplier)                                       AS supplier,
    "Contracting Authority"                                   AS contracting_authority,
    "Contracting Authority" ILIKE '%office of government procurement%'
        OR "Contracting Authority" ILIKE '%education procurement service%'
                                                              AS authority_is_central_purchasing,
    COUNT(*)                                                  AS n_awards,
    COUNT(DISTINCT year(TRY_STRPTIME("Notice Published Date/Contract Created Date", '%d/%m/%Y')::DATE))
                                                              AS n_distinct_years,
    MIN(year(TRY_STRPTIME("Notice Published Date/Contract Created Date", '%d/%m/%Y')::DATE))
                                                              AS first_year,
    MAX(year(TRY_STRPTIME("Notice Published Date/Contract Created Date", '%d/%m/%Y')::DATE))
                                                              AS last_year,
    SUM(value_eur) FILTER (WHERE value_safe_to_sum)           AS awarded_value_safe_eur
FROM read_parquet('data/gold/parquet/procurement_awards.parquet')
WHERE supplier_class = 'company'
  AND NOT name_truncated
  AND length(supplier_norm) >= 4
  AND supplier_norm <> 'null'
  AND TRY_STRPTIME("Notice Published Date/Contract Created Date", '%d/%m/%Y') IS NOT NULL
GROUP BY supplier_norm, "Contracting Authority"
HAVING COUNT(*) >= 2
ORDER BY n_distinct_years DESC, n_awards DESC;
