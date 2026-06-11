-- v_procurement_new_entrants — share of eTenders awards going to FIRST-TIME suppliers,
-- per year. The validated headline (2026-06-11): 51.3% of 2016 awards went to suppliers
-- with no prior award in the corpus; by 2024 that share was 17.3%. A shrinking entry
-- rate is a structural market fact — consistent with consolidation, framework
-- centralisation, or simply a maturing register. PRESENT THE SHAPE, NEVER A VERDICT.
--
-- ⚠️ LEFT-CENSORING: the corpus starts 2013-01, so a supplier's "first award" is its
-- first award IN THE CORPUS — early years are inflated by definition (2013 is 100% new
-- by construction). is_left_censored marks years before 2016; a consuming UI should
-- visually de-emphasise or exclude those years. The 2018→2024 halving survives this.
--
-- Grain: one row per calendar year. Company-class suppliers only (same population as
-- v_procurement_supplier_summary): excludes truncated names, short norms, literal 'NULL'.
CREATE OR REPLACE VIEW v_procurement_new_entrants AS
WITH base AS (
    SELECT
        supplier_norm,
        year(TRY_STRPTIME("Notice Published Date/Contract Created Date", '%d/%m/%Y')::DATE) AS yr
    FROM read_parquet('data/gold/parquet/procurement_awards.parquet')
    WHERE supplier_class = 'company'
      AND NOT name_truncated
      AND length(supplier_norm) >= 4
      AND supplier_norm <> 'null'
      AND TRY_STRPTIME("Notice Published Date/Contract Created Date", '%d/%m/%Y') IS NOT NULL
),
first_seen AS (
    SELECT supplier_norm, MIN(yr) AS first_yr FROM base GROUP BY supplier_norm
)
SELECT
    b.yr                                                          AS year,
    COUNT(*)                                                      AS n_awards,
    COUNT(DISTINCT b.supplier_norm)                               AS n_suppliers,
    COUNT(DISTINCT b.supplier_norm) FILTER (WHERE f.first_yr = b.yr)
                                                                  AS n_new_suppliers,
    COUNT(*) FILTER (WHERE f.first_yr = b.yr)                     AS n_awards_to_new,
    ROUND(100.0 * COUNT(*) FILTER (WHERE f.first_yr = b.yr) / COUNT(*), 1)
                                                                  AS pct_awards_to_new_entrants,
    b.yr < 2016                                                   AS is_left_censored
FROM base b
JOIN first_seen f USING (supplier_norm)
GROUP BY b.yr
ORDER BY b.yr;
