-- v_procurement_supplier_dependency — for each supplier with ≥5 eTenders awards: what
-- share of its awards come from its single biggest contracting authority. Validated
-- 2026-06-11: of 1,405 such suppliers, 224 take ≥80% from one authority and 109 take
-- ≥95% (e.g. Global Rail Services — 53 of 58 awards from Irish Rail).
--
-- ⚠️ DEPENDENCY IS A STRUCTURE FACT, NOT A RISK VERDICT: a specialist firm serving the
-- one body that buys its specialism is the market working. Copy must stay in the form
-- "X won N of its M awards from Y" — no concentration-risk language.
--
-- ⚠️ CENTRAL PURCHASING BODIES: a top-authority of OGP/EPS means central-framework
-- mechanics (Dell winning 110/111 via OGP frameworks is how the system is designed),
-- not bilateral dependency — top_authority_is_central_purchasing lets the UI badge or
-- segment those rows (same name-pattern flag as v_procurement_incumbency).
--
-- Grain: one row per supplier_norm with ≥5 awards (company-class population). Award
-- counts are the dependency measure (values are ceilings and would distort shares).
CREATE OR REPLACE VIEW v_procurement_supplier_dependency AS
WITH pairs AS (
    SELECT
        supplier_norm,
        ANY_VALUE(supplier)              AS supplier,
        "Contracting Authority"          AS contracting_authority,
        COUNT(*)                         AS n
    FROM read_parquet('data/gold/parquet/procurement_awards.parquet')
    WHERE supplier_class = 'company'
      AND NOT name_truncated
      AND length(supplier_norm) >= 4
      AND supplier_norm NOT IN ('NULL', 'null')
    GROUP BY supplier_norm, "Contracting Authority"
),
ranked AS (
    SELECT
        supplier_norm,
        supplier,
        contracting_authority,
        n,
        SUM(n) OVER (PARTITION BY supplier_norm)                       AS total_awards,
        COUNT(*) OVER (PARTITION BY supplier_norm)                     AS n_authorities,
        ROW_NUMBER() OVER (PARTITION BY supplier_norm ORDER BY n DESC) AS rk
    FROM pairs
)
SELECT
    supplier_norm,
    supplier,
    contracting_authority                                     AS top_authority,
    contracting_authority ILIKE '%office of government procurement%'
        OR contracting_authority ILIKE '%education procurement service%'
                                                              AS top_authority_is_central_purchasing,
    n                                                         AS awards_from_top_authority,
    total_awards,
    n_authorities,
    ROUND(100.0 * n / total_awards, 1)                        AS top_authority_share_pct
FROM ranked
WHERE rk = 1 AND total_awards >= 5
ORDER BY top_authority_share_pct DESC, total_awards DESC;
