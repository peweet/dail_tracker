-- v_procurement_supplier_summary — the main supplier ranking for the Procurement
-- page. One row per distinct normalised supplier (company-class, non-truncated),
-- with the CRO registration match and the lobbying-register overlap folded in.
--
-- Sources (all committed gold):
--   procurement_awards.parquet               (the awards)
--   procurement_supplier_cro_match.parquet   (exact normalised-name -> CRO)
--   procurement_lobbying_overlap.parquet     (supplier appears on lobbying register)
--
-- n_awards is the TRUSTWORTHY metric (counts). awarded_value_safe_eur sums only
-- value_safe_to_sum rows — never the framework-ceiling naive total. The lobbying
-- flags are co-occurrence by entity only (NOT influence) — see the xref header.
CREATE OR REPLACE VIEW v_procurement_supplier_summary AS
WITH agg AS (
    SELECT
        supplier_norm,
        mode(supplier)                                              AS supplier,
        COUNT(*)                                                    AS n_awards,
        COUNT(DISTINCT "Contracting Authority")                     AS n_authorities,
        COALESCE(SUM(value_eur) FILTER (WHERE value_safe_to_sum), 0) AS awarded_value_safe_eur
    FROM read_parquet('data/gold/parquet/procurement_awards.parquet')
    WHERE supplier_class = 'company'
      AND NOT name_truncated
      AND length(supplier_norm) >= 4
    GROUP BY supplier_norm
),
ov AS (
    SELECT
        supplier_norm,
        SUM(n_lobby_returns)                AS lobbying_returns,
        BOOL_OR(lobby_side = 'registrant')  AS is_lobbying_registrant,
        BOOL_OR(lobby_side = 'client')      AS is_lobbying_client
    FROM read_parquet('data/gold/parquet/procurement_lobbying_overlap.parquet')
    GROUP BY supplier_norm
)
SELECT
    a.supplier,
    a.supplier_norm,
    a.n_awards,
    a.n_authorities,
    a.awarded_value_safe_eur,
    c.company_num,
    c.company_status,
    c.match_method                            AS cro_match_method,
    (o.supplier_norm IS NOT NULL)             AS on_lobbying_register,
    COALESCE(o.lobbying_returns, 0)           AS lobbying_returns,
    COALESCE(o.is_lobbying_registrant, FALSE) AS is_lobbying_registrant,
    COALESCE(o.is_lobbying_client, FALSE)     AS is_lobbying_client
FROM agg a
LEFT JOIN read_parquet('data/gold/parquet/procurement_supplier_cro_match.parquet') c
    ON a.supplier_norm = c.supplier_norm
LEFT JOIN ov o ON a.supplier_norm = o.supplier_norm
ORDER BY a.n_awards DESC;
