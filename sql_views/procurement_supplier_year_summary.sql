-- v_procurement_supplier_year_summary — per-(supplier, year) version of
-- v_procurement_supplier_summary, powering the Procurement page's year-pill filter.
-- Identical gate (company-class, non-truncated, supplier_norm >= 4 chars), identical
-- value-safety (only value_safe_to_sum is summed) and identical CRO + lobbying joins;
-- the only addition is a `year` dimension parsed from the award date.
--
-- The CRO match and lobbying flags are entity-level (year-independent), so they
-- repeat across an entity's yearly rows — exactly as the all-time view carries them.
-- Rows with an unparseable date are dropped so a year filter is exact (the company
-- slice has a full date in the source, so nothing real is lost).
CREATE OR REPLACE VIEW v_procurement_supplier_year_summary AS
WITH agg AS (
    SELECT
        supplier_norm,
        EXTRACT(year FROM TRY_STRPTIME("Notice Published Date/Contract Created Date", '%d/%m/%Y'))::INT AS year,
        mode(supplier)                                              AS supplier,
        COUNT(*)                                                    AS n_awards,
        COUNT(DISTINCT "Contracting Authority")                     AS n_authorities,
        COALESCE(SUM(value_eur) FILTER (WHERE value_safe_to_sum), 0) AS awarded_value_safe_eur
    FROM read_parquet('data/gold/parquet/procurement_awards.parquet')
    WHERE supplier_class = 'company'
      AND NOT name_truncated
      AND length(supplier_norm) >= 4
      AND TRY_STRPTIME("Notice Published Date/Contract Created Date", '%d/%m/%Y') IS NOT NULL
    GROUP BY supplier_norm, year
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
    a.year,
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
