-- v_procurement_entity_chain — one row per CRO-matched company, showing which of the
-- three procurement registers it appears in: eTenders awards (national, ceilings), TED
-- awards (EU Official Journal, ceilings), and the public-body payments fact (realised
-- SPENT/COMMITTED). This is the backbone of a unified supplier profile: the same legal
-- entity currently surfaces in up to four disconnected places in the UI.
--
-- ⚠️ THE THREE MONEY COLUMNS ARE DIFFERENT GRAINS AND MUST NEVER BE SUMMED OR COMPARED
-- AS TOTALS (award ceilings ≠ realised payments — see doc/DATA_MAP.md). They appear
-- side by side strictly so a profile can show each register's own number with its own
-- label. Absence from a register is REGISTER COVERAGE, not missing money: only ~7% of
-- State spend is visible in the payments corpus, so most awarded suppliers legitimately
-- have no payment trace (68% of CRO-matched awarded suppliers, measured 2026-06-11).
--
-- Grain: one row per company_num present in at least one register. Joins are hard CRO
-- company-number matches only (no fuzzy name joins at this level); match provenance
-- stays on the per-register tables.
CREATE OR REPLACE VIEW v_procurement_entity_chain AS
WITH etenders AS (
    SELECT
        c.company_num,
        ANY_VALUE(c.supplier)                                     AS etenders_supplier_name,
        COUNT(*)                                                  AS etenders_award_rows,
        COUNT(DISTINCT a."Contracting Authority")                 AS etenders_n_authorities,
        SUM(a.value_eur) FILTER (WHERE a.value_safe_to_sum)       AS etenders_awarded_value_safe_eur
    FROM read_parquet('data/gold/parquet/procurement_supplier_cro_match.parquet') c
    JOIN read_parquet('data/gold/parquet/procurement_awards.parquet') a USING (supplier_norm)
    WHERE c.company_num IS NOT NULL
    GROUP BY c.company_num
),
ted AS (
    SELECT
        cro_company_num                                           AS company_num,
        ANY_VALUE(winner_name)                                    AS ted_winner_name,
        COUNT(*)                                                  AS ted_awards,
        COUNT(DISTINCT buyer_name)                                AS ted_n_buyers,
        SUM(award_value_eur) FILTER (WHERE value_safe_to_sum AND NOT is_pan_eu_outlier)
                                                                  AS ted_value_safe_eur
    FROM read_parquet('data/silver/parquet/ted_ie_awards.parquet')
    WHERE cro_company_num IS NOT NULL
    GROUP BY cro_company_num
),
payments AS (
    SELECT
        cro_company_num                                           AS company_num,
        ANY_VALUE(supplier_normalised)                            AS payments_supplier_name,
        COUNT(*)                                                  AS payment_lines,
        COUNT(DISTINCT publisher_id)                              AS payments_n_publishers,
        SUM(amount_eur) FILTER (WHERE value_safe_to_sum AND realisation_tier = 'SPENT')
                                                                  AS paid_safe_eur,
        SUM(amount_eur) FILTER (WHERE value_safe_to_sum AND realisation_tier = 'COMMITTED')
                                                                  AS committed_safe_eur
    FROM read_parquet('data/gold/parquet/procurement_payments_fact.parquet')
    WHERE cro_company_num IS NOT NULL AND public_display
    GROUP BY cro_company_num
)
SELECT
    COALESCE(e.company_num, t.company_num, p.company_num)         AS company_num,
    COALESCE(e.etenders_supplier_name, t.ted_winner_name, p.payments_supplier_name)
                                                                  AS display_name,
    e.company_num IS NOT NULL                                     AS in_etenders,
    t.company_num IS NOT NULL                                     AS in_ted,
    p.company_num IS NOT NULL                                     AS in_payments,
    (e.company_num IS NOT NULL)::INT + (t.company_num IS NOT NULL)::INT
        + (p.company_num IS NOT NULL)::INT                        AS n_registers,
    e.etenders_award_rows,
    e.etenders_n_authorities,
    e.etenders_awarded_value_safe_eur,
    t.ted_awards,
    t.ted_n_buyers,
    t.ted_value_safe_eur,
    p.payment_lines,
    p.payments_n_publishers,
    p.paid_safe_eur,
    p.committed_safe_eur
FROM etenders e
FULL OUTER JOIN ted t ON e.company_num = t.company_num
FULL OUTER JOIN payments p ON COALESCE(e.company_num, t.company_num) = p.company_num
ORDER BY n_registers DESC, p.paid_safe_eur DESC NULLS LAST;
