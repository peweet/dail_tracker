-- v_procurement_charity_overlap — registered charities that also appear on the
-- public-procurement award register. The new question this answers: "which
-- State-funded / charitable bodies also win public contracts, and for how much?"
--
-- Sibling of v_procurement_lobbying_overlap and v_procurement_supplier_summary:
-- a CO-OCCURRENCE-BY-ENTITY join, NOT evidence of any impropriety. The link is a
-- shared CRO company number, which is a HARD identifier (not a fuzzy name match):
--   charity_resolved.cro_number  ==  procurement_supplier_cro_match.company_num
-- A charity declares its CRO number to the Charities Regulator; the procurement
-- supplier→CRO match resolves an awarded supplier's normalised name to the same
-- CRO row. When both point at the same company_num, the charity and the supplier
-- are the same registered legal entity.
--
-- Sources (all committed; charity side is silver, procurement side is gold):
--   data/silver/charities/charity_resolved.parquet         (RCN ⨝ CRO, Tier A)
--   data/gold/parquet/procurement_supplier_cro_match.parquet (supplier_norm → CRO)
--   data/gold/parquet/procurement_awards.parquet           (the awards)
--
-- Grain: ONE row per (rcn, supplier_norm). A single charity (one company_num) can
-- match more than one supplier_norm where the awards register holds trading-name
-- variants of the same company — each surfaces as its own row.
--
-- VALUE RULE (the project's money-grain firewall): awarded_value_safe_eur sums
-- ONLY value_safe_to_sum award rows — never the framework/DPS ceiling notices
-- (those are AWARD CEILINGS, not money paid). n_awards is the trustworthy count.
-- gov_funded_share_latest / share_government come straight from the charity's
-- latest annual return (0–1), so a high value with a large awarded total is the
-- "State funds it AND buys from it" pattern — surfaced as data, not a claim.
CREATE OR REPLACE VIEW v_procurement_charity_overlap AS
WITH supplier_awards AS (
    SELECT
        supplier_norm,
        mode(supplier)                                              AS matched_supplier_name,
        COUNT(*)                                                    AS n_awards,
        COUNT(DISTINCT "Contracting Authority")                     AS n_authorities,
        COALESCE(SUM(value_eur) FILTER (WHERE value_safe_to_sum), 0) AS awarded_value_safe_eur,
        COUNT(*) FILTER (WHERE value_safe_to_sum)                   AS n_value_safe_awards,
        COUNT(*) FILTER (WHERE is_framework_or_dps)                 AS n_ceiling_notices
    FROM read_parquet('data/gold/parquet/procurement_awards.parquet')
    WHERE supplier_class = 'company'
      AND NOT name_truncated
      AND length(supplier_norm) >= 4
    GROUP BY supplier_norm
),
supplier_cro AS (
    -- distinct supplier_norm → company_num (exact normalised-name CRO match only)
    SELECT supplier_norm, company_num, match_method AS cro_match_method
    FROM read_parquet('data/gold/parquet/procurement_supplier_cro_match.parquet')
    WHERE company_num IS NOT NULL
)
SELECT
    ch.rcn,
    ch.registered_charity_name,
    ch.cro_number                              AS company_num,
    ch.company_status,
    ch.classification_primary                  AS charity_classification,
    ch.state_adjacent_flag,
    ch.funding_profile,
    ch.dominant_income_source,
    ch.gov_funded_share_latest,
    ch.share_government,
    ch.gross_income_latest_eur,
    sc.supplier_norm,
    sc.cro_match_method,
    sa.matched_supplier_name,
    sa.n_awards,
    sa.n_authorities,
    sa.awarded_value_safe_eur,
    sa.n_value_safe_awards,
    sa.n_ceiling_notices
FROM read_parquet('data/silver/charities/charity_resolved.parquet') ch
JOIN supplier_cro sc
    ON ch.cro_number = sc.company_num
JOIN supplier_awards sa
    ON sc.supplier_norm = sa.supplier_norm
WHERE ch.cro_number IS NOT NULL
ORDER BY sa.awarded_value_safe_eur DESC, sa.n_awards DESC;
