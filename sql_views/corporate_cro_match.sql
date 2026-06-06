-- v_corporate_cro_notice_match — per-notice CRO registration badge for the
--                                 Corporate page.
--
-- Source: data/gold/parquet/cro_xref_corporate_notices.parquet
--   produced by extractors/cro_corporate_xref_enrichment.py (the PROMOTED
--   output, committed gold, run as the `cro` pipeline chain). It is an
--   inner-join of v_corporate_notices entity_name against the de-duplicated CRO
--   silver company index, EXACT normalised match only, restricted to names that
--   resolve to exactly ONE company (ambiguous names get no badge).
--
-- Why this lives behind sql_views/corporate_*.sql (mirrors the CBI xref):
--   every row is a strict subset of the corporate-notices grain — one notice
--   joined to the CRO company that the named entity is registered as. The page
--   uses it for display-only annotation (status pill + reg/dissolved date) — no
--   civic claim beyond "the wound-up / receivership entity on this notice is the
--   CRO company numbered X, status Y".
--
-- Keyed on entity_norm (not notice_ref): some corporate_notices rows lack
-- notice_ref upstream (40% null / blank), so the page joins on its own
-- normalised display key, exactly as it does for v_corporate_cbi_notice_match.
CREATE OR REPLACE VIEW v_corporate_cro_notice_match AS
SELECT
    notice_ref,
    entity_name,
    entity_norm,
    issue_date,
    notice_category,
    notice_subtype,
    company_num,
    company_status,
    company_reg_date,
    comp_dissolved_date,
    status_pill_value
FROM read_parquet('data/gold/parquet/cro_xref_corporate_notices.parquet')
WHERE entity_norm IS NOT NULL;
