-- v_procurement_ted_awards — the TED (EU Official Journal / Tenders Electronic Daily)
-- Irish award-notice feed, exposed to the Procurement page as a SEPARATE register from
-- eTenders. Reads the enriched SILVER parquet (same pattern as the lobbying-overlap view,
-- which also reads silver) — TED is already cleaned/CRO-matched/value-gated upstream by
-- extractors/ted_ireland_extract.py; this view is the display feed on top.
--
-- ⚠️ AWARD GRAIN, never summed with eTenders. 66% of TED winners also appear in eTenders
-- (same firm, two registers) — cross-reference per firm, but NEVER add the totals.
-- ⚠️ pan-EU outliers (is_pan_eu_outlier, e.g. GÉANT research frameworks where Ireland is
-- one of dozens of participants) carry vast ceilings and are EXCLUDED from value totals by
-- default downstream — €586bn of the €624bn naive TED total is just 375 such rows.
--
-- winner_name carries a `_NNNNN` org-id suffix on ~9% of rows (a TED eForms artefact); it
-- is stripped here for display and a recovered join-norm is derived so those rows can still
-- cross-match eTenders. The proper fix is to clean before normalising in the extractor.
CREATE OR REPLACE VIEW v_procurement_ted_awards AS
SELECT
    publication_number,
    notice_url,
    buyer_name,
    regexp_replace(winner_name, '_[0-9]+$', '')        AS winner_name,
    -- recovered join key: drop the trailing " NNNNN" the suffix leaves in the norm
    regexp_replace(winner_name_norm, ' [0-9]+$', '')   AS winner_join_norm,
    award_value_eur,
    value_kind,
    value_safe_to_sum,
    is_pan_eu_outlier,
    is_multi_supplier_framework,
    cpv_code,
    cpv_division,
    dispatch_date,
    year,
    supplier_class,
    cro_company_num,
    cro_company_status,
    cro_match_method,
    privacy_status
FROM read_parquet('data/silver/parquet/ted_ie_awards.parquet');
