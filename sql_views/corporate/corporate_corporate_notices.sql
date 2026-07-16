-- v_corporate_notices — corporate distress / register notices from Iris Oifigiúil.
-- Source: data/gold/parquet/corporate_notices_enriched.parquet — the notices
--   SUPERSET written by extractors/corporate_receiver_enrich.py: every column of
--   the iris-produced corporate_notices.parquet (corporate_notices_enrichment.py)
--   PLUS the per-notice receiver flags the Corporate page used to derive at render
--   time (is_receivership / is_spv / has_parent_mention / receiver_firms /
--   has_receiver_firm). Falls back to nothing — the receiver enrichment runs right
--   after the iris chain, so the superset tracks the base gold. Personal insolvency
--   (individual bankruptcies) is excluded by policy at the enrichment level —
--   see [[feedback_personal_insolvency_privacy]].
--
-- Grain: one row per corporate notice across:
--   corporate_insolvency / corporate_notice / corporate_rescue /
--   investment_vehicle_register_notice.
--
-- brand_mentions + parent_fund_mentions are list columns tagged from the
-- curated data/_meta/loan_book_fund_aliases.csv (~25 starter entries).
-- receiver_firms lists curated professional firms named in raw_text. The
-- receiver-appointer ranking + operator-firm concentration are precomputed in
-- sql_views/corporate/corporate_receiver.sql (graduated out of the page).
--
-- Display/derived columns graduated out of the page (logic-firewall audit
-- 2026-07-16 — utility/pages_code/corporate.py used to derive these in pandas):
--   year                    INTEGER — enrichment-precomputed notice year
--   display_ref             VARCHAR — notice_ref, or a stable 'row-N' fallback
--                           for the split fragments whose notice_ref is null
--   parent_mentions_str     VARCHAR — parent_fund_mentions joined with ', '
--                           (the page's fund str.contains filter column)
--   is_receivership_shaped  BOOLEAN — subtype = 'receivership' OR the
--                           appointment-of-receiver wording in raw_text (the
--                           firm-view's receivership-shaped subset definition)
CREATE OR REPLACE VIEW v_corporate_notices AS
SELECT
    notice_ref,
    issue_date,
    issue_number,
    notice_category,
    notice_subtype,
    entity_name,
    display_title,
    title,
    raw_text,
    brand_mentions,
    parent_fund_mentions,
    fund_type_mentions,
    iris_source_pdf,
    is_receivership,
    is_spv,
    has_parent_mention,
    receiver_firms,
    has_receiver_firm,
    cbi_register,
    cbi_ref_no,
    year,
    COALESCE(
        NULLIF(TRIM(notice_ref), ''),
        'row-' || CAST(row_number() OVER (ORDER BY issue_date DESC NULLS LAST) - 1 AS VARCHAR)
    )                                                            AS display_ref,
    COALESCE(array_to_string(parent_fund_mentions, ', '), '')   AS parent_mentions_str,
    (
        notice_subtype = 'receivership'
        OR regexp_matches(
            COALESCE(raw_text, ''),
            '(?i)APPOINTMENT OF (STATUTORY )?RECEIVER|NOTICE OF APPOINTMENT OF RECEIVER'
        )
    )                                                            AS is_receivership_shaped
FROM read_parquet('data/gold/parquet/corporate_notices_enriched.parquet')
ORDER BY issue_date DESC NULLS LAST;
