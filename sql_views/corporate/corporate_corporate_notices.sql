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
    has_receiver_firm
FROM read_parquet('data/gold/parquet/corporate_notices_enriched.parquet')
ORDER BY issue_date DESC NULLS LAST;
