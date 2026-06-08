-- FILE named procurement_ted_awards_history.sql (not _winner_history) ON PURPOSE: views are
-- registered in ALPHABETICAL filename order (dail_tracker_core/db.py), and
-- v_procurement_ted_supplier_summary now reads THIS view — so this file must sort BEFORE it
-- (awards_history < supplier_summary). The view keeps the name v_procurement_ted_winner_history.
--
-- v_procurement_ted_winner_history — the FULL 2016-2026 TED Irish winner history, one
-- continuous (notice × winner) feed UNIONing the two ingestion lanes:
--   * source_lane='api'            — 2024+ eForms via the Search API (ted_ie_awards.parquet)
--   * source_lane='per_notice_xml' — 2016-2023 recovered from per-notice legacy TED_EXPORT
--                                    XML (ted_ie_winner_history.parquet), because the Search
--                                    API returns the WINNER at 0% for pre-2024 notices.
-- Both lanes run the SAME enrichment (extractors/ted_enrich.py) so classification / CRO match
-- / value flags are byte-identical and the UNION is sound. See doc/TED_ENRICHMENT.md §6.
--
-- ⚠️ AWARD GRAIN — never summed with eTenders or the public-payments spend facts (different
--    value_kind). pan-EU outliers + frameworks + large awards are excluded from sum-safe
--    totals downstream; COUNT is the trustworthy metric.
-- ⚠️ The eForms competition-intensity fields (procedure_type, single-bid, criteria) only exist
--    for source_lane='api' (2024+); they are NULL for the per-notice-XML backfill.
-- winner_name carries a `_NNNNN` org-id suffix on some 2024+ rows (eForms artefact); stripped
-- here for display, with a recovered winner_join_norm for cross-matching (matches
-- v_procurement_ted_awards). Harmless no-op on the pre-2024 rows.
CREATE OR REPLACE VIEW v_procurement_ted_winner_history AS
WITH unioned AS (
    SELECT
        'api' AS source_lane,
        publication_number, notice_url, buyer_name, winner_name, winner_name_norm,
        award_value_eur, currency, value_kind, value_safe_to_sum,
        is_pan_eu_outlier, is_multi_supplier_framework, n_winners,
        cpv_code, cpv_division, dispatch_date, year, month,
        supplier_class, cro_company_num, cro_company_status, cro_match_method, privacy_status,
        procedure_type, is_uncompetitive_procedure, n_tenders_received,
        is_single_bid, award_criteria_kind, is_price_only
    FROM read_parquet('data/silver/parquet/ted_ie_awards.parquet')

    UNION ALL BY NAME

    SELECT
        source_lane,
        publication_number, notice_url, buyer_name, winner_name, winner_name_norm,
        award_value_eur, currency, value_kind, value_safe_to_sum,
        is_pan_eu_outlier, is_multi_supplier_framework, n_winners,
        cpv_code, cpv_division, dispatch_date, year, month,
        supplier_class, cro_company_num, cro_company_status, cro_match_method, privacy_status,
        NULL::VARCHAR AS procedure_type,
        NULL::BOOLEAN AS is_uncompetitive_procedure,
        NULL::BIGINT  AS n_tenders_received,
        NULL::BOOLEAN AS is_single_bid,
        NULL::VARCHAR AS award_criteria_kind,
        NULL::BOOLEAN AS is_price_only
    FROM read_parquet('data/silver/parquet/ted_ie_winner_history.parquet')
    -- Boundary dedupe: a notice published in Jan but dispatched the prior Dec can land in
    -- BOTH lanes (api filters by publication-date>=2024, the XML backfill by dispatch-year
    -- 2016-2023). The API lane is authoritative (richer: real winner + competition fields),
    -- so drop any XML-lane PN that the API lane already has.
    WHERE publication_number NOT IN (
        SELECT publication_number FROM read_parquet('data/silver/parquet/ted_ie_awards.parquet')
    )
)
SELECT
    source_lane,
    publication_number,
    notice_url,
    buyer_name,
    regexp_replace(winner_name, '_[0-9]+$', '')       AS winner_name,
    regexp_replace(winner_name_norm, ' [0-9]+$', '')  AS winner_join_norm,
    award_value_eur,
    value_kind,
    value_safe_to_sum,
    is_pan_eu_outlier,
    is_multi_supplier_framework,
    n_winners,
    cpv_code,
    cpv_division,
    dispatch_date,
    year,
    month,
    supplier_class,
    cro_company_num,
    cro_company_status,
    cro_match_method,
    privacy_status,
    procedure_type,
    is_uncompetitive_procedure,
    n_tenders_received,
    is_single_bid,
    award_criteria_kind,
    is_price_only
FROM unioned;
