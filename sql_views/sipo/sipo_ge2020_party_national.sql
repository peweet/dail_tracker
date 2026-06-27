-- GE2020 National-Agent itemised expenses — the GE2020 counterpart of
-- sipo_party_national_expenses.sql (GE2024). SEPARATE gold facts + SEPARATE views so the
-- live GE2024 page is untouched. OCR-derived from the scanned GE2020 National-Agent returns
-- (doc/OCR_RUN_ASSESSMENT_2026_06_26.md); same contract as GE2024:
--   * `category_total_eur` (is_overall) is the PRINTED official figure — the trustworthy headline.
--   * `reconciles` flags parties whose OCR'd line items don't sum to it (SF/IFP/Aontú — a
--     duplicate upload + ×100 decimal-drops on SF; verify against the official SIPO PDF).
-- DO NOT SUM with Part-3 candidate apportionment or across elections.

-- v_sipo_ge2020_party_national_categories — one row per party × heading (8 sections + Overall).
CREATE OR REPLACE VIEW v_sipo_ge2020_party_national_categories AS
SELECT
    'General Election 2020'  AS election,
    party,
    section,
    category                 AS category_label,
    category_total_eur,  -- NB: non-reconciling overalls (SF etc.) can be OCR decimal-loss
                         -- mis-reads; the page marks reconciles=false "items partial". A
                         -- cross-election fix to suppress garbled overalls is pending (needs
                         -- Streamlit verification) — see doc/OCR_RUN_ASSESSMENT_2026_06_26.md.
    items_sum_eur,
    reconciles,
    is_overall,
    total_confidence,
    source_page
FROM read_parquet('data/gold/parquet/sipo_ge2020_expense_categories.parquet');

-- v_sipo_ge2020_party_national_items — line-item grain (Ref A1.., supplier/description + cost).
CREATE OR REPLACE VIEW v_sipo_ge2020_party_national_items AS
SELECT
    'General Election 2020'  AS election,
    party,
    section,
    category                 AS category_label,
    ref,
    item_description,
    cost_eur,
    cost_confidence,
    -- flag wasn't carried into the GE2020 silver; derive it here from the confidences,
    -- mirroring sipo_expense_items_paddle_etl (ok / low_confidence_verify / no_cost).
    CASE
        WHEN cost_eur IS NULL THEN 'no_cost'
        WHEN COALESCE(cost_confidence, 1) < 0.85 OR COALESCE(row_min_confidence, 1) < 0.85
            THEN 'low_confidence_verify'
        ELSE 'ok'
    END                      AS flag,
    (cost_eur IS NOT NULL
        AND COALESCE(cost_confidence, 1) >= 0.85
        AND COALESCE(row_min_confidence, 1) >= 0.85) AS is_verified,
    source_page
FROM read_parquet('data/gold/parquet/sipo_ge2020_expense_items.parquet');
