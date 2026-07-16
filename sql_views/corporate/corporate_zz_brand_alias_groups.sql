-- v_corporate_brand_alias_groups — the methodology-expander table on the
-- Corporate page: the curated brand → parent_fund → fund_type alias map rolled
-- up to one row per (parent_fund, fund_type) with its brand strings joined.
-- Graduated out of utility/pages_code/corporate.py::_render_methodology_expander
-- (logic-firewall audit 2026-07-16).
--
-- Source of truth stays data/_meta/loan_book_fund_aliases.csv via
-- v_corporate_brand_aliases. That CSV currently carries ONLY
-- brand/parent_fund/fund_type — if a curated `notes` column is (re)introduced,
-- extend this view to aggregate it into notes_concat (string_agg of distinct
-- non-blank notes, ' · '-joined); the page renders notes_concat as-is.
--
-- Grain: parent_fund × fund_type.
-- Depends on v_corporate_brand_aliases — the zz_ filename keeps it loading
-- after corporate_brand_aliases.sql within the corporate_*.sql glob.
CREATE OR REPLACE VIEW v_corporate_brand_alias_groups AS
SELECT
    parent_fund,
    fund_type,
    string_agg(brand, ', ' ORDER BY brand) AS brands,
    ''                                     AS notes_concat
FROM v_corporate_brand_aliases
GROUP BY parent_fund, fund_type
ORDER BY fund_type, parent_fund;
