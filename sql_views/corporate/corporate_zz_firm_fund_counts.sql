-- v_corporate_firm_fund_counts — the fund↔firm connection behind the Corporate
-- page's firm view (?firm=): for every curated professional firm tagged in
-- receiver_firms, how often each appointing parent fund/bank is co-named on the
-- same notice. One count per notice per distinct parent (list_distinct dedupes
-- repeat mentions within a notice), split into:
--   n_recv — receivership-shaped notices only (is_receivership_shaped on
--            v_corporate_notices: subtype = 'receivership' OR the
--            appointment-of-receiver wording) — the page's primary series;
--   n_all  — every notice naming the firm — the page's fallback when the firm
--            has no receivership-shaped co-mentions.
--
-- Graduated out of utility/pages_code/corporate.py::_explode_fund_counts
-- (logic-firewall audit 2026-07-16). Matching is notice PRESENCE (curated tag
-- over raw text), never a confirmed appointment — the page copy carries that
-- caveat. Free-text (non-curated) firms are absent here by construction; the
-- page's firm view degrades for those.
--
-- Grain: firm × parent.
-- Depends on v_corporate_notices — the zz_ filename keeps it loading after
-- corporate_corporate_notices.sql within the corporate_*.sql glob.
CREATE OR REPLACE VIEW v_corporate_firm_fund_counts AS
WITH firm_notice AS (
    SELECT
        unnest(receiver_firms)  AS firm,
        display_ref,
        is_receivership_shaped,
        parent_fund_mentions
    FROM v_corporate_notices
    WHERE has_receiver_firm
),
firm_parent AS (
    SELECT
        firm,
        display_ref,
        is_receivership_shaped,
        unnest(list_distinct(parent_fund_mentions)) AS parent
    FROM firm_notice
)
SELECT
    firm,
    parent,
    COUNT(*) FILTER (WHERE is_receivership_shaped) AS n_recv,
    COUNT(*)                                       AS n_all
FROM firm_parent
WHERE parent IS NOT NULL AND parent <> ''
GROUP BY firm, parent
ORDER BY firm, n_recv DESC, n_all DESC, parent;
