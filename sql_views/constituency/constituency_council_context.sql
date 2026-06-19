-- v_constituency_council_context — for each constituency, the local authority(ies)
-- serving it with each council's OWN published money, surfaced side by side as
-- CONTEXT. One row per (constituency, serving council).
--
-- Joins the crosswalk (v_constituency_la_crosswalk) to the existing council-grain
-- procurement/AFS views (read directly; this view registers after them). Carries,
-- per council: the latest-year audited REVENUE-account spend + NET cost, the
-- latest-year CAPITAL investment, and the purchase-order/payment totals.
--
-- ⚠️ FOUR DISTINCT GRAINS, NEVER SUMMED, NEVER APPORTIONED:
--   * afs_revenue_gross_eur / afs_revenue_net_eur  — BUDGET/accounts (running a service)
--   * capital_expenditure_eur                      — CAPITAL (building/acquiring)
--   * ordered_safe_eur / paid_safe_eur             — PURCHASE-ORDER / PAYMENT (>€20k)
-- They are different stages/accounts of council money. AND the council area is not
-- the constituency — these are the council's WHOLE-AREA figures, shown because the
-- council serves (part of) this constituency. link_type='partial' => sliver. The
-- page must label both facts: "council area, not apportioned".
CREATE OR REPLACE VIEW v_constituency_council_context AS
WITH afs_rev AS (
    SELECT council, year, gross_expenditure_eur, net_expenditure_eur,
           ROW_NUMBER() OVER (PARTITION BY council ORDER BY year DESC) AS rn
    FROM v_procurement_afs_total_by_year
),
afs_cap AS (
    SELECT council, year, capital_expenditure_eur,
           ROW_NUMBER() OVER (PARTITION BY council ORDER BY year DESC) AS rn
    FROM v_procurement_afs_capital_by_year
)
SELECT
    x.constituency_name,
    x.local_authority,
    x.link_type,
    x.la_serves_multiple_constituencies,
    x.constituency_multi_la,
    -- audited revenue account (latest year)
    r.year                       AS afs_revenue_year,
    r.gross_expenditure_eur      AS afs_revenue_gross_eur,
    r.net_expenditure_eur        AS afs_revenue_net_eur,
    -- capital account (latest year)
    c.year                       AS capital_year,
    c.capital_expenditure_eur    AS capital_expenditure_eur,
    -- purchase orders / payments over €20k
    cs.n_suppliers,
    cs.min_year                  AS po_min_year,
    cs.max_year                  AS po_max_year,
    cs.ordered_safe_eur,
    cs.paid_safe_eur,
    cs.n_ordered,
    cs.n_paid,
    (cs.council IS NOT NULL OR r.council IS NOT NULL OR c.council IS NOT NULL) AS has_spending_data
FROM v_constituency_la_crosswalk x
LEFT JOIN v_procurement_council_summary cs ON cs.council = x.local_authority
LEFT JOIN afs_rev r ON r.council = x.local_authority AND r.rn = 1
LEFT JOIN afs_cap c ON c.council = x.local_authority AND c.rn = 1
ORDER BY
    x.constituency_name,
    (x.link_type = 'primary') DESC,
    GREATEST(COALESCE(r.gross_expenditure_eur, 0), COALESCE(cs.ordered_safe_eur, 0),
             COALESCE(cs.paid_safe_eur, 0)) DESC;
