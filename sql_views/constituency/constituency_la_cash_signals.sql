-- v_la_cash_signals — a council's three published finance / collection figures shown
-- together (so a reader sees them in one place rather than hunting across separate cards),
-- each beside its national median:
--   * revenue account balance (% of income)   — NOAC M1
--   * commercial-rates collection (%)          — NOAC M2
--   * derelict-site levy collection (%)        — Dept of Housing return
--
-- DISPLAY-ONLY: published values + benchmarks. NO composite score, and NO relationship is
-- asserted between the three — they are independent figures. (An earlier "financial stress"
-- framing — that a council weak on one tends to be weak on the others — was REMOVED: the
-- apparent cross-council correlation (Pearson 0.44) collapses to ~0.05 once the single
-- Sligo outlier is dropped, so co-variation is not stated as fact. The firewall forbids
-- asserting an unsupported relationship even in comments.)
--
-- Built by JOINing three already-registered council views (each has done its own la_map
-- crosswalk to the local_authority key), so this view must register AFTER them. The
-- derelict source view exposes no median, so it is computed here as a window aggregate.
CREATE OR REPLACE VIEW v_la_cash_signals AS
SELECT
    s.local_authority,
    2024                                                       AS year,
    s.revenue_balance_pct,
    s.nat_revenue_balance_pct,
    c.commercial_rates_pct,
    c.nat_commercial_rates_pct,
    d.collection_rate_pct                                      AS derelict_collection_pct,
    ROUND(MEDIAN(d.collection_rate_pct) OVER (), 1)            AS nat_derelict_collection_pct
FROM v_la_noac_scorecard s
JOIN v_la_collection_rates c ON c.local_authority = s.local_authority
LEFT JOIN v_la_derelict_sites_levy d ON d.local_authority = s.local_authority
ORDER BY s.local_authority;
