-- v_la_accountability_summary — one-row national headline for the local-government
-- landing. Pulls the already-computed national figures from the per-council
-- accountability views so the page can show the big picture with no derivation.
--
-- ⚠️ Registration order: this view reads v_la_derelict_sites_levy /
-- v_la_planning_overturn / v_la_chief_executives, so it MUST register AFTER them
-- (handled by the order in connections.CONSTITUENCY_FILES).
CREATE OR REPLACE VIEW v_la_accountability_summary AS
SELECT
    (SELECT national_amount_levied_eur  FROM v_la_derelict_sites_levy LIMIT 1)        AS derelict_levied_eur,
    (SELECT national_total_received_eur FROM v_la_derelict_sites_levy LIMIT 1)        AS derelict_received_eur,
    (SELECT national_outstanding_eur    FROM v_la_derelict_sites_levy LIMIT 1)        AS derelict_outstanding_eur,
    (SELECT COUNT(*) FROM v_la_derelict_sites_levy WHERE levied_nothing)              AS n_councils_levied_nothing,
    (SELECT national_overturn_rate_pct  FROM v_la_planning_overturn LIMIT 1)          AS national_overturn_rate_pct,
    (SELECT COUNT(*) FROM v_la_chief_executives)                                      AS n_councils;
