-- v_payments_alltime_summary — single hero-row totals for the Rankings view
--
-- Audit fix (2026-05-26): replaces fetch_since_2020_summary in
-- utility/data_access/payments_data.py which read parquet directly + ran
-- `.sum()` / `.n_unique()` in Streamlit. Both are forbidden by the
-- payments.yaml contract. The aggregation now happens here, in the
-- pipeline layer, and the page does a one-row SELECT.

-- One summary row per house so the Rankings hero strip scopes to the selected
-- chamber (Senators are not counted in the "TDs with payments" total).
CREATE OR REPLACE VIEW v_payments_alltime_summary AS
SELECT
    house,
    SUM(total_paid_since_2020)                                       AS total_paid_since_2020,
    COUNT(*)                                                         AS member_count,
    ROUND(SUM(total_paid_since_2020) / NULLIF(COUNT(*), 0), 2)       AS avg_per_td_since_2020
FROM v_payments_alltime_ranking
GROUP BY house;
