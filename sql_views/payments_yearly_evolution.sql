-- v_payments_yearly_evolution — one row per (member, year) with totals and year rank
-- Depends on: v_payments_base
--
-- Aggregation and ranking live here (pipeline layer), not in Streamlit.
-- rank_high: 1 = highest paid member for that year (most total PSA received)
--
-- TODO_PIPELINE_VIEW_REQUIRED: party_name and constituency (not in payments source CSV;
-- requires canonical member_id join to member reference table)

CREATE OR REPLACE VIEW v_payments_yearly_evolution AS
SELECT
    member_name,
    position,
    taa_band_raw,
    taa_band_label,
    payment_year,
    SUM(amount_num)  AS total_paid,
    COUNT(*)         AS payment_count,
    RANK() OVER (
        PARTITION BY payment_year
        ORDER BY SUM(amount_num) DESC
    ) AS rank_high
FROM v_payments_base
GROUP BY member_name, position, taa_band_raw, taa_band_label, payment_year;
