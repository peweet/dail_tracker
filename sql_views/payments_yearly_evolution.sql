-- v_payments_yearly_evolution — one row per (member, year) with totals and year rank
-- Depends on: v_payments_base
--
-- Aggregation and ranking live here (pipeline layer), not in Streamlit.
-- rank_high: 1 = highest paid member for that year (most total PSA received)
--
-- party_name and constituency enriched by pipeline_sandbox/payments_member_enrichment.py
-- (normalise_join_key join against flattened_members.csv — 34th Dail members only).
-- Pre-34th Dail payment rows will have empty party_name/constituency by design.

CREATE OR REPLACE VIEW v_payments_yearly_evolution AS
SELECT
    member_name,
    position,
    taa_band_raw,
    taa_band_label,
    payment_year,
    MAX(COALESCE(unique_member_code, '')) AS unique_member_code,
    MAX(COALESCE(party_name,   ''))       AS party_name,
    MAX(COALESCE(constituency, ''))       AS constituency,
    SUM(amount_num)  AS total_paid,
    COUNT(*)         AS payment_count,
    RANK() OVER (
        PARTITION BY payment_year
        ORDER BY SUM(amount_num) DESC
    ) AS rank_high
FROM v_payments_base
GROUP BY member_name, position, taa_band_raw, taa_band_label, payment_year;
