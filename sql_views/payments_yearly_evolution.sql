-- v_payments_yearly_evolution — one row per (member, year) with totals and year rank
-- Depends on: v_payments_base
--
-- Aggregation and ranking live here (pipeline layer), not in Streamlit.
-- rank_high: 1 = highest paid member for that year (most total PSA received)
--
-- party_name and constituency enriched by pipeline_sandbox/payments_member_enrichment.py
-- (normalise_join_key join against flattened_members.csv — 34th Dail members only).
-- Pre-34th Dail payment rows will have empty party_name/constituency by design.
--
-- Pre-computed window columns (Streamlit must not aggregate):
--   year_total_paid    — total PSA paid across all members for that year
--   year_member_count  — number of members with payments that year
--   year_avg_per_td    — simple average payment per TD that year
--   member_alltime_total — cumulative total across all years for that member

CREATE OR REPLACE VIEW v_payments_yearly_evolution AS
WITH per_member_year AS (
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
        COUNT(*)         AS payment_count
    FROM v_payments_base
    GROUP BY member_name, position, taa_band_raw, taa_band_label, payment_year
)
SELECT
    member_name,
    position,
    taa_band_raw,
    taa_band_label,
    payment_year,
    unique_member_code,
    party_name,
    constituency,
    total_paid,
    payment_count,
    RANK()  OVER (PARTITION BY payment_year ORDER BY total_paid DESC)           AS rank_high,
    SUM(total_paid) OVER (PARTITION BY payment_year)                            AS year_total_paid,
    COUNT(*) OVER (PARTITION BY payment_year)                                   AS year_member_count,
    ROUND(
        SUM(total_paid) OVER (PARTITION BY payment_year)
        / NULLIF(COUNT(*) OVER (PARTITION BY payment_year), 0),
        2
    )                                                                           AS year_avg_per_td,
    SUM(total_paid) OVER (PARTITION BY member_name)                             AS member_alltime_total
FROM per_member_year;
