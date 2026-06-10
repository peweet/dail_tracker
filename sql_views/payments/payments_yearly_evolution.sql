-- v_payments_yearly_evolution — one row per (member, year) with totals and year rank
-- Depends on: v_payments_base
--
-- Aggregation and ranking live here (pipeline layer), not in Streamlit.
-- rank_high: 1 = highest paid member for that year (most total PSA received)
--
-- party_name and constituency enriched by payments_member_enrichment.py
-- (normalise_join_key join against flattened_members.csv — 34th Dail members only).
-- Pre-34th Dail payment rows will have empty party_name/constituency by design.
--
-- Pre-computed window columns (Streamlit must not aggregate):
--   year_total_paid    — total PSA paid across all members for that year
--   year_member_count  — number of members with payments that year
--   year_avg_per_td    — simple average payment per TD that year
--   member_alltime_total — cumulative total across all years for that member

-- `house` is threaded through the grain and EVERY window so the Dáil and
-- Seanad rankings/totals never mix. Before this, v_payments_base unioned both
-- chambers but this view partitioned by payment_year alone, silently blending
-- ~60 Senators into the TD year rankings + per-year totals (and downstream
-- into the all-time ranking/summary). Partitioning by (payment_year, house)
-- gives each chamber its own rank_high #1 and its own year_total/avg.
--
-- Grain fix (2026-06-11): position and taa_band_raw/label used to be part of
-- the GROUP BY, so a member whose position or TAA band changed mid-year split
-- into TWO rows for that year (duplicate "All years" rows, overlapping chart
-- bars, and a LIMIT-1 year summary that silently dropped the other row's
-- total). Member identity is unique_member_code when enriched (the source
-- PDFs spell the same member differently across years — e.g. an ASCII "-"
-- vs a U+2010 hyphen in "Healy-Rae" — so raw member_name also split years),
-- falling back to member_name for the handful of unenriched rows. The grain
-- is now truly (member, house, payment_year); position/band/display-name
-- collapse to the value on the year's most recent payment.
CREATE OR REPLACE VIEW v_payments_yearly_evolution AS
WITH per_member_year AS (
    SELECT
        COALESCE(NULLIF(unique_member_code, ''), member_name) AS member_key,
        payment_year,
        house,
        arg_max(member_name,    date_paid)    AS member_name,
        arg_max(position,       date_paid)    AS position,
        arg_max(taa_band_raw,   date_paid)    AS taa_band_raw,
        arg_max(taa_band_label, date_paid)    AS taa_band_label,
        MAX(COALESCE(unique_member_code, '')) AS unique_member_code,
        MAX(COALESCE(party_name,   ''))       AS party_name,
        MAX(COALESCE(constituency, ''))       AS constituency,
        SUM(amount_num)  AS total_paid,
        COUNT(*)         AS payment_count
    FROM v_payments_base
    GROUP BY member_key, payment_year, house
)
SELECT
    member_name,
    position,
    taa_band_raw,
    taa_band_label,
    payment_year,
    house,
    unique_member_code,
    party_name,
    constituency,
    total_paid,
    payment_count,
    RANK()  OVER (PARTITION BY payment_year, house ORDER BY total_paid DESC)    AS rank_high,
    SUM(total_paid) OVER (PARTITION BY payment_year, house)                     AS year_total_paid,
    COUNT(*) OVER (PARTITION BY payment_year, house)                            AS year_member_count,
    ROUND(
        SUM(total_paid) OVER (PARTITION BY payment_year, house)
        / NULLIF(COUNT(*) OVER (PARTITION BY payment_year, house), 0),
        2
    )                                                                           AS year_avg_per_td,
    -- Member identity is (member_key, house): two names (Flaherty, Joe;
    -- Tully, Pauline) exist in both chambers' payment files.
    SUM(total_paid) OVER (PARTITION BY member_key, house)                       AS member_alltime_total
FROM per_member_year;
