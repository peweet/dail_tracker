-- v_payments_alltime_ranking — all-time PSA ranking since 2020
--
-- Audit fix (2026-05-26):  the page previously read
-- data/gold/parquet/current_td_payment_rankings.parquet directly via Polars
-- + computed .sum() / .n_unique() in Streamlit. Two problems:
--   1. Parquet schema diverged from what the page expects — `member_name`
--      no longer exists; `identifier` holds Oireachtas slugs like
--      `Michael-Collins.D.2016-10-03`. Every Rankings card rendered "—".
--   2. Reading parquet from Streamlit + doing aggregation there both
--      violate the page contract (no read_parquet, SUM not in allowed
--      aggregates).
-- This view replaces both. The page does retrieval-only SELECT against it.
--
-- The TAA band / position returned are the member's MOST RECENT (highest
-- payment_year) — bands sometimes change between years.
--
-- party_name / constituency are still NULL upstream
-- (payments_member_enrichment.py not yet built, per payments_base.sql).
-- They are projected as empty strings here so downstream SELECTs compile.

-- `house` is carried through so the all-time ranking is per-chamber: Senators
-- rank among Senators (rank_high partitioned by house).
--
-- Grain fix (2026-06-11, mirrors v_payments_yearly_evolution): member identity
-- is unique_member_code when enriched, falling back to member_name — the source
-- PDFs spell the same member differently across years (ASCII "-" vs U+2010
-- hyphen in "Healy-Rae"), so grouping by raw member_name split one member into
-- two all-time entries. Display name/band/position collapse to the latest
-- year's values via arg_max, which also retires the old latest_band CTE.
CREATE OR REPLACE VIEW v_payments_alltime_ranking AS
WITH per_member AS (
    SELECT
        COALESCE(NULLIF(unique_member_code, ''), member_name)    AS member_key,
        house,
        arg_max(member_name, payment_year)                       AS member_name,
        MAX(NULLIF(unique_member_code, ''))                      AS unique_member_code,
        arg_max(position, payment_year)                          AS position_latest,
        arg_max(taa_band_label, payment_year)                    AS taa_band_label_latest,
        SUM(total_paid)                                          AS total_paid_since_2020,
        SUM(payment_count)                                       AS payment_count_since_2020,
        MAX(payment_year)                                        AS latest_year,
        MIN(payment_year)                                        AS earliest_year
    FROM v_payments_yearly_evolution
    WHERE payment_year >= 2020
    GROUP BY member_key, house
)
SELECT
    pm.member_name,
    pm.house,
    COALESCE(pm.unique_member_code, '')                          AS unique_member_code,
    COALESCE(pm.position_latest, 'Deputy')                       AS position,
    ''                                                           AS party_name,
    ''                                                           AS constituency,
    COALESCE(pm.taa_band_label_latest, '')                       AS taa_band_label,
    pm.total_paid_since_2020,
    pm.payment_count_since_2020,
    pm.earliest_year,
    pm.latest_year,
    RANK() OVER (PARTITION BY pm.house ORDER BY pm.total_paid_since_2020 DESC)  AS rank_high
FROM per_member pm
ORDER BY pm.house, rank_high ASC;
