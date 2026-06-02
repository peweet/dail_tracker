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
-- The TAA band returned is the member's MOST RECENT band (highest
-- payment_year) so the card pill reflects current state — bands sometimes
-- change between years.
--
-- party_name / constituency are still NULL upstream
-- (payments_member_enrichment.py not yet built, per payments_base.sql).
-- They are projected as empty strings here so downstream SELECTs compile.

-- `house` is carried through so the all-time ranking is per-chamber: Senators
-- rank among Senators (rank_high partitioned by house), and identity is
-- (member_name, house) — two names appear in both chambers' payment files.
CREATE OR REPLACE VIEW v_payments_alltime_ranking AS
WITH per_member AS (
    SELECT
        member_name,
        house,
        -- unique_member_code is consistent per member_name in the yearly view
        -- (NULLIF strips the empty-string COALESCE so MAX picks the real code
        -- when any row has it). MAX over a single value is a no-op.
        MAX(NULLIF(unique_member_code, ''))                      AS unique_member_code,
        SUM(total_paid)                                          AS total_paid_since_2020,
        SUM(payment_count)                                       AS payment_count_since_2020,
        MAX(payment_year)                                        AS latest_year,
        MIN(payment_year)                                        AS earliest_year
    FROM v_payments_yearly_evolution
    WHERE payment_year >= 2020
    GROUP BY member_name, house
),
latest_band AS (
    -- Pick the band label from the member's most-recent year. Joining back
    -- to v_payments_yearly_evolution on (member_name, house, latest_year).
    SELECT DISTINCT ON (y.member_name, y.house)
        y.member_name,
        y.house,
        y.taa_band_label                                         AS taa_band_label_latest,
        y.position                                               AS position_latest
    FROM v_payments_yearly_evolution y
    JOIN per_member pm
      ON y.member_name = pm.member_name
     AND y.house = pm.house
     AND y.payment_year = pm.latest_year
)
SELECT
    pm.member_name,
    pm.house,
    COALESCE(pm.unique_member_code, '')                          AS unique_member_code,
    COALESCE(lb.position_latest, 'Deputy')                       AS position,
    ''                                                           AS party_name,
    ''                                                           AS constituency,
    COALESCE(lb.taa_band_label_latest, '')                       AS taa_band_label,
    pm.total_paid_since_2020,
    pm.payment_count_since_2020,
    pm.earliest_year,
    pm.latest_year,
    RANK() OVER (PARTITION BY pm.house ORDER BY pm.total_paid_since_2020 DESC)  AS rank_high
FROM per_member pm
LEFT JOIN latest_band lb ON pm.member_name = lb.member_name AND pm.house = lb.house
ORDER BY pm.house, rank_high ASC;
