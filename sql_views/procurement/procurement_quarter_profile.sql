-- v_procurement_quarter_profile — quarterly shape of COMMITTED (purchase-order) lines
-- per publisher, for the year-end-spike seasonality fact. Validated 2026-06-11: Q4
-- carries €2.54bn safe vs a €1.79bn Q1–Q3 average (+41%), and the most Q4-skewed
-- publishers order >50% of their lines in Q4 (Galway County 52.3%, Clare 51.5%).
--
-- ⚠️ SEASONALITY IS A SHAPE FACT, NEVER A MOTIVE: "use-it-or-lose-it" budgeting is one
-- known public-finance explanation, but invoicing cycles, grant schedules, and works
-- seasons are others. Copy describes the shape ("X placed N% of its orders in Q4"),
-- never the reason.
--
-- Grain: one row per publisher × quarter, COMMITTED tier only (SPENT lines record when
-- money moved, which lags ordering and muddies the signal; tiers must not be mixed).
-- pct_of_publisher_lines gives each publisher's own quarterly distribution so small and
-- large publishers compare fairly. Publishers with <100 quartered lines are excluded —
-- a 30-line publisher's "60% in Q4" is noise.
CREATE OR REPLACE VIEW v_procurement_quarter_profile AS
WITH per_quarter AS (
    SELECT
        publisher_id,
        publisher_name,
        quarter,
        COUNT(*)                                          AS n_lines,
        SUM(amount_eur) FILTER (WHERE value_safe_to_sum)  AS committed_safe_eur
    FROM read_parquet('data/gold/parquet/procurement_payments_fact.parquet')
    WHERE public_display
      AND realisation_tier = 'COMMITTED'
      AND quarter IS NOT NULL
    GROUP BY publisher_id, publisher_name, quarter
)
SELECT
    publisher_id,
    publisher_name,
    quarter,
    n_lines,
    committed_safe_eur,
    ROUND(100.0 * n_lines / SUM(n_lines) OVER (PARTITION BY publisher_id), 1)
                                                          AS pct_of_publisher_lines
FROM per_quarter
QUALIFY SUM(n_lines) OVER (PARTITION BY publisher_id) >= 100
ORDER BY publisher_name, quarter;
