-- v_procurement_bid_signal — EXPERIMENTAL "Should I bid?" signal panel, per CPV trade.
-- Source: data/gold/parquet/procurement_awards.parquet. Grain: one row per 4-digit CPV
-- trade group (substr of "Main Cpv Code"), the level at which award samples are dense
-- enough to read (full 8-digit codes are too sparse — often <10 valued awards each).
--
-- WHY THIS EXISTS (doc trail: this is the "signals, not answers" outcome of the
-- pricing-by-comparable investigation). The investigation proved you CANNOT price a job
-- from this data: within every construction trade the contract-award interquartile spread
-- is 4.5x–15x, so a "comparable" only tells you a job sits somewhere in a wide band, and
-- the headline value field mixes framework CEILINGS (median often 14x–79x the real award)
-- with actual awards. So this view does NOT produce a price. It produces FACTS a bidder can
-- reason from themselves — exactly the no-inference rule:
--   * award band (p25 / median / p75) over sum-safe contract awards ONLY — shown as a band,
--     never a point estimate, so it can't be misread as a quote;
--   * framework/DPS ceilings counted and median'd SEPARATELY (never mixed into the band);
--   * competition: median bids + single-bid rate (the EU Single Market Scoreboard signal —
--     "what are my odds?", the question a bidder actually has);
--   * SME winnability: share of awards (with SME data) that an SME actually won.
-- Every signal carries its own n so a thin sample is visible, not hidden. A single bid is
-- often wholly legitimate (niche/specialist/urgent) — a high rate is a prompt to look, never
-- a verdict. No metric leaves this layer; the consuming page renders, it does not compute.
--
-- value_kind recap (from the awards gold): contract_award_value <-> value_safe_to_sum=true
-- (a real per-contract award); framework_or_dps_ceiling <-> is_framework_or_dps=true (an
-- agreement ceiling, NOT a job price). The two are never summed or averaged together.
CREATE OR REPLACE VIEW v_procurement_bid_signal AS
WITH base AS (
    SELECT
        substr("Main Cpv Code", 1, 4)                       AS trade_code,
        "Main Cpv Code Description"                          AS cpv_description,
        value_eur,
        value_safe_to_sum,
        is_framework_or_dps,
        TRY_CAST("No of Bids Received" AS INTEGER)           AS n_bids,
        TRY_CAST("No of Awarded SMEs" AS INTEGER)            AS n_awarded_smes,
        TRY_CAST(substr("Notice Published Date/Contract Created Date", 7, 4) AS INTEGER) AS award_year
    FROM read_parquet('data/gold/parquet/procurement_awards.parquet')
    -- The eTenders extractor writes the literal STRING 'NULL' for a missing CPV (~71% of
    -- rows); exclude it so the signal covers only awards carrying a real CPV (honest gap).
    WHERE "Main Cpv Code" IS NOT NULL
      AND "Main Cpv Code" NOT IN ('', 'NULL')
)
SELECT
    trade_code,
    substr(trade_code, 1, 2)                                                                  AS sector_code,
    -- Sector = CPV 2-digit division, labelled from the canonical CPV_DIV map used across the
    -- TED extractors (extractors/ted_ireland_extract.py) so sector names match the rest of the
    -- app. Anything outside the common divisions falls to 'Other/Unknown' — an honest bucket.
    CASE substr(trade_code, 1, 2)
        WHEN '45' THEN 'Construction'              WHEN '71' THEN 'Architecture/Engineering'
        WHEN '79' THEN 'Business/Consulting'       WHEN '72' THEN 'IT services'
        WHEN '85' THEN 'Health/Social'             WHEN '80' THEN 'Education'
        WHEN '90' THEN 'Environment/Waste'         WHEN '50' THEN 'Repair/Maintenance'
        WHEN '48' THEN 'Software'                  WHEN '33' THEN 'Medical equipment'
        WHEN '34' THEN 'Transport equipment'       WHEN '09' THEN 'Energy/Fuel'
        WHEN '73' THEN 'R&D'                       WHEN '55' THEN 'Hotel/Catering'
        WHEN '60' THEN 'Transport services'        WHEN '92' THEN 'Recreation/Culture'
        WHEN '30' THEN 'Office/IT equipment'       WHEN '98' THEN 'Other services'
        WHEN '70' THEN 'Real estate'               WHEN '66' THEN 'Financial/Insurance'
        ELSE 'Other/Unknown'
    END                                                                                       AS sector_label,
    mode(cpv_description)                                                                     AS trade_label,
    COUNT(*)                                                                                  AS n_awards_total,

    -- ── Comparable band: sum-safe CONTRACT awards only (ceilings excluded by the gate) ──
    COUNT(*) FILTER (WHERE value_safe_to_sum AND value_eur > 0)                               AS n_contract_awards,
    quantile_cont(value_eur, 0.25) FILTER (WHERE value_safe_to_sum AND value_eur > 0)         AS award_p25_eur,
    median(value_eur)              FILTER (WHERE value_safe_to_sum AND value_eur > 0)          AS award_median_eur,
    quantile_cont(value_eur, 0.75) FILTER (WHERE value_safe_to_sum AND value_eur > 0)         AS award_p75_eur,
    COUNT(*) FILTER (WHERE value_safe_to_sum AND value_eur > 0 AND award_year >= 2022)        AS n_recent_contract_awards,

    -- ── Framework / DPS ceiling context (shown SEPARATELY, never folded into the band) ──
    COUNT(*) FILTER (WHERE is_framework_or_dps AND value_eur > 0)                             AS n_framework_ceilings,
    median(value_eur) FILTER (WHERE is_framework_or_dps AND value_eur > 0)                    AS ceiling_median_eur,

    -- ── Competition signal (how many bidders typically show up) ──
    COUNT(*) FILTER (WHERE n_bids IS NOT NULL)                                                AS n_with_bid_data,
    median(n_bids) FILTER (WHERE n_bids IS NOT NULL)                                          AS median_bids,
    COUNT(*) FILTER (WHERE n_bids = 1)                                                        AS n_single_bid,
    ROUND(100.0 * COUNT(*) FILTER (WHERE n_bids = 1)
          / NULLIF(COUNT(*) FILTER (WHERE n_bids IS NOT NULL), 0), 1)                         AS single_bid_pct,

    -- ── SME winnability (does a small firm actually win this category's work?) ──
    COUNT(*) FILTER (WHERE n_awarded_smes IS NOT NULL)                                        AS n_with_sme_data,
    COUNT(*) FILTER (WHERE n_awarded_smes > 0)                                                AS n_sme_won,
    ROUND(100.0 * COUNT(*) FILTER (WHERE n_awarded_smes > 0)
          / NULLIF(COUNT(*) FILTER (WHERE n_awarded_smes IS NOT NULL), 0), 1)                 AS sme_win_pct
FROM base
GROUP BY trade_code
ORDER BY n_awards_total DESC;
