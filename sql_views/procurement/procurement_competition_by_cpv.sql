-- v_procurement_competition_by_cpv — single-bidder rate per CPV DIVISION (market),
-- from TED Irish award notices. Sibling of v_procurement_competition (per buyer): the
-- spread across markets is the story — competition health is a property of the MARKET
-- as much as the buyer. Bid counts exist only on eForms 2024+ notices, so this is the
-- 2024+ window — same as the per-buyer view.
--
-- ⚠️ FACTUAL SIGNAL, NEVER A VERDICT (same posture as v_procurement_competition): a
-- single bid is often wholly legitimate. High rate = a prompt to look, not evidence.
--
-- Grain: one row per cpv_division. THE RATE IS LOT-LEVEL (single-bid lots / lots with a
-- bid count, each contract part counted once) — the notice-level min-across-lots metric
-- over-states single-bid for multi-lot notices (measured 2026-06-11: IT services 46.7%
-- notice-level vs 25.3% lot-level). Rows dedupe to one per publication_number before
-- summing, or multi-winner notices double-count their lots. The 'Other/Unknown'
-- division is real corpus mass and is kept — a consuming UI may list it last.
CREATE OR REPLACE VIEW v_procurement_competition_by_cpv AS
WITH per_notice AS (
    SELECT
        publication_number,
        ANY_VALUE(cpv_division)                  AS cpv_division,
        ANY_VALUE(buyer_name)                    AS buyer_name,
        ANY_VALUE(n_lots_with_bidcount)          AS n_lots_with_bidcount,
        ANY_VALUE(n_single_bid_lots)             AS n_single_bid_lots,
        bool_or(is_uncompetitive_procedure)      AS is_uncompetitive_procedure,
        bool_or(is_price_only)                   AS is_price_only,
        ANY_VALUE(year)                          AS year
    FROM read_parquet('data/silver/parquet/ted_ie_awards.parquet')
    WHERE cpv_division IS NOT NULL
    GROUP BY publication_number
)
SELECT
    cpv_division,
    COUNT(*)                                                AS n_notices,
    COALESCE(SUM(n_lots_with_bidcount), 0)                  AS n_lots_with_bidcount,
    COALESCE(SUM(n_single_bid_lots), 0)                     AS n_single_bid_lots,
    ROUND(
        100.0 * SUM(n_single_bid_lots)
        / NULLIF(SUM(n_lots_with_bidcount), 0)
    , 1)                                                    AS single_bid_lot_pct,
    COUNT(*) FILTER (WHERE is_uncompetitive_procedure)      AS n_uncompetitive_notices,
    COUNT(*) FILTER (WHERE is_price_only)                   AS n_price_only_notices,
    COUNT(DISTINCT buyer_name)                              AS n_buyers,
    MIN(year)                                               AS first_year,
    MAX(year)                                               AS last_year
FROM per_notice
GROUP BY cpv_division
ORDER BY single_bid_lot_pct DESC NULLS LAST;
