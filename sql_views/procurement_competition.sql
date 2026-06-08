-- v_procurement_competition — procurement COMPETITION-QUALITY per contracting authority
-- (buyer), from TED (EU Official Journal) Irish award notices. Bid counts exist only on
-- eForms 2024+ notices (ted_ie_awards.parquet). The single-bidder rate is the EU Single
-- Market Scoreboard's flagship procurement-integrity indicator — novel for Ireland here.
--
-- ✅ LOT-LEVEL rate (the honest one). Each contract part (lot) is counted ONCE:
-- single_bid_lot_pct = single-bid LOTS / lots-with-a-bid-count. This replaces the old
-- notice-level reading, which flagged a whole multi-lot notice as "single bid" if its
-- least-competitive lot drew one bidder — inflating multi-lot buyers (universities ran
-- bundled research frameworks). Fixing it at lot grain dropped the all-buyer baseline
-- 24.2%->20.4% and e.g. UCD 35.7%->25.6%, UL 65.6%->51.1%. (n_single_bid_lots /
-- n_lots_with_bidcount come from extractors/ted_ireland_extract.competition_fields.)
--
-- ⚠️ FACTUAL SIGNAL, NEVER A VERDICT. A single bid is often wholly legitimate — a niche or
-- specialist supplier, bespoke research equipment, genuine urgency. A high rate is a prompt
-- to LOOK, not evidence of wrongdoing; research-heavy universities legitimately single-source
-- a lot (Galway stays ~74% even at lot level). No inference in any consuming UI, and rank only
-- buyers with a healthy n_lots_with_bidcount (small samples are noisy).
--
-- Grain: one row per buyer. Bid counts are notice-level (repeated across a notice's winner
-- rows), so notices are de-duplicated before the lot counts are summed.
CREATE OR REPLACE VIEW v_procurement_competition AS
WITH notices AS (
    SELECT DISTINCT
        publication_number,
        trim(regexp_replace(regexp_replace(buyer_name, '_[0-9]+$', ''),
                            '\s*\(ID\s*[0-9]+\)$', ''))    AS buyer_name,
        n_lots_with_bidcount,
        n_single_bid_lots,
        is_uncompetitive_procedure,
        is_price_only,
        year
    FROM read_parquet('data/silver/parquet/ted_ie_awards.parquet')
    WHERE buyer_name IS NOT NULL
)
SELECT
    buyer_name,
    COUNT(*)                                                AS n_notices,
    COALESCE(SUM(n_lots_with_bidcount), 0)::BIGINT          AS n_lots_with_bidcount,
    COALESCE(SUM(n_single_bid_lots), 0)::BIGINT             AS n_single_bid_lots,
    ROUND(
        100.0 * SUM(n_single_bid_lots)
        / NULLIF(SUM(n_lots_with_bidcount), 0)
    , 1)                                                    AS single_bid_lot_pct,
    COUNT(*) FILTER (WHERE is_uncompetitive_procedure)      AS n_uncompetitive_notices,
    COUNT(*) FILTER (WHERE is_price_only)                   AS n_price_only_notices,
    MIN(year)                                               AS first_year,
    MAX(year)                                               AS last_year
FROM notices
GROUP BY buyer_name
ORDER BY n_lots_with_bidcount DESC;
