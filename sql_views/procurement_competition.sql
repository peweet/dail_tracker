-- v_procurement_competition — procurement COMPETITION-QUALITY signals per contracting
-- authority (buyer), derived from TED (EU Official Journal) Irish award notices. The bid-
-- count fields exist only on eForms 2024+ notices (ted_ie_awards.parquet), so this view is
-- the 2024+ window. The single-bidder rate is the EU Single Market Scoreboard's flagship
-- procurement-integrity indicator — a recognised, comparable KPI, novel for Ireland here.
--
-- ⚠️ FACTUAL SIGNAL, NEVER A VERDICT. A single bid is often wholly legitimate (a niche or
-- specialist supplier, bespoke research equipment, genuine urgency). A high single-bid rate
-- is a prompt to LOOK, not evidence of wrongdoing. Any consuming UI must present it as a
-- neutral competition signal with that caveat — no inference (see no-inference posture).
--
-- Grain: one row per buyer. The rate DENOMINATOR is awards that carry a tender count
-- (n_tenders_received IS NOT NULL); awards without a count are excluded from the rate but
-- still appear in n_awards_total, so single_bid_pct is honest about its own coverage.
--
-- buyer_name carries the same TED eForms org-id artefact as winner_name — a trailing
-- "_NNNNN" or " (ID NNNNN)" — so it's stripped here BEFORE grouping (mirrors the
-- regexp_replace in v_procurement_ted_awards) or the same authority would split into
-- several rows. The proper fix is to clean before normalising in the extractor.
CREATE OR REPLACE VIEW v_procurement_competition AS
WITH cleaned AS (
    SELECT
        trim(regexp_replace(regexp_replace(buyer_name, '_[0-9]+$', ''),
                            '\s*\(ID\s*[0-9]+\)$', ''))    AS buyer_name,
        n_tenders_received,
        is_single_bid,
        is_uncompetitive_procedure,
        is_price_only,
        year
    FROM read_parquet('data/silver/parquet/ted_ie_awards.parquet')
    WHERE buyer_name IS NOT NULL
)
SELECT
    buyer_name,
    COUNT(*)                                                AS n_awards_total,
    COUNT(*) FILTER (WHERE n_tenders_received IS NOT NULL)  AS n_awards_with_bidcount,
    COUNT(*) FILTER (WHERE is_single_bid)                   AS n_single_bid,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE is_single_bid)
        / NULLIF(COUNT(*) FILTER (WHERE n_tenders_received IS NOT NULL), 0)
    , 1)                                                    AS single_bid_pct,
    COUNT(*) FILTER (WHERE is_uncompetitive_procedure)      AS n_uncompetitive_procedure,
    COUNT(*) FILTER (WHERE is_price_only)                   AS n_price_only,
    median(n_tenders_received)                              AS median_tenders_received,
    MIN(year)                                               AS first_year,
    MAX(year)                                               AS last_year
FROM cleaned
GROUP BY buyer_name
ORDER BY n_awards_with_bidcount DESC;
