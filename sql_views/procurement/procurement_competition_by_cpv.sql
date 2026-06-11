-- v_procurement_competition_by_cpv — single-bidder rate per CPV DIVISION (market),
-- from TED Irish award notices. Sibling of v_procurement_competition (per buyer): the
-- validated spread (2026-06-11) runs Hotel/Catering 33.6% → Construction 9.4% against a
-- 17.5% national baseline, i.e. competition health is a property of the MARKET as much
-- as the buyer. Bid counts exist only on eForms 2024+ notices, so this is the 2024+
-- window — same as the per-buyer view.
--
-- ⚠️ FACTUAL SIGNAL, NEVER A VERDICT (same posture as v_procurement_competition): a
-- single bid is often wholly legitimate. High rate = a prompt to look, not evidence.
--
-- Grain: one row per cpv_division. Rate DENOMINATOR is awards carrying a tender count
-- (n_tenders_received IS NOT NULL); n_awards_total sits alongside so the rate is honest
-- about its own coverage. The 'Other/Unknown' division is real corpus mass (~1,750
-- awards with bid counts) and is kept — a consuming UI may list it last.
CREATE OR REPLACE VIEW v_procurement_competition_by_cpv AS
SELECT
    cpv_division,
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
    COUNT(DISTINCT buyer_name)                              AS n_buyers,
    MIN(year)                                               AS first_year,
    MAX(year)                                               AS last_year
FROM read_parquet('data/silver/parquet/ted_ie_awards.parquet')
WHERE cpv_division IS NOT NULL
GROUP BY cpv_division
ORDER BY single_bid_pct DESC NULLS LAST;
