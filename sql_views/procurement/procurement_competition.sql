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
-- Grain: one row per buyer. THE RATE IS LOT-LEVEL: single-bid lots / lots-with-a-bid-count,
-- each contract part counted once. The notice-level alternative (is_single_bid = the
-- notice's LEAST-competitive lot drew one bid) over-states single-bid for multi-lot
-- notices and is not used here — this matches dail_tracker_core.queries.procurement,
-- which selects these exact columns.
--
-- ⚠️ NOTICE DEDUP: the silver is notice×winner grain (a multi-winner notice repeats its
-- lot counts on every winner row), so rows are deduplicated to one per publication_number
-- BEFORE summing — otherwise multi-winner frameworks double-count their lots.
--
-- buyer_name carries the same TED eForms org-id artefact as winner_name — a trailing
-- "_NNNNN" or " (ID NNNNN)" — so it's stripped here BEFORE grouping (mirrors the
-- regexp_replace in v_procurement_ted_awards) or the same authority would split into
-- several rows. The proper fix is to clean before normalising in the extractor.
CREATE OR REPLACE VIEW v_procurement_competition AS
WITH per_notice AS (
    SELECT
        publication_number,
        ANY_VALUE(trim(regexp_replace(regexp_replace(buyer_name, '_[0-9]+$', ''),
                                      '\s*\(ID\s*[0-9]+\)$', '')))  AS buyer_name,
        ANY_VALUE(n_lots_with_bidcount)                             AS n_lots_with_bidcount,
        ANY_VALUE(n_single_bid_lots)                                AS n_single_bid_lots,
        bool_or(is_uncompetitive_procedure)                         AS is_uncompetitive_procedure,
        bool_or(is_price_only)                                      AS is_price_only,
        ANY_VALUE(year)                                             AS year
    FROM read_parquet('data/silver/parquet/ted_ie_awards.parquet')
    WHERE buyer_name IS NOT NULL
    GROUP BY publication_number
)
SELECT
    buyer_name,
    COUNT(*)                                                AS n_notices,
    COALESCE(SUM(n_lots_with_bidcount), 0)                  AS n_lots_with_bidcount,
    COALESCE(SUM(n_single_bid_lots), 0)                     AS n_single_bid_lots,
    ROUND(
        100.0 * SUM(n_single_bid_lots)
        / NULLIF(SUM(n_lots_with_bidcount), 0)
    , 1)                                                    AS single_bid_lot_pct,
    COUNT(*) FILTER (WHERE is_uncompetitive_procedure)      AS n_uncompetitive_notices,
    COUNT(*) FILTER (WHERE is_price_only)                   AS n_price_only_notices,
    MIN(year)                                               AS first_year,
    MAX(year)                                               AS last_year
FROM per_notice
GROUP BY buyer_name
ORDER BY n_lots_with_bidcount DESC;
