-- v_procurement_supplier_single_bid — per-supplier competition context for the supplier
-- profile: of the TED award notices this supplier won OUTRIGHT (single-winner notices),
-- how many contract lots reported a bid count, and how many of those drew exactly one
-- tender. Lets a profile render "won N of its M bid-counted lots unopposed" next to the
-- national lot-level baseline from v_procurement_competition_by_cpv.
--
-- ⚠️ FACTUAL SIGNAL, NEVER A VERDICT (no-inference posture): single-bid wins are often
-- wholly legitimate — niche specialism, genuine urgency. Prompt to look, not evidence.
--
-- ⚠️ SINGLE-WINNER NOTICES ONLY: lot bid-counts live at NOTICE level, so on a multi-
-- winner notice they cannot be attributed to any one winner. Those notices are excluded
-- from the rate and counted in n_multi_winner_notices_excluded so the rate is honest
-- about its own coverage. eForms 2024+ window, same as the other competition views.
--
-- Grain: one row per winner_join_norm (company-class) — the SAME join key the page's
-- eTenders supplier_norm matches against (mirrors v_procurement_ted_winner_history:
-- winner_name_norm with the eForms org-id digits stripped). cro_company_num carried
-- for CRO-keyed joins (v_procurement_entity_chain).
CREATE OR REPLACE VIEW v_procurement_supplier_single_bid AS
WITH cleaned AS (
    SELECT
        regexp_replace(winner_name_norm, ' [0-9]+$', '')          AS winner_join_norm,
        trim(regexp_replace(regexp_replace(winner_name, '_[0-9]+$', ''),
                            '\s*\(ID\s*[0-9]+\)$', ''))           AS winner_name,
        cro_company_num,
        n_winners,
        n_lots_with_bidcount,
        n_single_bid_lots,
        year
    FROM read_parquet('data/silver/parquet/ted_ie_awards.parquet')
    WHERE winner_name_norm IS NOT NULL
      AND supplier_class = 'company'
)
SELECT
    winner_join_norm,
    ANY_VALUE(winner_name)                                   AS winner_name,
    ANY_VALUE(cro_company_num)                               AS cro_company_num,
    COUNT(*) FILTER (WHERE n_winners = 1)                    AS n_sole_winner_notices,
    COUNT(*) FILTER (WHERE n_winners > 1)                    AS n_multi_winner_notices_excluded,
    COALESCE(SUM(n_lots_with_bidcount) FILTER (WHERE n_winners = 1), 0)
                                                             AS n_lots_with_bidcount,
    COALESCE(SUM(n_single_bid_lots) FILTER (WHERE n_winners = 1), 0)
                                                             AS n_single_bid_lots,
    ROUND(
        100.0 * SUM(n_single_bid_lots) FILTER (WHERE n_winners = 1)
        / NULLIF(SUM(n_lots_with_bidcount) FILTER (WHERE n_winners = 1), 0)
    , 1)                                                     AS single_bid_lot_pct,
    MIN(year)                                                AS first_year,
    MAX(year)                                                AS last_year
FROM cleaned
GROUP BY winner_join_norm
ORDER BY n_lots_with_bidcount DESC;
