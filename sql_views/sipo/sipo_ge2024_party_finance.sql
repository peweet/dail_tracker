-- Combined GE2024 political-finance rollup — one wide row per party, joining the
-- THREE separate SIPO returns so the "Election 2024" page can show a party's full
-- money picture in a single card without doing the join in the Streamlit layer
-- (the logic firewall forbids cross-view JOINs / aggregation in UI code).
--
-- Reads three EXISTING per-party rollups (no new aggregation is invented here):
--   * v_sipo_donations_by_party            (sipo_donations.sql)       — money IN
--   * v_sipo_expenses_by_party             (sipo_expenses_base.sql)   — party national-agent spend ON candidates
--   * v_sipo_candidate_expenses_by_party   (sipo_candidate_expenses.sql) — candidates' OWN expenses statements
--
-- DEPENDENCY ORDER: this file is named to sort AFTER all three base files inside
-- the alphabetical `sql_views/**/sipo_*.sql` glob (candidate < donations < expenses
-- < ge2024), so the three source views already exist when this one registers.
--
-- ⚠ NEVER-SUM-ACROSS-GRAINS: the three money columns are DIFFERENT records at
-- DIFFERENT grains and MUST NOT be added together:
--   * donated_in_eur    = donations a party DECLARED receiving (> EUR 1,500).
--   * agent_spend_eur   = the national agent's per-candidate spend (Part 3). It
--     UNDER-counts parties that book spend centrally; not a total campaign outlay.
--   * candidate_spend_eur = the candidates' own Expenses Statements (incremental
--     OCR coverage — only candidates processed so far). agent_spend and
--     candidate_spend are two views of campaign spend from DIFFERENT returns and
--     OVERLAP — they are not additive.
-- The page surfaces each figure on its own with its own caveat; the only place
-- they meet here is the ORDER BY sort key (display order, never a presented total).
--
-- NULL columns are honest gaps: a party present in one return but not another is
-- shown as "—", never coerced to 0. The party spine UNIONs the non-NULL party
-- labels from each return (the candidate rollup keeps a NULL "unknown party"
-- bucket which is intentionally excluded from the per-party cards).

CREATE OR REPLACE VIEW v_sipo_ge2024_party_finance AS
WITH party_spine AS (
    SELECT party FROM v_sipo_donations_by_party            WHERE party IS NOT NULL
    UNION
    SELECT party FROM v_sipo_expenses_by_party             WHERE party IS NOT NULL
    UNION
    SELECT party FROM v_sipo_candidate_expenses_by_party   WHERE party IS NOT NULL
)
SELECT
    s.party,
    -- money IN
    d.total_value         AS donated_in_eur,
    d.donation_count      AS donation_count,
    d.verify_count        AS donation_verify_count,
    -- party national-agent spend ON candidates (Part 3)
    e.total_expenditure   AS agent_spend_eur,
    e.candidate_count     AS agent_candidate_count,
    e.verify_count        AS agent_verify_count,
    e.excluded_count      AS agent_excluded_count,
    -- candidates' OWN expenses statements (incremental OCR)
    c.total_spend         AS candidate_spend_eur,
    c.candidate_count     AS candidate_count,
    c.verify_count        AS candidate_verify_count
FROM party_spine s
LEFT JOIN v_sipo_donations_by_party          d ON d.party = s.party
LEFT JOIN v_sipo_expenses_by_party           e ON e.party = s.party
LEFT JOIN v_sipo_candidate_expenses_by_party c ON c.party = s.party
-- sort key only — biggest overall election footprint first; NOT a displayed total.
ORDER BY COALESCE(d.total_value, 0)
       + COALESCE(e.total_expenditure, 0)
       + COALESCE(c.total_spend, 0) DESC;
