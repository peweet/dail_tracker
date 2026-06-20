-- v_ministerial_diary_company_influence — the ACCESS x MONEY cross-reference: companies that
-- appear in ministers' published diaries, joined to the public-money registers (contracts won +
-- payments received) and their lobbying-register footprint. One row per diary company.
-- Source: data/gold/parquet/diary_company_influence.parquet (built by
--   extractors/diary_company_influence.py — the name-fold match + FP guards live THERE, this
--   view is a thin read so the logic stays in the vetted pipeline).
--
-- READ HONESTLY (co-occurrence, never causation — surfaced wherever shown): a diary meeting is
-- ACCESS, not a lobbying return and not proof it caused a contract. `matched_supplier` is kept so
-- a reader can verify the name match; awards/payment € carry the procurement layer's own caveats;
-- diaries are self-curated and quarterly-in-arrears. State/semi-state bodies are excluded upstream.
CREATE OR REPLACE VIEW v_ministerial_diary_company_influence AS
SELECT
    organisation,
    sector,
    meetings,
    ministers_met,
    ministers_lobbied_and_met,
    total_lobbying_returns,
    corroborated,
    n_awards,
    awards_eur,
    paid_eur,
    won_public_money,
    matched_supplier,
    first_meeting,
    last_meeting
FROM read_parquet('data/gold/parquet/diary_company_influence.parquet')
ORDER BY awards_eur DESC, paid_eur DESC, meetings DESC;
