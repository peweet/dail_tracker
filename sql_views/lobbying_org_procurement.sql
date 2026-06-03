-- v_lobbying_org_procurement — enriches a lobbying ORGANISATION with its state
-- procurement footprint: registrants on lobbying.ie that ALSO won public contracts.
--
-- Source: data/gold/parquet/procurement_lobbying_overlap.parquet
--   (procurement_lobbying chain, pipeline_sandbox/procurement_lobbying_xref.py —
--   an exact normalised-name join of eTenders suppliers to lobbying registrants).
--
-- Keyed on lobbyist_name so the Lobbying page joins it to v_lobbying_org_index on
-- the org's display name. Only the 'registrant' side is exposed here — the org
-- profile is about the registrant entity, so "this lobbying org also supplies the
-- State" is the registrant match (client-side matches are a different relationship,
-- surfaced on the Procurement side).
--
-- FRAMING (feedback_no_inference_in_app): co-occurrence by ENTITY only. The org
-- appears on both registers. NOT evidence lobbying influenced any contract. n_awards
-- is the trustworthy count; awarded_value_safe_eur is awarded value, NOT spend.
CREATE OR REPLACE VIEW v_lobbying_org_procurement AS
SELECT
    lobby_name                          AS lobbyist_name,
    mode(supplier)                      AS supplier,
    SUM(n_award_rows)                   AS n_awards,
    MAX(n_authorities)                  AS n_authorities,
    SUM(awarded_value_safe_eur)         AS awarded_value_safe_eur
FROM read_parquet('data/gold/parquet/procurement_lobbying_overlap.parquet')
WHERE lobby_side = 'registrant'
GROUP BY lobby_name;
