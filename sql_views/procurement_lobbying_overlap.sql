-- v_procurement_lobbying_overlap — companies that appear on BOTH the procurement
-- and lobbying registers. Source: data/gold/parquet/procurement_lobbying_overlap.parquet
-- (procurement_lobbying chain, pipeline_sandbox/procurement_lobbying_xref.py).
--
-- One row per matched lobbying entity (registrant OR client). lobby_name is the raw
-- lobbying display name; supplier_norm/supplier the procurement side. This is the
-- PROCUREMENT-side view (the future Procurement page); the Lobbying page reads its
-- own v_lobbying_org_procurement keyed on lobbyist_name.
--
-- FRAMING (feedback_no_inference_in_app): co-occurrence by ENTITY only. A company
-- appears on both registers. NOT evidence lobbying influenced any award — there is
-- no key linking a specific lobby to a specific contract. Display as neutral
-- disclosure ("won N contracts • filed M lobbying returns"), never causally.
CREATE OR REPLACE VIEW v_procurement_lobbying_overlap AS
SELECT
    lobby_name,
    lobby_side,
    supplier,
    supplier_norm,
    n_lobby_returns,
    n_award_rows,
    n_authorities,
    awarded_value_safe_eur
FROM read_parquet('data/gold/parquet/procurement_lobbying_overlap.parquet')
ORDER BY n_award_rows DESC, n_lobby_returns DESC;
