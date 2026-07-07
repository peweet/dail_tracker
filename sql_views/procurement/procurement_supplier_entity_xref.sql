-- v_supplier_entity_xref — the organisation-360 spine (supplier-anchored).
-- Source: data/gold/parquet/supplier_entity_xref.parquet (extractors/entity_xref_build.py,
-- the `entity_xref` pipeline chain, run AFTER procurement / lobbying / corporate / charity gold).
--
-- One row per procurement supplier (keyed on supplier_norm — the same key the company
-- dossier page is entered on, /company?supplier=), carrying its cross-register presence
-- fused on the CANONICAL normalised name (shared/name_norm): CRO identity, lobbying
-- footprint, corporate-notice count, charity status, EPA licence. The extractor did every
-- aggregation and the canonical re-norm; this view only SELECTs (logic-firewall).
--
-- FRAMING (feedback_no_inference_in_app): co-occurrence by ENTITY only — the same
-- organisation appears on several registers. NOT evidence one caused another; there is no
-- key linking a specific lobby/meeting to a specific contract. Exact name / CRO matching
-- UNDERCOUNTS (subsidiary/trading-name variants missed) and short names can collide —
-- counts are floors, not verdicts. Individuals excluded upstream.
CREATE OR REPLACE VIEW v_supplier_entity_xref AS
SELECT *
FROM read_parquet('data/gold/parquet/supplier_entity_xref.parquet');
