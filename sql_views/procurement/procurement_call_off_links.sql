-- v_procurement_call_off_links — eTenders CALL-OFF awards (drawdowns under a framework
-- or DPS) linked back to their parent agreement where the parent notice exists in the
-- corpus. This is the only place the data lets us connect a framework CEILING to its
-- downstream awards — the "everything is nested" pain made visible. Validated
-- 2026-06-11: 2,277 call-off rows, all carrying a Parent Agreement ID, of which 455
-- (20%) resolve to a parent notice inside the corpus.
--
-- ⚠️ AN UNRESOLVED PARENT IS ITSELF A TRANSPARENCY FACT ("parent agreement not in the
-- published corpus") — render it as such, never hide the row. Where the parent does
-- resolve, a consuming UI can say "call-off under framework X (ceiling €Y)"; the
-- ceiling is the parent's value_kind and is NEVER added to the call-off's own value.
--
-- Grain: one row per call-off award row (award×supplier, same as v_procurement_awards).
-- Parent columns are NULL when unresolved. Parent value/value_kind come from one
-- representative parent row (frameworks repeat the ceiling across supplier rows).
CREATE OR REPLACE VIEW v_procurement_call_off_links AS
WITH parents AS (
    SELECT
        "Tender ID"                              AS parent_tender_id,
        ANY_VALUE("Contracting Authority")       AS parent_authority,
        ANY_VALUE(value_eur)                     AS parent_value_eur,
        ANY_VALUE(value_kind)                    AS parent_value_kind,
        COUNT(DISTINCT supplier_norm)            AS parent_n_suppliers
    FROM read_parquet('data/gold/parquet/procurement_awards.parquet')
    GROUP BY "Tender ID"
)
SELECT
    c."Tender ID"                                AS tender_id,
    c.supplier,
    c.supplier_norm,
    c."Contracting Authority"                    AS contracting_authority,
    TRY_STRPTIME(c."Notice Published Date/Contract Created Date", '%d/%m/%Y')::DATE
                                                 AS award_date,
    c.value_eur,
    c.value_kind,
    c.value_safe_to_sum,
    c."Parent Agreement ID"                      AS parent_agreement_id,
    p.parent_tender_id IS NOT NULL               AS parent_in_corpus,
    p.parent_authority,
    p.parent_value_eur,
    p.parent_value_kind,
    p.parent_n_suppliers
FROM read_parquet('data/gold/parquet/procurement_awards.parquet') c
LEFT JOIN parents p ON c."Parent Agreement ID" = p.parent_tender_id
WHERE c.is_call_off
ORDER BY award_date DESC NULLS LAST;
