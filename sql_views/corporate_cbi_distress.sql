-- v_corporate_cbi_notice_match — per-notice badge lookup for the Corporate page.
-- v_corporate_cbi_repeat_distress — per-firm aggregate for the "regulated firms
--                                    in repeat distress" panel.
--
-- Source: data/gold/parquet/cbi_xref_corporate_notices.parquet
--   produced by extractors/cbi_registers_extract.py (the corporate xref is
--   the one PROMOTED output — committed gold, run as the `cbi` pipeline chain).
--   The underlying CBI register PDFs are extracted heuristically (see the script
--   docstring), but this xref is an inner-join of v_corporate_notices entity_name
--   against the de-duplicated CBI firm-name index, EXACT normalised match only.
--
-- Why this lives behind sql_views/corporate_*.sql and not its own family:
--   it's a strict subset of the corporate notices grain — every row in
--   v_corporate_cbi_notice_match also exists in v_corporate_notices, joined to
--   the CBI register that authorises the entity. The page uses it for
--   display-only annotation (badge + panel) — no civic claim beyond "this
--   wound-up / receivership entity is/was CBI-authorised under register X".
--
-- Civic frame is honest: we do NOT claim the receiver/liquidator action is
-- itself a regulatory matter. We surface the regulatory provenance of the
-- entity that appears on the notice. Members' Voluntary Liquidation
-- (n_members_vl) is a SOLVENT wind-up — fund-lifecycle, not distress — and
-- the repeat-distress view's HAVING clause is weighted to suppress that case.

-- 1. Per-notice match — used for the on-card / on-detail badge.
--    Keyed on entity_norm (not notice_ref) because some corporate_notices
--    rows lack notice_ref upstream; the page joins to its own display_ref.
CREATE OR REPLACE VIEW v_corporate_cbi_notice_match AS
SELECT
    notice_ref,
    entity_name,
    entity_norm,
    issue_date,
    notice_category,
    notice_subtype,
    registers,
    ref_nos,
    -- Pull the first register/ref for compact badge rendering; the full list
    -- remains available for the detail view.
    COALESCE(registers[1], '')                            AS primary_register,
    COALESCE(ref_nos[1],   '')                            AS primary_ref_no
FROM read_parquet('data/gold/parquet/cbi_xref_corporate_notices.parquet')
WHERE entity_norm IS NOT NULL;

-- 2. Per-firm aggregate — used for the "regulated firms in repeat distress"
--    panel. HAVING gate suppresses ETF / fund-lifecycle noise: require either
--    two genuine-distress notices OR three+ total notices including at least
--    one distress event.
CREATE OR REPLACE VIEW v_corporate_cbi_repeat_distress AS
WITH base AS (
    SELECT
        entity_norm,
        ANY_VALUE(entity_name)                            AS entity_name,
        ANY_VALUE(registers)                              AS registers,
        ANY_VALUE(ref_nos)                                AS ref_nos,
        COALESCE(ANY_VALUE(registers)[1], '')             AS primary_register,
        COALESCE(ANY_VALUE(ref_nos)[1],   '')             AS primary_ref_no,
        COUNT(*)                                                                          AS n_notices_total,
        COUNT(*) FILTER (WHERE notice_subtype = 'receivership')                           AS n_receivership,
        COUNT(*) FILTER (WHERE notice_subtype = 'court_winding_up')                       AS n_court_winding_up,
        COUNT(*) FILTER (WHERE notice_subtype = 'examinership')                           AS n_examinership,
        COUNT(*) FILTER (WHERE notice_subtype = 'scarp_process_adviser')                  AS n_scarp,
        COUNT(*) FILTER (WHERE notice_subtype = 'creditors_voluntary_liquidation')        AS n_creditors_vl,
        COUNT(*) FILTER (WHERE notice_subtype = 'members_voluntary_liquidation')          AS n_members_vl,
        COUNT(*) FILTER (WHERE notice_subtype = 'voluntary_liquidation_unspecified')      AS n_vl_unspec,
        COUNT(*) FILTER (WHERE notice_subtype = 'companies_act_notice')                   AS n_companies_act,
        MIN(issue_date)                                   AS first_notice_date,
        MAX(issue_date)                                   AS last_notice_date
    FROM read_parquet('data/gold/parquet/cbi_xref_corporate_notices.parquet')
    WHERE entity_norm IS NOT NULL
    GROUP BY entity_norm
)
SELECT
    *,
    -- Distress = secured-creditor / court / insolvent / rescue actions.
    n_receivership + n_court_winding_up + n_examinership + n_scarp + n_creditors_vl
                                                                AS n_distress,
    -- Routine = solvent fund lifecycle.
    n_members_vl                                                AS n_routine
FROM base
WHERE
    (n_receivership + n_court_winding_up + n_examinership + n_scarp + n_creditors_vl) >= 2
 OR (n_notices_total >= 3
     AND (n_receivership + n_court_winding_up + n_examinership + n_scarp + n_creditors_vl) >= 1)
ORDER BY n_distress DESC, n_notices_total DESC;
