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
--
-- PARTY-ROLE / PARSE-QUALITY GATES (2026-07-13, MCP sweep defect 1). Three
-- data-anchored filters, each keyed on what the notice ITSELF says — no
-- hand-blacklisting of names:
--   • FRAGMENT ENTITY NAMES — entity_name that is a mid-clause fragment of the
--     indenture parties ("Limited and Allied Irish Banks … and") is an
--     extraction artifact: the named string is the MORTGAGEE position of a
--     "made between X and [bank]" clause, not the distressed company. Excluded.
--   • MISFILED NON-CORPORATE NOTICES — rows whose text opens as a foreshore-
--     licence determination list are not corporate notices at all (a fund name
--     matched inside the licence table). Excluded.
--   • TRUSTEE-CAPACITY RECEIVERSHIPS — where the notice states the entity is
--     named "(in its capacity as (a) trustee of …)", the receivership attaches
--     to TRUST assets, not the trustee company's own solvency. These rows are
--     kept but flagged (is_trustee_capacity) and EXCLUDED from n_distress.
--   RESIDUAL (gated): many receivership notices naming professional-trustee
--   entities store only the headline text, so the charged party's role cannot
--   be recovered at view level — resolving those needs party-role
--   re-extraction upstream (iris/corporate enrichment, pipeline-owned).

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
    COALESCE(ref_nos[1],   '')                            AS primary_ref_no,
    regexp_matches(raw_text, '(?i)capacity as (a )?trustee') AS is_trustee_capacity
FROM read_parquet('data/gold/parquet/cbi_xref_corporate_notices.parquet')
WHERE entity_norm IS NOT NULL
  -- fragment-of-clause entity names (extraction artifact — wrong party)
  AND NOT regexp_matches(entity_name, '^(and|Limited and|limited and) ')
  AND NOT regexp_matches(entity_name, ' and[,.]?$')
  -- misfiled non-corporate notices (foreshore-licence determination lists)
  AND NOT regexp_matches(substr(raw_text, 1, 300), '(?i)foreshore licen');

-- 2. Per-firm aggregate — used for the "regulated firms in repeat distress"
--    panel. HAVING gate suppresses ETF / fund-lifecycle noise: require either
--    two genuine-distress notices OR three+ total notices including at least
--    one distress event.
CREATE OR REPLACE VIEW v_corporate_cbi_repeat_distress AS
WITH gated AS (
    -- Same parse-quality gates as v_corporate_cbi_notice_match (see header):
    -- fragment entity names + misfiled non-corporate notices are excluded;
    -- trustee-capacity receiverships are flagged so they count as notices but
    -- NOT as distress events of the named trustee company.
    SELECT
        *,
        regexp_matches(raw_text, '(?i)capacity as (a )?trustee') AS is_trustee_capacity
    FROM read_parquet('data/gold/parquet/cbi_xref_corporate_notices.parquet')
    WHERE entity_norm IS NOT NULL
      AND NOT regexp_matches(entity_name, '^(and|Limited and|limited and) ')
      AND NOT regexp_matches(entity_name, ' and[,.]?$')
      AND NOT regexp_matches(substr(raw_text, 1, 300), '(?i)foreshore licen')
),
base AS (
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
        -- Notices where the entity is named only as trustee of a trust — the
        -- receivership attaches to trust assets, per the notice's own wording.
        COUNT(*) FILTER (WHERE is_trustee_capacity)                                       AS n_trustee_capacity,
        -- Distress = secured-creditor / court / insolvent / rescue actions,
        -- excluding trustee-capacity rows (not the named entity's solvency).
        COUNT(*) FILTER (
            WHERE notice_subtype IN ('receivership', 'court_winding_up', 'examinership',
                                     'scarp_process_adviser', 'creditors_voluntary_liquidation')
              AND NOT is_trustee_capacity
        )                                                                                 AS n_distress,
        MIN(issue_date)                                   AS first_notice_date,
        MAX(issue_date)                                   AS last_notice_date
    FROM gated
    GROUP BY entity_norm
)
SELECT
    *,
    -- Routine = solvent fund lifecycle.
    n_members_vl                                                AS n_routine
FROM base
WHERE
    n_distress >= 2
 OR (n_notices_total >= 3 AND n_distress >= 1)
ORDER BY n_distress DESC, n_notices_total DESC;
