-- v_procurement_council_summary — one row per council that PUBLISHES ANY of the three
-- council-money lanes, for the "Your council" index tab (utility/pages_code/procurement.py,
-- _render_councils) and as the reachability spine for the per-council dossier.
--
-- Reads the three council facts DIRECTLY from parquet (not via the v_procurement_* views) on
-- purpose: the procurement_*.sql glob loads alphabetically with swallow_errors=True, and this
-- file (procurement_council_*) sorts AFTER procurement_afs_* but BEFORE procurement_payments.sql,
-- so a dependency on v_procurement_payments would register against a not-yet-existing view and be
-- silently dropped (see memory feedback_sql_view_dependency_order). Reading parquet sidesteps it.
--
-- WHY A UNION (changed 2026-06-26): the index used to read ONLY the payments fact, so the four
-- councils that publish audited accounts but NOT a purchase-order list — Dublin City (the largest
-- LA in the State, €668m capital), Dún Laoghaire-Rathdown, Louth, Tipperary — never appeared in the
-- directory AND were unreachable in the dossier (the profile bailed when a council had no payment
-- row). The three lanes have DIFFERENT council coverage, so the directory must be the UNION of all
-- publishers, with per-lane availability flags (has_paying / has_running / has_building) the page
-- uses to render the right pills and to explain an absent lane rather than render it blank.
--
-- ⚠️ NEVER-SUM. ordered_safe_eur (realisation_tier='COMMITTED' / purchase orders, "ordered €X")
-- and paid_safe_eur ('SPENT' / actual payments, "paid €X") are DIFFERENT lifecycle stages of
-- public money — surfaced as two columns and NEVER added. In this corpus only Meath and Offaly
-- publish 'SPENT'; the other payers publish 'COMMITTED' only. Only value_safe_to_sum rows
-- contribute to either total. The running/building year spans come from the audited-AFS facts
-- (a SIBLING budget grain) and carry NO euro here — those live in the per-lane views.
--
-- province / province_order are static Irish geography (the 4 historic provinces) used to group
-- the index North->South (1=Ulster .. 4=Munster). Dublin LAs sit in Leinster (Dublin is in
-- Leinster). This is fixed geography, not data inference.
CREATE OR REPLACE VIEW v_procurement_council_summary AS
WITH paying AS (
    SELECT
        publisher_name AS council,
        COUNT(DISTINCT supplier_normalised)                                                  AS n_suppliers,
        MIN(year)::INT                                                                       AS min_year,
        MAX(year)::INT                                                                       AS max_year,
        COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum AND realisation_tier = 'COMMITTED'), 0)
                                                                                             AS ordered_safe_eur,
        COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum AND realisation_tier = 'SPENT'), 0)
                                                                                             AS paid_safe_eur,
        COUNT(*) FILTER (WHERE realisation_tier = 'COMMITTED')                               AS n_ordered,
        COUNT(*) FILTER (WHERE realisation_tier = 'SPENT')                                   AS n_paid
    FROM read_parquet('data/gold/parquet/procurement_payments_fact.parquet')
    WHERE publisher_type = 'local_authority'
      AND extraction_status = 'extracted'
      AND supplier_normalised IS NOT NULL
      AND length(supplier_normalised) >= 2
    GROUP BY publisher_name
),
running AS (  -- audited REVENUE account (la_afs_divisions) — the RUNNING lane
    SELECT council, MIN(year)::INT AS running_min_year, MAX(year)::INT AS running_max_year
    FROM read_parquet('data/silver/parquet/la_afs_divisions.parquet')
    GROUP BY council
),
building AS (  -- audited CAPITAL account (la_afs_capital_divisions) — the BUILDING lane
    SELECT council, MIN(year)::INT AS building_min_year, MAX(year)::INT AS building_max_year
    FROM read_parquet('data/silver/parquet/la_afs_capital_divisions.parquet')
    GROUP BY council
),
councils AS (
    SELECT council FROM paying
    UNION SELECT council FROM running
    UNION SELECT council FROM building
)
SELECT
    c.council,
    CASE
        WHEN c.council IN ('Donegal', 'Monaghan')                                            THEN 'Ulster'
        WHEN c.council IN ('Galway City', 'Galway County', 'Leitrim', 'Mayo', 'Sligo')       THEN 'Connacht'
        WHEN c.council IN ('Clare', 'Cork City', 'Cork County', 'Limerick', 'Tipperary',
                           'Waterford')                                                       THEN 'Munster'
        ELSE 'Leinster'  -- Dublin City, Dún Laoghaire-Rathdown, Louth, Kildare, Kilkenny, Longford,
                         -- Meath, Offaly, South Dublin, Westmeath, Wexford, Laois, Fingal
    END AS province,
    CASE
        WHEN c.council IN ('Donegal', 'Monaghan')                                            THEN 1
        WHEN c.council IN ('Galway City', 'Galway County', 'Leitrim', 'Mayo', 'Sligo')       THEN 2
        WHEN c.council IN ('Clare', 'Cork City', 'Cork County', 'Limerick', 'Tipperary',
                           'Waterford')                                                       THEN 4
        ELSE 3  -- Leinster
    END AS province_order,
    p.n_suppliers,
    p.min_year,
    p.max_year,
    COALESCE(p.ordered_safe_eur, 0)                                                          AS ordered_safe_eur,
    COALESCE(p.paid_safe_eur, 0)                                                             AS paid_safe_eur,
    COALESCE(p.n_ordered, 0)                                                                 AS n_ordered,
    COALESCE(p.n_paid, 0)                                                                    AS n_paid,
    (p.council IS NOT NULL)                                                                  AS has_paying,
    (r.council IS NOT NULL)                                                                  AS has_running,
    (b.council IS NOT NULL)                                                                  AS has_building,
    r.running_min_year,
    r.running_max_year,
    b.building_min_year,
    b.building_max_year
FROM councils c
LEFT JOIN paying p   ON p.council = c.council
LEFT JOIN running r  ON r.council = c.council
LEFT JOIN building b ON b.council = c.council
-- payers first within each province (they carry the headline euro), then audited-accounts-only
-- councils. GREATEST() here is a SORT key only — never a sum of the two never-summed lifecycle tiers.
ORDER BY province_order,
         (p.council IS NOT NULL) DESC,
         GREATEST(COALESCE(p.ordered_safe_eur, 0), COALESCE(p.paid_safe_eur, 0)) DESC,
         c.council;
