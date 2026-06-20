-- v_la_planning_overturn — per-council planning quality signal: how often An Bord
-- Pleanála (ABP) OVERTURNED the council's own planning decision on appeal. A high
-- overturn rate means the executive's decisions are frequently found wrong by the
-- independent appeals body. Planning permission is an EXECUTIVE function (the
-- planner's delegated order), so this is an accountability signal for the council's
-- administration, i.e. the Chief Executive's office — not the elected councillors.
--
-- Source: data/silver/parquet/planning_appeal_outcomes.parquet (built by
-- extractors/planning_appeal_outcomes.py — council decision joined to ABP's OWN
-- decision via PC02; `overturned` is boolean). 13k matched appeals, 2016 onwards.
--
-- Grain: one row per council. overturn_rate_pct = 100 * overturned / matched
-- appeals (plain arithmetic, display_only). national_overturn_rate_pct is the
-- all-council benchmark (window over the whole set) so the page can draw the line.
--
-- PlanningAuthority is normalised to the local_authority join key used by
-- v_la_chief_executives / v_constituency_la_crosswalk. ⚠️ Cork County is ABSENT
-- from the appeals source (only 30 of 31 councils present) — it simply won't appear
-- here; that is a source coverage gap, not a zero.
CREATE OR REPLACE VIEW v_la_planning_overturn AS
WITH base AS (
    SELECT
        CASE PlanningAuthority
            WHEN 'Dun Laoghaire Rathdown County Council' THEN 'Dun Laoghaire-Rathdown'
            WHEN 'Cork City Council'                     THEN 'Cork City'
            WHEN 'Galway City Council'                   THEN 'Galway City'
            WHEN 'Galway County Council'                 THEN 'Galway County'
            WHEN 'Dublin City Council'                   THEN 'Dublin City'
            WHEN 'South Dublin County Council'           THEN 'South Dublin'
            WHEN 'Limerick County Council'               THEN 'Limerick'
            WHEN 'Waterford City and County Council'     THEN 'Waterford'
            ELSE trim(replace(replace(PlanningAuthority, ' County Council', ''), ' City Council', ''))
        END                              AS local_authority,
        CAST(overturned AS INTEGER)      AS overturned_int
    FROM read_parquet('data/silver/parquet/planning_appeal_outcomes.parquet')
)
SELECT
    local_authority,
    COUNT(*)                                                     AS n_appeals,
    SUM(overturned_int)                                          AS n_overturned,
    ROUND(100.0 * SUM(overturned_int) / COUNT(*), 1)            AS overturn_rate_pct,
    ROUND(100.0 * SUM(SUM(overturned_int)) OVER ()
                / SUM(COUNT(*)) OVER (), 1)                      AS national_overturn_rate_pct
FROM base
GROUP BY local_authority
ORDER BY overturn_rate_pct DESC;
