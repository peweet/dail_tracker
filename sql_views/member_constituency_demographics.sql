-- v_member_constituency_demographics — population per Dáil constituency,
-- with the constituency's headline civic-context numbers attached. One row
-- per constituency, joinable to v_member_registry.constituency.
--
-- Source: data/gold/parquet/cso_fy005.parquet
--   CSO PxStat table FY005 — "Population of each Constituency of Dáil
--   Éireann" (Census 2022 Profile 1). Pre-computes Pop 2016 / Pop 2022 /
--   % change / TDs per constituency / Population per TD.
--
-- BOUNDARY NOTE — important: FY005 is keyed on the **2017** constituency
-- boundaries (39 constituencies). The current 34th Dáil sits on the 2023
-- Electoral Commission boundaries (43 constituencies). 35 of 43 current
-- constituency names carry forward unchanged; the rest split or renamed:
--
--   FY005 (2017)             →  v_member_registry (2023 current)
--   Dublin Fingal             →  Dublin Fingal East + Dublin Fingal West
--   Tipperary                 →  Tipperary North + Tipperary South
--   Laois-Offaly              →  Laois + Offaly
--   Dublin South Central      →  Dublin South-Central  (hyphen — normalised
--                                here so it joins cleanly)
--   Wicklow (some EDs)        →  Wicklow + Wicklow-Wexford  (boundary
--                                changed; FY005 figure stays attached to
--                                "Wicklow" which still exists by name)
--
-- For the four split cases above, FY005 has NO row matching the new sub-
-- constituency name. Joins from those TDs will return NULL — the UI must
-- caption gracefully ("2017 boundary aggregate split for 2024 election")
-- and not present a misleading number. Phase 2 (SAPS spatial-join to 2023
-- Tailte Éireann boundaries) will close this gap.
--
-- The "All Constituencies" aggregator row is excluded; it is recoverable
-- as the national population denominator from cso_pea08 (PEA08).

CREATE OR REPLACE VIEW v_member_constituency_demographics AS
WITH fy005 AS (
    SELECT
        -- Strip the literal " Constituency" suffix to match the form used
        -- in v_member_registry.constituency (e.g. "Carlow-Kilkenny", not
        -- "Carlow-Kilkenny Constituency"). Then apply the small alias map
        -- for known cosmetic differences between FY005 (2017) and the
        -- current registry (2023).
        CASE
          WHEN regexp_replace("Constituency 2017", ' Constituency$', '')
               = 'Dublin South Central'
            THEN 'Dublin South-Central'
          ELSE regexp_replace("Constituency 2017", ' Constituency$', '')
        END                                                        AS constituency_2017,
        "STATISTIC"                                                AS stat_code,
        TRY_CAST("VALUE" AS DOUBLE)                                AS stat_value
    FROM read_parquet('data/gold/parquet/cso_fy005.parquet')
    WHERE "Constituency 2017" <> 'All Constituencies'
)
SELECT
    constituency_2017                                       AS constituency_name,
    CAST(MAX(stat_value) FILTER (WHERE stat_code = 'FY005C01') AS BIGINT) AS population_2016,
    CAST(MAX(stat_value) FILTER (WHERE stat_code = 'FY005C02') AS BIGINT) AS population_2022,
    ROUND(MAX(stat_value) FILTER (WHERE stat_code = 'FY005C03'), 1)       AS pct_change_2016_2022,
    CAST(MAX(stat_value) FILTER (WHERE stat_code = 'FY005C04') AS INTEGER) AS td_seats_2022,
    CAST(MAX(stat_value) FILTER (WHERE stat_code = 'FY005C05') AS BIGINT) AS population_per_td_2022,
    'Census 2022 (2017 boundaries)'                         AS boundaries_label,
    'CSO PxStat FY005'                                      AS source_key
FROM fy005
GROUP BY constituency_2017
ORDER BY constituency_2017;
