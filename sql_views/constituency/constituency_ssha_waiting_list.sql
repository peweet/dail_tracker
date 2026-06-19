-- v_constituency_ssha_waiting_list — for each constituency, the social-housing
-- WAITING LIST (net need) in the local-authority area(s) serving it. The
-- demand-side companion to v_constituency_housing_context (which is supply-side:
-- vacancy, price, completions). One row per (constituency, serving council).
-- CONTEXT, council-area grain — NOT a per-constituency figure, never apportioned.
--
-- Source (gold, from the Housing Agency's Summary of Social Housing Assessments
-- 2025 report — pipeline_sandbox/housing/ssha_appendix_wide_extract_experimental.py):
--   * ssha_a1_8_time_on_list_wide.parquet — per-LA households on the Record of
--     Qualified Households (waiting list) by length of time waiting, for 2024 and
--     2025. The `total` column is the headline waiting-list count (identical across
--     all SSHA A1.x tables; extraction validated sum(buckets)==total, 62/62).
--
-- Metrics (2025 latest, with 2024 for year-on-year):
--   waiting_total_2025 / waiting_total_2024 — households qualified & waiting
--   waiting_yoy_pct      — % change 2024 -> 2025
--   long_wait_pct        — share waiting MORE THAN 4 years (4-5 + 5-7 + 7+)
--   over_7yr_pct         — share waiting MORE THAN 7 years
--
-- LA-name mapping is EXPLICIT (verified 31-row table): SSHA uses "Carlow County",
-- "Dun Laoghaire Rathdown County", "Limerick City and County" etc., while the
-- crosswalk uses "Carlow", "Dun Laoghaire-Rathdown", "Limerick" — a string-strip
-- would silently mis-join, so every LA is mapped by hand. local_authority matches
-- v_constituency_la_crosswalk exactly.
CREATE OR REPLACE VIEW v_constituency_ssha_waiting_list AS
WITH la_map(local_authority, ssha_la) AS (
    VALUES
    ('Carlow', 'Carlow County'),
    ('Cavan', 'Cavan County'),
    ('Clare', 'Clare County'),
    ('Cork City', 'Cork City'),
    ('Cork County', 'Cork County'),
    ('Donegal', 'Donegal County'),
    ('Dublin City', 'Dublin City'),
    ('Dun Laoghaire-Rathdown', 'Dun Laoghaire Rathdown County'),
    ('Fingal', 'Fingal County'),
    ('Galway City', 'Galway City'),
    ('Galway County', 'Galway County'),
    ('Kerry', 'Kerry County'),
    ('Kildare', 'Kildare County'),
    ('Kilkenny', 'Kilkenny County'),
    ('Laois', 'Laois County'),
    ('Leitrim', 'Leitrim County'),
    ('Limerick', 'Limerick City and County'),
    ('Longford', 'Longford County'),
    ('Louth', 'Louth County'),
    ('Mayo', 'Mayo County'),
    ('Meath', 'Meath County'),
    ('Monaghan', 'Monaghan County'),
    ('Offaly', 'Offaly County'),
    ('Roscommon', 'Roscommon County'),
    ('Sligo', 'Sligo County'),
    ('South Dublin', 'South Dublin County'),
    ('Tipperary', 'Tipperary County'),
    ('Waterford', 'Waterford City and County'),
    ('Westmeath', 'Westmeath County'),
    ('Wexford', 'Wexford County'),
    ('Wicklow', 'Wicklow County')
),
tol AS (
    SELECT
        la AS ssha_la,
        year,
        total,
        (CAST("4_5_years" AS DOUBLE) + CAST("5_7_years" AS DOUBLE)
            + CAST("more_than_7_years" AS DOUBLE)) AS over_4yr,
        CAST("more_than_7_years" AS DOUBLE) AS over_7yr
    FROM read_parquet('data/gold/parquet/ssha_a1_8_time_on_list_wide.parquet')
),
ssha AS (
    SELECT
        m.ssha_la,
        MAX(t.total) FILTER (WHERE t.year = 2025) AS waiting_total_2025,
        MAX(t.total) FILTER (WHERE t.year = 2024) AS waiting_total_2024,
        MAX(t.over_4yr) FILTER (WHERE t.year = 2025) AS over_4yr_2025,
        MAX(t.over_7yr) FILTER (WHERE t.year = 2025) AS over_7yr_2025
    FROM la_map m
    JOIN tol t ON t.ssha_la = m.ssha_la
    GROUP BY m.ssha_la
)
SELECT
    x.constituency_name,
    x.local_authority,
    x.link_type,
    s.waiting_total_2025,
    s.waiting_total_2024,
    CASE WHEN s.waiting_total_2024 > 0
         THEN ROUND(100.0 * (s.waiting_total_2025 - s.waiting_total_2024) / s.waiting_total_2024, 1)
    END AS waiting_yoy_pct,
    CASE WHEN s.waiting_total_2025 > 0
         THEN ROUND(100.0 * s.over_4yr_2025 / s.waiting_total_2025, 1)
    END AS long_wait_pct,
    CASE WHEN s.waiting_total_2025 > 0
         THEN ROUND(100.0 * s.over_7yr_2025 / s.waiting_total_2025, 1)
    END AS over_7yr_pct,
    2025 AS ssha_period
FROM v_constituency_la_crosswalk x
JOIN la_map  lm ON lm.local_authority = x.local_authority
LEFT JOIN ssha s ON s.ssha_la = lm.ssha_la
ORDER BY x.constituency_name, (x.link_type = 'primary') DESC, x.local_authority;
