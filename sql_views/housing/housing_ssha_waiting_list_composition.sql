-- v_ssha_waiting_list_composition — the COMPOSITION of the social-housing waiting
-- list (Housing Agency SSHA 2025): who is waiting, by five dimensions, at three
-- grains in one tidy view. Powers the national Housing screen, the county view, and
-- the per-constituency "who's waiting here" slice from a single source.
--
-- One row per (grain, area, year, dimension, category).
--   grain     : 'national' | 'county' (26) | 'la' (31)
--   area      : 'Ireland' | county name | local-authority name
--   dimension : time_on_list | tenure | employment | household | citizenship
--   ord       : display order within a dimension (time buckets MUST stay short→long;
--               others get a curated order); NULL only if a slug is unmapped
--   count     : households   pct : count / (grain,area,year,dimension) total, 1dp
--
-- Source (gold, SSHA 2025 wide tables — ssha_appendix_wide_extract_experimental.py):
--   A1.8 time-on-list · A1.7 tenure · A1.2 employment · A1.4 household · A1.9 citizenship.
-- Each table is UNPIVOTed (EXCLUDE la, year, total), tagged with its dimension, and
-- joined to an explicit slug→label/ord meta table (keeps all labelling in SQL, not UI).
-- National = SUM over the 31 LAs; county = SUM over the LA→county rollup (Dublin's 4,
-- Cork's/Galway's 2 merged). All aggregation lives here; the page only renders.
--
-- NOTE citizenship is included as a neutral, aggregate-only distribution (main-applicant
-- citizenship as a share of qualified households — not "who gets housing"). The page
-- presents it factually with the denominator stated and never derives ratios in prose.
CREATE OR REPLACE VIEW v_ssha_waiting_list_composition AS
WITH tol AS (
    SELECT la, year, 'time_on_list' AS dimension, slug, CAST(cnt AS BIGINT) AS count
    FROM (UNPIVOT read_parquet('data/gold/parquet/ssha_a1_8_time_on_list_wide.parquet')
          ON COLUMNS(* EXCLUDE (la, year, total)) INTO NAME slug VALUE cnt)
),
ten AS (
    SELECT la, year, 'tenure' AS dimension, slug, CAST(cnt AS BIGINT) AS count
    FROM (UNPIVOT read_parquet('data/gold/parquet/ssha_a1_7_tenure_wide.parquet')
          ON COLUMNS(* EXCLUDE (la, year, total)) INTO NAME slug VALUE cnt)
),
emp AS (
    SELECT la, year, 'employment' AS dimension, slug, CAST(cnt AS BIGINT) AS count
    FROM (UNPIVOT read_parquet('data/gold/parquet/ssha_a1_2_employment_wide.parquet')
          ON COLUMNS(* EXCLUDE (la, year, total)) INTO NAME slug VALUE cnt)
),
hh AS (
    SELECT la, year, 'household' AS dimension, slug, CAST(cnt AS BIGINT) AS count
    FROM (UNPIVOT read_parquet('data/gold/parquet/ssha_a1_4_household_size_wide.parquet')
          ON COLUMNS(* EXCLUDE (la, year, total)) INTO NAME slug VALUE cnt)
),
cit AS (
    SELECT la, year, 'citizenship' AS dimension, slug, CAST(cnt AS BIGINT) AS count
    FROM (UNPIVOT read_parquet('data/gold/parquet/ssha_a1_9_citizenship_wide.parquet')
          ON COLUMNS(* EXCLUDE (la, year, total)) INTO NAME slug VALUE cnt)
),
la_long AS (
    SELECT * FROM tol UNION ALL SELECT * FROM ten UNION ALL SELECT * FROM emp
    UNION ALL SELECT * FROM hh UNION ALL SELECT * FROM cit
),
meta(dimension, slug, label, ord) AS (
    VALUES
    -- time_on_list (order is load-bearing: short → long)
    ('time_on_list','less_than_6_months','Under 6 months',1),
    ('time_on_list','6_12_months','6–12 months',2),
    ('time_on_list','1_2_years','1–2 years',3),
    ('time_on_list','2_3_years','2–3 years',4),
    ('time_on_list','3_4_years','3–4 years',5),
    ('time_on_list','4_5_years','4–5 years',6),
    ('time_on_list','5_7_years','5–7 years',7),
    ('time_on_list','more_than_7_years','More than 7 years',8),
    -- tenure
    ('tenure','private_rented_accommodation_with_and_without_rent_supplement','Private rented',1),
    ('tenure','living_with_parents','Living with parents',2),
    ('tenure','living_with_relatives_friends','Living with relatives / friends',3),
    ('tenure','emergency_accommodation_none','Emergency accommodation / none',4),
    ('tenure','owner_occupier','Owner occupier',5),
    ('tenure','other','Other',6),
    -- employment
    ('employment','unemployed_and_in_receipt_of_social_community_welfare_benefit','Unemployed (welfare)',1),
    ('employment','employed_full_part_self','Employed',2),
    ('employment','one_parent_family_support_only','One-parent family payment',3),
    ('employment','pensioner_retired','Pensioner / retired',4),
    ('employment','homemaker_no_income','Homemaker (no income)',5),
    ('employment','training_back_to_work_solas_scheme','Training / back-to-work',6),
    ('employment','other','Other',7),
    -- household
    ('household','1_adult','1 adult',1),
    ('household','1_adult_1_2_children','1 adult, 1–2 children',2),
    ('household','1_adult_3_or_more_children','1 adult, 3+ children',3),
    ('household','couple','Couple',4),
    ('household','couple_1_2_children','Couple, 1–2 children',5),
    ('household','couple_3_or_more_children','Couple, 3+ children',6),
    ('household','2_adults','2 adults',7),
    ('household','2_adults_with_children','2 adults with children',8),
    ('household','couple_1_or_more_adults_1_2_children','Couple + adult(s), 1–2 children',9),
    ('household','couple_with_1_or_more_other_adults','Couple + other adult(s)',10),
    ('household','3_or_more_adults','3+ adults',11),
    ('household','3_or_more_adults_with_children','3+ adults with children',12),
    ('household','couple_1_or_more_adults_3_or_more_children','Couple + adult(s), 3+ children',13),
    -- citizenship
    ('citizenship','irish_citizen','Irish',1),
    ('citizenship','eea_citizen','EEA',2),
    ('citizenship','non_eea_citizen','Non-EEA',3),
    ('citizenship','uk_citizen','UK',4)
),
la_lab AS (
    SELECT l.la, l.year, l.dimension, COALESCE(m.label, l.slug) AS category, m.ord, l.count
    FROM la_long l
    LEFT JOIN meta m ON m.dimension = l.dimension AND m.slug = l.slug
),
-- SSHA LA (31) → traditional county (26). Dublin/Cork/Galway merge their city+county LAs.
la_county(la, county) AS (
    VALUES
    ('Carlow County','Carlow'), ('Cavan County','Cavan'), ('Clare County','Clare'),
    ('Cork City','Cork'), ('Cork County','Cork'), ('Donegal County','Donegal'),
    ('Dublin City','Dublin'), ('Dun Laoghaire Rathdown County','Dublin'), ('Fingal County','Dublin'),
    ('South Dublin County','Dublin'), ('Galway City','Galway'), ('Galway County','Galway'),
    ('Kerry County','Kerry'), ('Kildare County','Kildare'), ('Kilkenny County','Kilkenny'),
    ('Laois County','Laois'), ('Leitrim County','Leitrim'), ('Limerick City and County','Limerick'),
    ('Longford County','Longford'), ('Louth County','Louth'), ('Mayo County','Mayo'),
    ('Meath County','Meath'), ('Monaghan County','Monaghan'), ('Offaly County','Offaly'),
    ('Roscommon County','Roscommon'), ('Sligo County','Sligo'), ('Tipperary County','Tipperary'),
    ('Waterford City and County','Waterford'), ('Westmeath County','Westmeath'),
    ('Wexford County','Wexford'), ('Wicklow County','Wicklow')
),
la_grain AS (
    SELECT 'la' AS grain, la AS area, year, dimension, category, ord, count FROM la_lab
),
county_grain AS (
    SELECT 'county' AS grain, c.county AS area, l.year, l.dimension, l.category, l.ord,
           SUM(l.count) AS count
    FROM la_lab l JOIN la_county c ON c.la = l.la
    GROUP BY c.county, l.year, l.dimension, l.category, l.ord
),
national_grain AS (
    SELECT 'national' AS grain, 'Ireland' AS area, year, dimension, category, ord,
           SUM(count) AS count
    FROM la_lab
    GROUP BY year, dimension, category, ord
),
allg AS (
    SELECT * FROM la_grain UNION ALL SELECT * FROM county_grain UNION ALL SELECT * FROM national_grain
)
SELECT
    grain, area, year, dimension, category, ord, count,
    ROUND(100.0 * count / NULLIF(SUM(count) OVER (PARTITION BY grain, area, year, dimension), 0), 1) AS pct
FROM allg
ORDER BY grain, area, year, dimension, ord NULLS LAST, count DESC;
