-- v_housing_rent_by_county — average weekly private rent by county (Census 2022),
-- the affordability figure shown when drilling into a county on the Housing screen.
--
-- Source: cso_f2023b ('Average weekly rent', 'Rented from private landlord', latest
-- census). F2023B's geography differs from the 26-county rollup used elsewhere:
-- Cork is a single 'Cork City and Cork County' row (maps to Cork), but DUBLIN is
-- split into 4 (Dublin City / DLR / Fingal / South Dublin) and GALWAY into 2, with no
-- single county total. Rather than invent a population-weighted blend, those two
-- counties are deliberately OMITTED (24 of 26 counties carry a rent figure). An
-- explicit map — no string-strip — so the City/County merges can't silently mis-join.
CREATE OR REPLACE VIEW v_housing_rent_by_county AS
WITH cmap(county, f2023b_area) AS (
    VALUES
    ('Carlow','Carlow'), ('Cavan','Cavan'), ('Clare','Clare'),
    ('Cork','Cork City and Cork County'), ('Donegal','Donegal'),
    ('Kerry','Kerry'), ('Kildare','Kildare'), ('Kilkenny','Kilkenny'),
    ('Laois','Laois'), ('Leitrim','Leitrim'), ('Limerick','Limerick City and County'),
    ('Longford','Longford'), ('Louth','Louth'), ('Mayo','Mayo'),
    ('Meath','Meath'), ('Monaghan','Monaghan'), ('Offaly','Offaly'),
    ('Roscommon','Roscommon'), ('Sligo','Sligo'), ('Tipperary','Tipperary'),
    ('Waterford','Waterford City and County'), ('Westmeath','Westmeath'),
    ('Wexford','Wexford'), ('Wicklow','Wicklow')
),
rent AS (
    SELECT "County and City" AS area, CAST(VALUE AS DOUBLE) AS avg_weekly_private_rent,
           "Census Year" AS rent_period
    FROM read_parquet('data/gold/parquet/cso_f2023b.parquet')
    WHERE "Statistic Label" = 'Average weekly rent'
      AND "Nature of Occupancy" = 'Rented from private landlord'
      AND "Census Year" = (SELECT MAX("Census Year") FROM read_parquet('data/gold/parquet/cso_f2023b.parquet'))
)
SELECT m.county, r.avg_weekly_private_rent, r.rent_period
FROM cmap m JOIN rent r ON r.area = m.f2023b_area
ORDER BY m.county;
