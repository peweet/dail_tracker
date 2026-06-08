-- v_courthouses — active, geocoded courthouses for the venue map.
-- Source: data/gold/parquet/judiciary_courthouses.parquet
--   (Courts Service court-office register,
--   https://data.courts.ie/files/court-offices/court-offices.csv, CC-BY; promoted by
--   extractors/judiciary_bench_extract.py, active offices with non-null lat/lon only).
-- Pure projection — no classification. latitude/longitude are named for st.map.
CREATE OR REPLACE VIEW v_courthouses AS
SELECT
    court_house,
    address,
    eircode,
    region,
    county,
    circuit,
    latitude,
    longitude,
    source_name,
    source_url
FROM read_parquet('data/gold/parquet/judiciary_courthouses.parquet')
ORDER BY court_house;
