-- v_committee_office_holders — one row per (member × government office).
-- Source: data/silver/committees/office_holders.parquet, produced by
-- committees_long_format_etl.py.
--
-- Replaces the second df.iterrows() pass in committees.py::_load that
-- unpivoted office_N_* wide columns. Used by:
--   - Stage 2b TD profile ("Government offices" section)
--   - register-page govt-offices badge

CREATE OR REPLACE VIEW v_committee_office_holders AS
SELECT
    chamber,
    name,
    party,
    office,
    start,
    "end"
FROM read_parquet('data/silver/committees/office_holders.parquet');
