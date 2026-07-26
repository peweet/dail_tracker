-- v_committee_office_holders — one row per (member × government office).
-- Source: data/silver/committees/office_holders.parquet, produced by
-- committees_long_format_etl.py.
--
-- Replaces the second df.iterrows() pass in committees.py::_load that
-- unpivoted office_N_* wide columns. Used by:
--   - Stage 2b TD profile ("Government offices" section)
--   - register-page govt-offices badge
--
-- unique_member_code is resolved via the shared v_lobbying_base_member_codes
-- normalised-name lookup (LEFT JOIN — an unmatched name yields '', not absent).

CREATE OR REPLACE VIEW v_committee_office_holders AS
SELECT
    o.chamber,
    o.name,
    o.party,
    o.office,
    o.start,
    o."end",
    COALESCE(mc.unique_member_code, '') AS unique_member_code
FROM read_parquet('data/silver/committees/office_holders.parquet') o
LEFT JOIN v_lobbying_base_member_codes mc
       ON LOWER(strip_accents(TRIM(o.name))) = mc.norm_name;
