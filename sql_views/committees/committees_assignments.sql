-- v_committee_assignments — one row per (member × committee).
-- Source: data/silver/committees/committee_assignments.parquet,
-- produced by committees_long_format_etl.py.
--
-- Replaces the in-page unpivot of committee_*/office_* wide columns that
-- used to live in utility/pages_code/committees.py::_load (two
-- df.iterrows() passes per page render — the actual hot path).
--
-- Columns match the contract the page consumes today: name, party,
-- constituency, dail_number, committee, committee_url, type, status,
-- role, is_chair, start, end. `chamber` is added so a single query
-- can filter to the active chamber. unique_member_code is resolved via
-- the shared v_lobbying_base_member_codes normalised-name lookup (LEFT
-- JOIN — an unmatched name yields '', which is NOT the same as absent)
-- so the page can link a member straight to their profile.

CREATE OR REPLACE VIEW v_committee_assignments AS
SELECT
    a.chamber,
    a.name,
    a.party,
    a.constituency,
    a.dail_number,
    a.committee,
    a.committee_url,
    a.type,
    a.status,
    a.role,
    a.is_chair,
    a.start,
    a."end",
    COALESCE(mc.unique_member_code, '') AS unique_member_code
FROM read_parquet('data/silver/committees/committee_assignments.parquet') a
LEFT JOIN v_lobbying_base_member_codes mc
       ON LOWER(strip_accents(TRIM(a.name))) = mc.norm_name;
