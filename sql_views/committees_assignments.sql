-- v_committee_assignments — one row per (member × committee).
-- Source: data/silver/committees/committee_assignments.parquet,
-- produced by pipeline_sandbox/committees_long_format_etl.py.
--
-- Replaces the in-page unpivot of committee_*/office_* wide columns that
-- used to live in utility/pages_code/committees.py::_load (two
-- df.iterrows() passes per page render — the actual hot path).
--
-- Columns match the contract the page consumes today: name, party,
-- constituency, dail_number, committee, committee_url, type, status,
-- role, is_chair, start, end. `chamber` is added so a single query
-- can filter to the active chamber.

CREATE OR REPLACE VIEW v_committee_assignments AS
SELECT
    chamber,
    name,
    party,
    constituency,
    dail_number,
    committee,
    committee_url,
    type,
    status,
    role,
    is_chair,
    start,
    "end"
FROM read_parquet('data/silver/committees/committee_assignments.parquet');
