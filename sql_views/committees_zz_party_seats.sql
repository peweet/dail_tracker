-- v_committee_party_seats — long-format party seats per committee.
-- One row per (chamber, committee, party). Replaces the in-page
-- `df_long.groupby("committee")["party"].value_counts()` loop in
-- committees.py::_committee_summary.
--
-- Exposed separately from v_committee_member_detail's JSON column so
-- callers that don't want JSON have a flat-table option (charts, exports).

CREATE OR REPLACE VIEW v_committee_party_seats AS
SELECT
    chamber,
    committee,
    party,
    COUNT(*) AS seats
FROM v_committee_assignments
GROUP BY chamber, committee, party;
