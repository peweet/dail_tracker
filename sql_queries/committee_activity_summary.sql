-- Grain: one row per member (TD or Senator) with a committee assignment.
-- Source: committee_assignments — LONG format parquet (data/silver/committees/, auto-registered by
--   lobby_processing.save_gold_outputs): one row per member x committee, cols incl
--   name, party, chamber, committee, status, role, is_chair, start, end.
-- Used by: committee gold outputs + the output-regression baseline (si_baseline.json). No UI page
--   reads this table directly (the Committees page uses committee_assignments itself).
--
-- Long-format value domains (replacing the old wide 'Live'/role_title columns):
--   status ∈ {Active, Ended};  role ∈ {Member, Cathaoirleach, Leas-Chathaoirleach};  party inline.
SELECT
    name                                                    AS full_name,
    COUNT(DISTINCT committee)                               AS total_committees,
    SUM(CASE WHEN status = 'Active' THEN 1 ELSE 0 END)      AS active_committees,
    SUM(CASE WHEN role = 'Cathaoirleach' THEN 1 ELSE 0 END) AS chairs_held,
    MAX(party)                                              AS party
FROM committee_assignments
WHERE committee IS NOT NULL
GROUP BY name
ORDER BY total_committees DESC
