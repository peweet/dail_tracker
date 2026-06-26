-- Grain: one row per party (active committee memberships only).
-- Source: committee_assignments — LONG format parquet (data/silver/committees/, auto-registered by
--   lobby_processing.save_gold_outputs): one row per member x committee, party carried inline.
-- Used by: committee gold outputs + the output-regression baseline (si_baseline.json). No UI page
--   reads this table directly.
--
-- Long-format value domains (replacing the old wide 'Live'/role_title columns):
--   status ∈ {Active, Ended};  role ∈ {Member, Cathaoirleach, Leas-Chathaoirleach};  party inline.
SELECT
    party                                                  AS party,
    COUNT(*)                                               AS seats,
    COUNT(DISTINCT name)                                   AS distinct_members,
    SUM(CASE WHEN role = 'Cathaoirleach' THEN 1 ELSE 0 END) AS chairs_held
FROM committee_assignments
WHERE status = 'Active' AND committee IS NOT NULL
GROUP BY party
ORDER BY seats DESC
