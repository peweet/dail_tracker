-- v_lobbying_base_member_codes — normalised member-name → unique_member_code resolver.
-- Source: data/silver/parquet/flattened_members.parquet (canonical Dáil/Seanad list).
--
-- Shared base view for the lobbying surface. The four v_lobbying_* views that link a
-- lobbying-register name back to an Oireachtas member (contact_detail, org_intensity,
-- policy_exposure, revolving_door) all need the SAME accent-folded name→code lookup,
-- which was previously copy-pasted VERBATIM as an inline `member_codes` CTE into each.
-- Extracting it here keeps the normalisation rule (LOWER(strip_accents(TRIM(...)))) and
-- the dedup tie-break in ONE place, so a change to either can't silently drift across
-- four files. Consumers keep their own source-side LOWER(strip_accents(TRIM(...))) in the
-- JOIN ON clause; only the member-registry side moved here, so output is unchanged.
--
-- Grain: one row per normalised name. Where a normalised name maps to >1 code (rare —
-- a reused display name across terms) the HIGHEST unique_member_code wins, preserving
-- the prior inline behaviour exactly.
--
-- REGISTRATION ORDER (load-bearing — see [[feedback_sql_view_dependency_order]]): this
-- view MUST register before its consumers. Two mechanisms guarantee that:
--   1. Within the `lobbying_*.sql` glob it sorts FIRST ('lobbying_b…' < 'lobbying_c…'),
--      so the alphabetical register_views loop creates it ahead of every consumer.
--   2. It is listed immediately AHEAD of lobbying_revolving_door.sql in
--      dail_tracker_core.connections.DOMAIN_FILES (the member-overview / constituency /
--      api connections register revolving_door explicitly, not via the glob).
-- Do NOT rename this file to drop the `base` token — the 'b' is what keeps it first.

CREATE OR REPLACE VIEW v_lobbying_base_member_codes AS
SELECT norm_name, unique_member_code
FROM (
    SELECT
        LOWER(strip_accents(TRIM(full_name))) AS norm_name,
        unique_member_code,
        ROW_NUMBER() OVER (
            PARTITION BY LOWER(strip_accents(TRIM(full_name)))
            ORDER BY unique_member_code DESC
        ) AS rn
    FROM read_parquet('data/silver/parquet/flattened_members.parquet')
    WHERE full_name IS NOT NULL AND unique_member_code IS NOT NULL
)
WHERE rn = 1;
