-- v_vote_base — chamber-unioned division roll-call rows (one row per member-vote).
--
-- Mirrors member_registry.sql's two-placeholder pattern:
--   {PARQUET_PATH}              → Dáil gold   (current_dail_vote_history.parquet)
--   {SEANAD_VOTE_PARQUET_PATH}  → Seanad gold (current_seanad_vote_history.parquet)
-- Both share an identical 17-column schema (same enrich._build_vote_history
-- helper), so UNION ALL BY NAME aligns them. A `house` literal is added so the
-- votes page can scope divisions / members / hero stats to one chamber, and so
-- cross-house unique_member_code collisions (e.g. Seán Kyne) stay separable.
--
-- ALL other vote_*.sql views read FROM v_vote_base — never read_parquet
-- directly — so the chamber union lives in exactly one place. A connection that
-- only wants one chamber points both placeholders at the same parquet (or a
-- glob); the per-member views filter by unique_member_code regardless.

CREATE OR REPLACE VIEW v_vote_base AS
SELECT *, 'Dáil'   AS house FROM read_parquet('{PARQUET_PATH}')
UNION ALL BY NAME
SELECT *, 'Seanad' AS house FROM read_parquet('{SEANAD_VOTE_PARQUET_PATH}');
