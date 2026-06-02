# Seanad app-parity — remaining view + UI gaps

**Context.** The Seanad ETL is promoted; all Senator gold datasets exist in
`data/gold/parquet/` (`current_seanad_vote_history`, `seanad_payments_full_psa`,
`seanad_attendance_by_year`, `current_senator_payment_rankings`,
`seanad_master_list`). `member_overview`, Interests and Committees already surface
both chambers. **These are not ETL gaps — the gold is built. They are SQL-view +
Streamlit wiring gaps.**

Status: **ALL DONE** on branch `seanad-app-parity` (2026-06-02).

| Page | State | Action |
|------|-------|--------|
| Attendance | ✅ **DONE** | Dáil/Seanad picker shipped; views were already house-partitioned |
| Payments | ✅ **DONE** | `house` threaded through `v_payments_member_detail` + `v_payments_yearly_evolution` + alltime ranking/summary (windows now partition by `(payment_year, house)` / `house`); contamination bug fixed (Dáil ranking 428→372 members); picker + house-aware labels shipped |
| Votes | ✅ **DONE** | New `v_vote_base` chokepoint unions both gold parquets with a `house` column; 7 views rewritten to read from it; `votes_data` + `member_overview_data` pass `{SEANAD_VOTE_PARQUET_PATH}`; chamber toggle + renamed Divisions/Members view toggle shipped |

Verified end-to-end against real gold: votes conn (1208 Seanad divisions, 60 senators), payments conn (Dáil 372 / Seanad 64 — clean split, each house its own rank #1), member_overview senator divisions match between summary + detail (no glob double-count). `test_sql_views.py` updated (v_vote_base dependency + second placeholder); full suite 248 passed / 0 failed.

The sections below are retained as the implementation record.

---

## 1. Payments — PRIORITY (fixes a live correctness bug)

### The bug (verified 2026-06-02)
`v_payments_base` ([payments_base.sql:41-45](../sql_views/payments_base.sql#L41-L45))
already UNIONs `seanad_payments_full_psa.parquet` with a `house` column — this was
added to make the per-member panel resolve senators. **But every downstream view
drops `house`:**

- [payments_yearly_evolution.sql](../sql_views/payments_yearly_evolution.sql) —
  `GROUP BY member_name, position, taa_band_raw, taa_band_label, payment_year`
  (no house); window totals/ranks `PARTITION BY payment_year` only.
- `v_payments_alltime_ranking`, `v_payments_alltime_summary`, `v_payments_summary`
  inherit the mix.

**Effect on the live `/payments` page right now:** 64 Senators are blended into the
TD rankings and the "TDs with payments" / "Total since 2020" / "Avg per TD" metrics.
Verified: 2 senators sit at rank 31 and 40 of the all-time list; the ranking reports
428 "members" (≈ 372 TDs + 56 senators). The page is mislabeled as TD-only.

### Cross-house collision caveat
The payments parquet keys members by `member_name` ("Last, First"). Two names
(`Flaherty, Joe`, `Tully, Pauline`) appear in **both** house parquets. Identity must
become `(member_name, house)` — or better `(unique_member_code, house)` — wherever
payments currently group/rank by `member_name` alone. (Same class of collision as
Seán Kyne in `member_registry`.)

### Fix
1. **`payments_yearly_evolution.sql`** — add `house` to the `SELECT` and `GROUP BY`;
   change every window `PARTITION BY payment_year` → `PARTITION BY payment_year, house`
   (`rank_high`, `year_total_paid`, `year_member_count`, `year_avg_per_td`);
   `member_alltime_total` → `PARTITION BY member_name, house`.
2. **`payments_zz_alltime_ranking.sql`** — `per_member` CTE `GROUP BY member_name, house`;
   carry `house` to the projection; `RANK() OVER (... ORDER BY total DESC)` →
   `PARTITION BY house`.
3. **`payments_zz_alltime_summary.sql`** — `GROUP BY house` (one summary row per house).
4. **`payments_summary.sql`** — add `house` (`GROUP BY house`) so the data layer can
   scope the hero summary.
5. **`payments_data.py`** — add a `house: str = "Dáil"` param + `WHERE house = ?` to
   `fetch_filter_options`, `fetch_year_ranking`, `fetch_since_2020_summary`,
   `fetch_alltime_ranking`, `fetch_member_all_years`, `fetch_member_year_summary`,
   `fetch_member_payments`, `fetch_payments_summary`.
6. **`payments.py`** — add the Dáil/Seanad `st.segmented_control` (copy the attendance
   pattern in [attendance.py](../utility/pages_code/attendance.py)); house-aware labels:
   hero "TD Payments", glossary "TD", `totals_strip` "TDs with payments" / "Avg per TD",
   the `position` fallback `"Deputy"` ([payments.py:108](../utility/pages_code/payments.py#L108)),
   export filename `td_payments_*.csv`, and `_CAVEAT` ("the amount a TD receives").
   Provenance: the `PAYMENTS` PDF list in `ui/source_pdfs.py` is Dáil-only — suppress
   it on the Seanad view (as attendance now does) until a Seanad PSA-PDF list exists.

**Effort:** M (4 view edits + data-access + page). **Risk:** window-partition semantics
+ the identity-key decision. Add a payments fixture to `test/test_sql_views.py` (currently
deferred) asserting house-partitioned ranks don't mix chambers.

---

## 2. Votes — the true data-layer gap

### State
Gold `current_seanad_vote_history.parquet` **exists** but no vote view reads it. All
**7** vote views read a single Dáil parquet via the `{PARQUET_PATH}` substitution
([votes_data.py:40](../utility/data_access/votes_data.py#L40) →
`GOLD_VOTE_HISTORY_PARQUET` = `current_dail_vote_history.parquet`):
`vote_index`, `vote_td_summary`, `vote_td_year_summary`, `vote_member_detail`,
`vote_party_breakdown`, `vote_result_summary`, `vote_sources`. No `house` column anywhere.

### Fix (mirror the `member_registry` two-placeholder UNION)
1. **New `sql_views/vote_base.sql`** — `v_vote_base` UNIONs `{PARQUET_PATH}` (literal
   `'Dáil'`) and a new `{SEANAD_VOTE_PARQUET_PATH}` (literal `'Seanad'`) `UNION ALL BY
   NAME`, adding a `house` column. (Verify both parquets are `union_by_name`-compatible;
   votes were built by reusing `normalize_vote_data`, so schemas should match.)
2. **Rewrite the 7 views** to `FROM v_vote_base` instead of `read_parquet('{PARQUET_PATH}')`,
   threading `house` and partitioning per-house any rank/summary (`v_vote_index`
   counts, `vote_result_summary`, `vote_party_breakdown`).
3. **`votes_data.py`** — add the `{SEANAD_VOTE_PARQUET_PATH}` substitution (new config
   const, glob or explicit path); add a `house` param to `fetch_member_names` and the
   division queries.
4. **`votes.py`** — Dáil/Seanad picker; house-aware "Find a TD" → "Find a Senator",
   "Dáil Divisions" hero, "TDs recorded", export `dail_divisions.csv`. The vote-detail
   URL is already chamber-aware (`services.votes.build_vote_url(chamber)`).
5. **Edge case:** Seanad divisions have no abstentions (`staonVotes` is Null-typed) —
   the `abstained_count` column must tolerate all-zero/Null without breaking the margin
   or party-breakdown rendering.

**Effort:** M–L (1 new base view + 7 rewrites + data-access + page). Add a vote fixture
to `test/test_sql_views.py`.

---

## 3. Smaller follow-ups
- **Seanad attendance provenance PDFs** — `ui/source_pdfs.py` `ATTENDANCE` is a curated
  Dáil ("deputies-verification") list; suppressed on the Seanad view this branch. Add a
  curated `ATTENDANCE_SEANAD` list so Senator provenance links to real source PDFs.
- **member_overview attendance DETAIL for Seanad** — still gated (pre-existing). Needs a
  house-partitioned rate denominator in `attendance_member_summary.sql` /
  `attendance_timeline.sql` (the global Dáil sitting-count cross-join). See
  `project_seanad_parity` memory follow-up (1).
- **Identity key** — standardise member identity on `(unique_member_code, house)` across
  payments/votes views to absorb cross-house name and code collisions (Seán Kyne;
  Flaherty/Tully).
