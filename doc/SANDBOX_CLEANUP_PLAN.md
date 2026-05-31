# Pipeline-sandbox cleanup plan

_Status: plan only — no code changes made by this doc. Written 2026-05-31._

This captures the state of the in-flight `pipeline_sandbox/` reorganisation and a
phased plan to land it cleanly. It is deliberately scoped to the **sandbox
graduation / `_experimental` convention** work currently sitting in the working
tree. It is **not** the package-layout reorg — that is the separate, still-deferred
`REORG_AUDIT.md` (`src/dail_tracker/` layout), which this plan feeds into but does
not execute.

> **Correction note.** An earlier draft of this plan referenced a "TD enrichment
> family" (`td_committee_membership`, `td_donations_sipo`, `td_electoral_history`,
> `td_register_interests`, `td_vote_attendance`). **No such files exist** in the
> repo — that list came from a misread of a truncated `git status`. Disregard it.
> The actual graduation question is about the sandbox scripts that *do* exist,
> listed below.

---

## 1. What is actually changing

The working tree has rolled out an explicit naming convention in
`pipeline_sandbox/`: anything that is a **provisional proof-of-concept, not
committed for promotion** gets an `_experimental.py` suffix; anything already
feeding a shipped SQL view / UI panel keeps its plain name.

This is consistent with the rule in memory ([[project-pipeline-sandbox-rule]]):
sandbox scripts are self-contained, never imported by the main pipeline, and are
cheap to discard. The suffix just makes "discardable vs load-bearing" legible at a
glance instead of tribal knowledge.

### 1a. In-scope (kept plain-named — load-bearing sandbox)

These 3 are already wired into shipped surfaces and must **not** get the suffix:

| Script | Feeds | Evidence |
|---|---|---|
| `cso_pxstat_extract.py` | `v_member_constituency_demographics`, (Phase 2) `v_constituency_civic_context` | ENRICHMENTS.md §H.1 |
| `cbi_registers_extract.py` | `sql_views/corporate_cbi_distress.sql`, Corporate Notices CBI panel | ENRICHMENTS.md §E.5a |
| `public_appointments_enrichment.py` | Public Appointments page | ENRICHMENTS.md §H.1 note |

### 1b. Deferred (renamed to `_experimental` — provisional PoC)

25 `_experimental.py` scripts total — the housing / local-authority / policy-table
family that backs the **provisional housing-locality PoC page**. In the current
*uncommitted* working tree, 23 are renames of previously-committed files and 1 is
genuinely net-new (`housing_locality_poc_experimental.py`);
`spending_review_shcep_extract_experimental.py` was already committed in an earlier
commit and is listed here only for completeness.

- **Renames (23):** `ahb_camelot`, `ahb_provision`, `census_saps_land`,
  `dcc_allocations`, `dfi_right_home`, `dlr_reports`, `eurostat`,
  `hap_funding_data`, `hap_funding_xlsx`, `housing_commission_targets`,
  `housing_la_master_build`, `housing_la_year_series_build`,
  `housing_national_year_series_build`, `noac_camelot`, `noac_housing`,
  `ombudsman_hap_limits`, `open_csv`, `pbo_camelot`, `policy_tables`,
  `ssha_a1_6a_traveller`, `ssha_appendix_a19`, `ssha_appendix_camelot`,
  `ssha_appendix_full` (all `_extract`/`_build` → `_extract_experimental` etc.)
- **Net-new (2):** `housing_locality_poc_experimental.py`,
  `spending_review_shcep_extract_experimental.py`

### 1c. Supporting edits

- `doc/ENRICHMENTS.md` §H.1 updated to point at `housing_la_master_build_experimental.py`
  and to document the intent of the suffix.
- `README.MD` updated.
- `data/.gitkeep`, `data/silver/.gitkeep`, `data/silver/lobbying/.gitkeep` deleted.

---

## 2. Verified-safe (nothing breaks when run)

- **All Python compiles.** Every `*_experimental.py` byte-compiles clean. (A bulk
  `py_compile` failure you may see is only because the *old* deleted paths no
  longer exist — the surviving files are fine.)
- **No dangling references.** Nothing outside `pipeline_sandbox/` imports or
  `subprocess`-launches any of the renamed scripts. `pipeline.py`'s `CHAINS` never
  referenced them; the sandbox is not on the orchestrated path. So the renames
  cannot break a pipeline run.
- **`.gitkeep` deletion is safe.** All three dirs are in `config.py:DIRS` and are
  recreated by `init_dirs()` at import, and the dirs are gitignored data sinks —
  the placeholders were redundant.

The one real risk is **git history**: the renames are currently staged as
`delete + add` (see §3), so without `git add -A` git will record them as
unrelated delete/create and lose `git log --follow` / blame continuity.

---

## 3. Cleanup steps — to lock the convention in

**Phase 0 — record the renames as renames (do first)**
1. `git add -A pipeline_sandbox/` so git pairs each delete with its `_experimental`
   add and records `R` (rename) entries, preserving blame/history.
2. Confirm with `git status` that the 23 pairs show as `renamed:` not
   `deleted:`/new.

**Phase 1 — doc + housekeeping consistency**
3. Re-scan `doc/ENRICHMENTS.md` for any remaining plain-named references to the
   renamed scripts (only §H.1 carried script paths; verify no others drifted).
4. Decide the `.gitkeep` question: either accept the deletions (dirs are
   auto-created) or, if you want the empty `data/` tree to survive a fresh clone
   for onboarding, restore one `.gitkeep` at `data/` only.
5. Commit as one coherent change: _"pipeline_sandbox: mark housing-locality PoC
   extractors `_experimental`; keep cso/cbi/appointments load-bearing"_.

**Phase 2 — graduation decision (the open call — see §4)**
6. For each in-scope script, decide whether it stays in `pipeline_sandbox/`
   permanently or graduates to a top-level domain module (the pattern from commit
   `018dd83 graduate 3 ETL scripts out of pipeline_sandbox/`). Graduation =
   move out, add a `*_refresh.py` chain entry in `pipeline.py:CHAINS` (or fold
   into an existing chain), add tests, keep the 258/11 baseline green.

**Phase 3 — fold into the package reorg (deferred)**
7. When the `src/dail_tracker/` reorg runs (`REORG_AUDIT.md`), the load-bearing
   sandbox scripts move into their `domains/<x>/` homes; the `_experimental`
   PoC family either graduates first or moves wholesale into a single
   `domains/_experimental/` (or stays out of the package until promoted).

---

## 4. Open decisions (need your call)

1. **Graduation set.** Which, if any, of the 3 load-bearing sandbox scripts
   (`cso_pxstat`, `cbi_registers`, `public_appointments`) should graduate out of
   `pipeline_sandbox/` into the orchestrated pipeline now vs stay sandboxed?
   Your "TD family only" answer was against a non-existent set, so this is still
   genuinely open.
2. **Housing-locality PoC fate.** The 25 `_experimental` scripts back a
   provisional page. Are they (a) staying indefinitely as a parked PoC, (b) on a
   path to promotion, or (c) candidates for deletion if the PoC is abandoned?
   This determines whether they belong in the package reorg at all.
3. **`.gitkeep` policy** (Phase 1 step 4).

---

## 5. Relationship to the big reorg

`REORG_AUDIT.md` / [[project-reorg-plan]] is the separate `src/dail_tracker/`
package migration, still deferred until ETL work hits a stable plateau
([[feedback-refactor-timing]]). This sandbox cleanup is a **prerequisite tidy**:
settling the graduated-vs-experimental split now means the reorg's
`domains/` mapping has a clean source set to move, instead of having to
adjudicate sandbox status mid-migration.
