# Data Distribution Plan — stop using git as the data channel

Sibling to [SCALABILITY_PLAN.md](SCALABILITY_PLAN.md). That doc scales *serving*
(concurrent users). This one scales the *data footprint*: the read-only parquet
that has to reach every deployment, and the push/clone cost of carrying it in git.

## Problem

The app boots from a **clean git clone with no ETL** (Streamlit Cloud) or a
`COPY data ./data` image build ([`Dockerfile`](../Dockerfile)). So the runtime
data must be *somewhere the clone/image can reach* — and today that "somewhere"
is git itself. Git is a poor blob channel:

- **No delta compression for parquet.** A 59 KB logical change to
  `questions.parquet` re-stores and re-pushes the whole ~18 MB blob.
- **History is forever.** Every refresh's blobs stay in `.git` permanently
  (already 167 MB packed / 742 MB loose). Push and clone cost only grow.
- **~60 hand-maintained `!negation` lines in `.gitignore`.** Every new runtime
  dataset needs a new exception line. Maintenance burden scales with datasets.

## Target model — R2 is the data channel; git carries code only

The R2 mirror already exists ([`tools/backup_to_r2.ps1`](../tools/backup_to_r2.ps1),
versioning on — see [DATA_BACKUP.md](DATA_BACKUP.md)). The missing piece is the
**fetch** side. Flip the model:

1. **One manifest replaces ~60 gitignore exceptions.**
   `data/_meta/runtime_data_manifest.json` listing exactly the files the app
   reads at runtime, each with its R2 key + content hash. Single source of truth
   for "what a deployment needs." Adding a dataset = one manifest line.

2. **A fetch step** (`tools/fetch_runtime_data.py`) pulls those keys from R2 if
   missing/stale (hash check). Runs in two places:
   - **Dockerfile / API + future `Dockerfile.streamlit` build** → bakes data into
     the image (replaces `COPY data ./data` with a fetch-from-R2 step).
   - **Streamlit Cloud cold start** (guarded + cached to disk) → for the
     no-Docker platform.

3. **Untrack the heavy parquet** (`git rm --cached`, drop the negations, keep the
   blanket `*.parquet` ignore). Pushes become code-only (seconds). Repo stops
   growing.

### Trade-offs (honest)

- **Cold-start latency on Streamlit Cloud** — first request after a container
  recycle downloads the runtime working set. Mitigate: fetch only the runtime
  set (not the 9.6 GB bronze), cache to disk, lazy per-page. This pressure is
  itself an argument for SCALABILITY_PLAN Layer 1 (Fly/Render, image baked once).
- **R2 becomes a boot dependency.** Keep the handful of tiny always-must-work
  files (the members CSVs) committed as fallback; everything heavy comes from R2.
- **Existing history bloat** stays until a `git gc` (cheap) or a `git filter-repo`
  rewrite (disruptive — force-push + everyone re-clones). Defer the rewrite
  unless clone time becomes painful.

## Audit — what's actually runtime-read (2026-06-18)

Method (reproducible): the runtime-read set = every parquet read by a registered
SQL view (`read_parquet('…')` literals in `sql_views/`) ∪ every parquet read
directly in `utility/`, `api/`, `mcp_server/`, `dail_tracker_core/`, `services/`
∪ the config path-constants behind the templated `{…_PARQUET_PATH}` SQL paths.
Diff that against `git ls-files 'data/**/*.parquet'`.

**190 tracked data files, 147 MB.** Findings:

### NOT a quick win — `companies.parquet` (34 MB) + `financial_statements.parquet` (8.2 MB) are LIVE

These back `v_experimental_lobbying_org_index_enriched`
([`sql_views/lobbying/lobbying_experimental_org_index_enriched.sql`](../sql_views/lobbying/lobbying_experimental_org_index_enriched.sql)),
which **despite its "experimental" name is wired into production** via
[`dail_tracker_core/queries/lobbying.py:157`](../dail_tracker_core/queries/lobbying.py#L157)
(MCP/API lobbying tools + the `lobbying_3.py` org-detail panel).
**Do not untrack or delete — it would break a live feature.**

> **Trap:** the view is `v_experimental_lobbying_org_index_enriched`
> (`experimental_lobbying`), not `lobbying_experimental`. A naive grep for
> "lobbying_experimental" misses it and wrongly reports it dead. Always confirm
> the exact `CREATE VIEW` name before concluding a view is unused.

**Optimization candidate (the real CRO win, ~40 MB):** the view joins CRO rows
only where `name_norm` matches a lobbyist org (a few thousand orgs), but ships
the entire CRO register (~hundreds of thousands of companies). A pre-filtered
silver extract — CRO `companies`/`financial_statements` rows restricted to the
lobbyist-org universe — could reclaim most of the 42 MB. This is a **pipeline
change** (new slim extract → view reads it), data-anchored, sandbox→vet→promote.
Measure the matched-row count first to size the win before committing.

### Candidate non-runtime — ~8 MB across 47 small parquet read by no view/page

Verified absent from all runtime code (only produced by `extractors/` /
`pipeline_sandbox/` or read by `tools/` build scripts + tests):

- **Silver payment-facts** folded into gold `procurement_payments_fact` by the
  consolidate chain (`public_payments_fact`, `hse_tusla`, `nta`, `nphdb`, `seai`,
  `dept_readingorder`, `la_payments`) — ETL *inputs*, intentionally committed for
  provenance, but never read in the clone.
- **Gold pre-union copies** superseded by the consolidated fact
  (`public_payments_fact` gold 1.3 MB, `hse_tusla` gold).
- **Unsurfaced CSO series** (`cso_hpm03/07/09`, `vac*`, `pea*`, `hap*`, `f20*`
  ≈ 3 MB). Note `cso_gfa01` *is* surfaced by a view; the rest are not.

> **Caveat — candidate list, not a kill list.** A static scan can miss a file
> read via a path built at runtime. Confirm per-file before untracking. Payoff is
> small (<8 MB) and several are committed deliberately for provenance, so this is
> a judgement call, not an automatic drop.

## Recommended sequence

1. **Build the manifest + fetch step** from the runtime-read set above; switch the
   Dockerfile and a Cloud cold-start hook to fetch-from-R2. This is the structural
   fix and makes everything below moot.
2. **CRO slim-extract** (~40 MB) — biggest single footprint lever; do as a proper
   pipeline change once measured.
3. **Tier-2 (~8 MB)** — confirm per-file, then untrack the genuinely-dead gold
   artifacts; leave provenance-intent silver facts to the manifest model.
4. **`git gc`** now (cheap); defer history rewrite unless clone time bites.
