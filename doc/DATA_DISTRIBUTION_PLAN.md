---
tier: PLAN
status: LIVE
domain: infra
updated: 2026-06-19
supersedes: []
read_when: planning or implementing the move of runtime parquet distribution from git to R2 (manifest-driven publish/fetch lane)
key: PLAN|LIVE|infra
---

# Data Distribution Plan — stop using git as the data channel

Sibling to [archive/SCALABILITY_PLAN.md](archive/SCALABILITY_PLAN.md). That doc scales *serving*
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

An R2 mirror exists ([`tools/backup_to_r2.ps1`](../tools/backup_to_r2.ps1) — see
[DATA_BACKUP.md](DATA_BACKUP.md)) — but it **cannot serve the runtime set as-is**, two
reasons (both verified, both easy to miss):

- It is **append-only** (`rclone copy --ignore-existing`, [backup_to_r2.ps1:9](../tools/backup_to_r2.ps1#L9)).
  A *refreshed* parquet (same path) would never overwrite the stale R2 object — but a runtime
  channel must serve the *current* table.
- It mirrors **bronze + silver only, not gold** ([backup_to_r2.ps1:17](../tools/backup_to_r2.ps1#L17)
  — gold "is backed up by `git push`"). The runtime-read set is gold-heavy (101 runtime parquet,
  ~142 MB; gold dominates).

So **both** the publish side *and* the fetch side need building — not just fetch. The fix is a
separate **runtime publish lane** (new `runtime/` prefix, hash-sync, includes gold) that leaves
the append-only archive untouched. Flip the model:

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

## Retention model + foundation tooling (shipped 2026-06-19)

The prose below blurred three states of a tracked data file into a "judgement call." They are now
a modeled, tested classification — `retention ∈ {runtime, lineage, dead}`:

- **`runtime`** — read by a registered SQL view, the `/v1/data` export catalog, or a member-view
  placeholder; **must reach the clone**.
- **`lineage`** — never read at runtime; an ETL *input* kept deliberately for reproducibility (the
  per-source silver payment-facts folded into gold `procurement_payments_fact`, and the gold
  pre-union copies it supersedes). The "kept for provenance" tail — **not** a deletion target.
- **`dead`** — no runtime reader found and not lineage; an untrack **candidate**, surfaced for
  human review, never auto-removed.

> **Two senses of "provenance" — don't conflate.** This is the *data-retention / lineage* sense.
> It is distinct from the *user-facing verifiability* sense (where a displayed number came from),
> which has its own well-structured tier model in [API_PROVENANCE_REVIEW.md](API_PROVENANCE_REVIEW.md)
> (T1/T2/T3). This feature deliberately uses the word **lineage** to avoid the collision.

**Source of truth:** [`data/_meta/runtime_data_manifest.json`](../data/_meta/runtime_data_manifest.json),
generated by [`tools/build_runtime_manifest.py`](../tools/build_runtime_manifest.py). It computes
the runtime set from *actual reads* (not a hand list) and carries each file's sha256 + `runtime/`
R2 key. Current split: **101 runtime / 9 lineage / 37 dead** of 147 tracked parquet (~142 MB
runtime). Tooling (Phase-1 foundation — additive, reversible; **no untracking, no Dockerfile/Cloud
changes yet**):

| Tool | Role |
|---|---|
| [`tools/build_runtime_manifest.py`](../tools/build_runtime_manifest.py) | classify + hash; `--check` is the CI drift guard |
| [`tools/publish_runtime_to_r2.ps1`](../tools/publish_runtime_to_r2.ps1) | R2-lane publish of the runtime set (incl. gold) to `runtime/`, hash-sync, completeness-gated |
| [`tools/fetch_runtime_data.py`](../tools/fetch_runtime_data.py) | hash-checked rehydrate from R2 (`--dry-run`) |
| [`test/tools/test_runtime_manifest.py`](../test/tools/test_runtime_manifest.py) | drift guard · `PUBLISH_PATHS` parity · placeholder-map · ship-gap tripwires |

**Ship gap found + fixed (2026-06-19):** `data/silver/parquet/stateboards_boards.parquet` backs the
live `v_stateboards_boards` but was **not git-tracked** — a fresh Cloud clone silently lost that
view. Surfaced by the manifest's `referenced_but_untracked`, now resolved by a `.gitignore`
negation + commit (it's a runtime entry, `referenced_but_untracked` is empty).

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
