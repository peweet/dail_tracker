---
tier: PLAN
status: SUPERSEDED
domain: infra
updated: 2026-07-17
supersedes: []
superseded_by: doc/HYBRID_REFRESH_PLAN.md
read_when: historical only — the live refresh runbook is HYBRID_REFRESH_PLAN.md; the two-lane R2 design below is DEAD (see [[project_two_lane_reconciliation_flaw_2026_07_09]])
key: PLAN|SUPERSEDED|infra
---

> **⚠️ SUPERSEDED (2026-07-17).** The live refresh design is **`doc/HYBRID_REFRESH_PLAN.md`**.
> The two-lane R2 reconciliation described below is **dead as designed**. Kept for history only.

# Continuous, reliable, broad refresh — runbook

**Created:** 2026-06-14. **Goal:** keep the live site's money-flow data fresh automatically, the
way paid procurement tools (Tussell/Stotles) do — without ever shipping bad data. This closes the
project's long-deferred "Phase 6" cloud-automation gap (see `project_freshness_architecture` Unit B,
`doc/CI_CD.md` §6).

## The two lanes

| Lane | Workflow | What it refreshes | State needed | Status |
|------|----------|-------------------|--------------|--------|
| **Live tenders** | `.github/workflows/live_tenders_refresh.yml` | the open national tender snapshot (Playwright scrape of etenders.gov.ie) | none — stateless rebuild | **works now, no secrets** |
| **Full money-flow** | `.github/workflows/money_flow_refresh.yml` | payments, procurement, TED, LA/HSE/Tusla, CRO, CBI, AFS, CSO + upstream deps | 9 GB bronze/silver, restored from R2 | **ready, pending R2 secrets + a timed probe** |

Both publish through the **same gated mechanism** (`tools/publish_data.py`) and push to `main`, which
triggers a Streamlit Cloud redeploy.

## Why it's safe to auto-commit to `main`

`tools/publish_data.py` is the only thing that commits, and it is built so it *cannot* go wrong:
- it stages/commits **only** the allow-listed data paths (`PUBLISH_PATHS`) — never code, never `git add .`;
- it runs an **integrity gate** first: every changed parquet must be readable + non-empty, and the whole
  gold layer must pass the completeness baseline (`tools/check_output_regressions.py --strict`). Any table
  that went MISSING / EMPTIED / ROW_DROP vs the committed baseline **aborts the publish** — nothing ships.

So a degraded scrape, a source outage, or a cold-thinned run can never regress the live app. Worst case
the job fails, opens an issue, and the site keeps the last good data.

## Why "live tenders" is safe today but "full pipeline" needs R2

The live-tenders extractor is **stateless** — it rebuilds the whole snapshot from the portal every run, so a
clean cloud runner produces complete data. The rest of the pipeline is **stateful**: `bronze/` (raw scrapes,
PDFs, CSVs) and big `silver/` are gitignored, so a clean runner would *cold-rebuild* incremental sources from
a thin slice ("cold-start thinning"). The fix is to **restore bronze+silver from the R2 backup first**, run the
pipeline incrementally, then back the refreshed state up again. That's what `money_flow_refresh.yml` does.

## What you need to do (the account-only steps)

### 1. Live tenders — turn it on (nothing else needed)
It's already scheduled (06:30 UTC daily). The first run (or a manual **Run workflow** from the Actions tab)
will scrape, then `publish_data.py` commits the snapshot. Confirm the Procurement page → *Open right now* shows
"National opportunities as of <today>". If your `main` has branch protection, allow the `github-actions` bot to
push, or switch this to a PR-based publish.

### 2. Full money-flow — three steps before relying on the schedule
1. **Add R2 secrets** (Settings → Secrets and variables → Actions). Reuse the same Cloudflare R2 token the
   local backup uses (`tools/backup_to_r2.ps1`, bucket `dail-tracker-backup`):
   - `R2_ACCESS_KEY_ID`
   - `R2_SECRET_ACCESS_KEY`
   - `R2_ENDPOINT`  (e.g. `https://<accountid>.r2.cloudflarestorage.com`)
2. **Measure the runtime** (the load-bearing unknown). Run `.github/workflows/pipeline_probe.yml` once with the
   `chains` input **blank** (full run, read-only — it publishes nothing). The summary prints elapsed seconds.
   Confirm it's comfortably under the 6 h GitHub job ceiling, then tighten `timeout-minutes` in
   `money_flow_refresh.yml` to ~1.5× that.
3. **Validate the loop once** without publishing: run `money_flow_refresh.yml` via **Run workflow** with
   `skip_publish = true`. This does restore → run → R2 backup but commits nothing. Check the log: R2 restore
   ran (no "secrets absent" warning), chains completed, no table thinned. Then let the daily schedule take over.

> Until R2 secrets exist, the daily `money_flow_refresh` run will cold-thin and the gate will (correctly) abort,
> opening a failure issue each day. Either complete step 2.1 first, or disable that workflow's `schedule`
> (comment the `cron:` line) and run it on demand until you're ready.

## How this maps to "continuous, reliable, broad"

- **Continuous** — scheduled GitHub Actions (live tenders daily 06:30; money-flow daily 04:00), auto-committing
  to `main` → Cloud redeploys. No human in the loop.
- **Reliable** — the publish gate refuses to ship regressions; R2 restore prevents cold-thinning; each lane opens
  a tracking issue on failure; the existing `freshness.yml` / `source_health.yml` canaries flag staleness; the
  Procurement page carries its own "snapshot may be out of date" guard past 3 days.
- **Broad** — the money-flow lane runs the whole pipeline (every money-flow chain + the upstream chains they
  depend on), not just one source. Live tenders adds the sub-EU-threshold national feed TED can't see.

## Not yet covered / follow-ups
- Register the live-tenders source in `tools/build_source_registry.py` so `source_health.json` tracks its
  staleness automatically (today the page's own freshness line covers it).
- OCR-dependent sources (SIPO expenses) stay off this lane — they need a GPU/long OCR pass and are handled
  off-box (see `feedback_paddleocr_crashes_local_box`).
- If `main` branch protection blocks bot pushes, switch `publish_data.py` to open a PR instead of pushing.
