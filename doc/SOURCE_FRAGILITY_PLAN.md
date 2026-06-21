# Source-Fragility Plan — make "tell me the moment a source breaks" the #1 priority

**Date:** 2026-06-21 · **Status:** plan (audit complete, actions prioritised, not yet executed)

This project has crossed from "an app you maintain" into "a **data platform you operate**."
The dominant maintenance cost is not the code — it is the **81 extractors scraping public
sources that change without warning** (PDF layouts shift, portals add WAFs, OCR drifts,
whole datasets get deleted upstream — as HSE did). The strategic decision: **treat source
fragility as the #1 priority. Lean into "tell me the moment a source breaks" over adding new
sources.** Every new source is a permanent liability, not a one-time build.

The good news the audit surfaced: **the instrumentation already largely exists.** The work is
mostly *arming what is already built* and *extending it to the uncovered high-value data* —
not building a monitoring stack from scratch.

Companion: **`doc/SOURCE_RECOVERY_RUNBOOK.md`** — what to actually DO when a source breaks.

---

## Part A — Audit: what monitoring exists today (2026-06-21)

Eight detection systems exist, in three families: **generators** (pipeline chains that write
state JSON, always exit 0), **reporters** (scheduled GitHub Actions that read that JSON and
open an Issue on breach), and **live probes** (network checks).

| # | System | Files | What it catches | Trigger | On breach |
|---|--------|-------|-----------------|---------|-----------|
| 1 | **Source-health** | `tools/build_source_health.py`, `source_health.json`, `source_health.yml` | per-source staleness over the registry | Mon/Thu 08:00 | Issue `data`,`sources` |
| 2 | **Freshness** | `tools/check_freshness.py`, `freshness.json`, `freshness.yml` | "did the pipeline / a dataset stop updating" (12 datasets + `generated_at`) | Mon/Thu 08:00 | Issue `data`,`freshness` |
| 2b | **Lane heartbeats** | `tools/freshness_status.py`, `freshness_heartbeat.py`, `data/_meta/heartbeats/*.json` | "did a scheduled lane stop running" (6 lanes) | in `freshness.yml` | *(non-strict — prints only)* |
| 3 | **Endpoint health** | `pdf_infra/pdf_endpoint_check.py`, `nightly.yml` | live reachability of ~90 PDFs + 5 API canaries | Monday 08:00 | Issue `sources`,`link-rot` |
| 4 | **Pipeline probe** | `pipeline_probe.yml`, `tools/gold_rowcounts.py` | does the full pipeline execute on Linux; runtime; cold-start thinning | manual only | step summary |
| 5 | **Procurement poller** | `tools/procurement_source_poller.py`, `procurement_source_poll.json` | new upstream quarter from orphan publishers (bespoke parsers not in `pipeline.py`) | Mon/Thu 08:00 | Issue `data`,`procurement` |
| 6 | **Output regressions** | `tools/check_output_regressions.py`, `output_baseline.json` | row-thinning / emptied table / dropped column across **109 gold tables** | CI `--strict` + publish gate | PR fail / publish abort |
| 6b | **Gold quality** | `tools/check_gold_quality.py`, `gold_quality_baseline.json` | content rot: null columns, dup multiplication, mojibake, null-sentinels | CI `--strict` | PR fail |
| 7 | **Data contracts** | `services/data_contracts.py`, `data/_meta/quarantine/` | closed-vocab + cross-column invariants + reconciliation (**procurement payment facts only**) | inline in pipeline | halt + quarantine rows |
| 8 | **Lobbying / Legal-Diary canaries** | `lobbying_freshness.yml`, `legal_diary_openview_health.yml` | upstream period ahead of held data; source-structure drift | weekly / Mon-Thu | Issue `data` |
| — | **Row-floor guard** | `services/parquet_io.save_parquet(min_rows=)` | refuses to overwrite a fact with a truncated harvest (**this is what saved HSE**) | inline, ~16 call sites | `RowFloorViolation` |
| — | **R2 backup** | `tools/backup_to_r2.ps1`, `backup_manifest.tsv` | off-box durability of bronze+silver | weekly, local Windows task | manifest `git diff` |

### Coverage matrix — which failure modes are caught

| Failure mode | Covered by | **Gap** |
|---|---|---|
| **Reachability (link rot, WAF, 404)** | endpoint check (~90 hardcoded URLs + 5 API canaries); poller (2 auto publishers); legal-diary health; source-health *only if* `DAIL_CHECK_LINKS=1` | 6 "manual" procurement publishers (SEAI/Tusla/HSE/3 depts) never probed; **118 registry sources `skipped` by default**; URL list hardcoded, drifts from registry |
| **Staleness (data age)** | freshness (12 datasets); source-health file_age (4 offline sources); lobbying canary | **lane ledger non-strict → silent**; 5 of 6 lanes never beat; PSA payments/attendance/interests/SIPO/diaries/planning/TED/CSO/news **not in freshness** |
| **Row-count drop** | output_regressions (109 tables, `--strict`); reconciliation (procurement); row-floor (~16 facts) | 50% default tolerance — a <50% silent loss passes; planning/news/live-tenders/TED **not in baseline** |
| **Schema drift** | output_regressions (`COL_REMOVED`); data-contract structural floor; SQL-view contract suite; API canaries | structural contract is **procurement-only**; other gold has no schema contract |
| **Content validity** | gold-quality baseline (CI); data-contract enums + invariants | enum/invariant contracts are **procurement-payment-facts only** |
| **Silent data loss (source vanishes)** | row-floor (~16 facts) + git tracking + R2 (bronze/silver) | SIPO gold, HSE *silver*, TED/eTenders, CSO **not floored**; **R2 excludes gold**; backup is a local weekly task (single point of failure) |

### The three systemic gaps (everything below flows from these)

1. **Liveness blind spot for manual / off-box sources.** SIPO OCR, ministerial-diary
   extract+OCR, and the planning national-silver ingest run *off-schedule, by hand*. Nothing
   beats a heartbeat for them, so a lapsed refresh is invisible until someone notices stale
   data in the app. The lane-heartbeat machinery to fix this **already exists** but is
   disarmed (`freshness_status.py` runs non-strict; 5 of 6 lanes never emit).
2. **Reachability is split-brained and partial.** `source_health` *could* probe all 122
   sources but is gated off; the endpoint check covers a *hardcoded* ~90 URLs that drift from
   the registry; the 6 manual procurement publishers fall between both. No single system owns
   "is every source still reachable."
3. **The strongest guards are procurement-only.** The row-floor, the reconciliation gate, and
   the data-contract invariants are world-class — and almost entirely scoped to the
   procurement payment facts. The other high-value, hard-to-reproduce datasets (SIPO finance,
   HSE silver, TED) rely only on the generic row/column baseline.

---

## Part B — The plan (prioritised; cheapest-highest-value first)

### P0 — Arm what is already built (days, not weeks)

- **P0.1 — Make the manual/off-box sources emit heartbeats, then turn the alarm on.**
  The single highest-leverage fix. (a) Have the SIPO OCR run, the diary extract/OCR run, and
  the planning ingest each call `tools/freshness_heartbeat.py` (or write
  `data/_meta/heartbeats/<lane>.json`) on success, with a realistic `cadence_hours`
  (e.g. SIPO 720h, diaries 2160h/quarterly, planning 720h). (b) Backfill the 5 dormant lanes
  (`live_tenders`, `money_flow`, `legal_diary_openview`, `legal_diary_docx`, `pipeline`) so
  they actually beat. (c) Flip `freshness_status.py` to `--strict` in `freshness.yml` so a
  LATE/MISSING lane opens an Issue instead of printing silently. *Result: "a source's refresh
  silently stopped" becomes a Monday/Thursday alarm instead of a someday-discovery.*

- **P0.2 — Decide a single owner for live reachability, and arm it.** Either (a) set
  `DAIL_CHECK_LINKS=1` in `source_health.yml` so the registry's 118 online sources are
  actually probed, or (b) keep the endpoint check as the owner but **derive its URL list from
  `tools/build_source_registry.build_records()`** instead of the hardcoded ~90 (so it can't
  drift). Recommendation: (b) — one list, registry-sourced, no duplication. Fold the 6 manual
  procurement publishers into whichever owns it.

- **P0.3 — Add the 4 missing high-value tables to `output_baseline.json`:** planning
  (`planning_appeal_outcomes`, `planning_applications`, `planning_decision_profiles`),
  `news_mentions`, `live_tenders`, and the TED facts (`ted_ie_*`). One
  `check_output_regressions.py --update-baseline` after confirming current counts are good.

### P1 — Extend the strongest guards to the most irreplaceable data (1–2 weeks)

- **P1.1 — Row-floor the datasets most likely to vanish next.** Add `min_rows=` to:
  - **SIPO gold facts** (`sipo_promote_to_gold.py`, `sipo_candidate_expenses_aggregate.py`) —
    *top risk*: source PDFs already 403, OCR is non-reproducible (PaddleOCR crashes this box).
  - **`hse_tusla_payments_fact.parquet` silver materialize** — the gold consolidate is floored,
    but the silver source-of-truth is not, and the upstream is permanently deleted.
  - **TED / eTenders facts** (`ted_ireland_*`, `procurement_etenders_extract.py`,
    `etenders_live_tenders_extract.py`) — live/JS/WAF-gated; a bot-challenge harvest → tiny
    frame → silent overwrite.

    This extends the *exact* protection that saved HSE to the next HSE.
- **P1.2 — Add staleness tracking for the uncovered datasets** in `check_freshness.py`
  `DATASETS`: PSA payments, attendance, interests, SIPO, diaries, planning, TED, CSO, news.
  (Row-thinning is caught only *when the chain runs*; staleness catches *the chain not running*.)
- **P1.3 — Back up the irreplaceable gold off-box.** R2 currently excludes `data/gold/`.
  Explicitly add the *non-reproducible* gold (SIPO finance, HSE rows, anything whose bronze is
  gone) to the R2 set, or confirm each is git-tracked. For these, "regenerable from bronze" is
  false — the bronze is also gone.

### P2 — Harden the backstops (when P0/P1 are stable)

- **P2.1 — Generalise the data-contract pattern** beyond procurement to 1–2 more high-value
  domains (candidate: votes, interests), or explicitly document why the generic gold-quality
  baseline is sufficient there.
- **P2.2 — De-risk the backup's single point of failure.** R2 backup is a *local Windows
  weekly task* — if the laptop is off Sunday 02:00, nothing runs. Add a backup heartbeat
  (so a missed backup alarms via the same lane ledger), or move it to a scheduled cloud job.
- **P2.3 — Tighten the row-drop tolerance** on the highest-value tables (the global 50%
  default lets a 40% silent loss pass); per-table tolerances for the irreplaceable facts.

---

## Part C — The breadth budget (the policy that keeps this from regressing)

> **A new extractor is a permanent maintenance liability, not a one-time build. Adding the
> 82nd source must clear a higher bar than the 8th did.**

**Definition of done for any new source — it is not "shipped" until it is monitored:**

1. A `data/_meta/<source>_coverage.json` (row counts, date span, known holds).
2. Either a `check_freshness.py` `DATASETS` entry **or** a lane heartbeat (so staleness alarms).
3. A reachability entry in the source registry (so the endpoint/health check probes it).
4. An `output_baseline.json` row (so row-thinning/schema-drift alarms).
5. A row-floor (`min_rows=`) **if** the fact is hard to reproduce (scrape/OCR/manual source).
6. A one-line entry in `doc/SOURCE_RECOVERY_RUNBOOK.md`'s per-source table.

If a candidate source cannot meet these, that is a signal its ongoing cost may exceed its
value. **Prefer deepening/monitoring existing sources over adding new ones.**
