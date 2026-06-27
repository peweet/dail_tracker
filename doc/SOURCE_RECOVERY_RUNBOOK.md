# Source Recovery Runbook — "source X broke, here's what to do"

**Date:** 2026-06-21 · **Audience:** future-you, or a collaborator who has never seen this break before.

This is the operational counterpart to `doc/archive/SOURCE_FRAGILITY_PLAN.md` (which covers *detection*).
This doc covers *response*: a source broke — now what. The goal is that nobody ever has to
re-derive the recovery from scratch.

**Golden rule:** the canonical facts are committed parquet + atomic writes + the row-floor.
A broken source **cannot** silently destroy floored, git-tracked data on its own — but a
*panicked manual re-run with the wrong flags can*. Slow down, read the failure class, follow
the playbook. When in doubt, **do nothing destructive**: the last good parquet is in git.

---

## 0. How you'll find out something broke

Alerts land as **GitHub Issues**, labelled by domain. State also lives in committed JSON you
can read directly:

| Signal | Where | Means |
|---|---|---|
| Issue `data`,`sources` | source-health / legal-diary health | a source is stale or its structure drifted |
| Issue `data`,`freshness` | freshness reporter | the pipeline or a dataset stopped updating |
| Issue `sources`,`link-rot` | endpoint check (`nightly.yml`, Mondays) | a PDF/API endpoint is unreachable |
| Issue `data`,`procurement` | procurement poller | an orphan publisher has a new quarter to ingest |
| Issue `data`,`lobbying` | lobbying canary | lobbying.ie has a newer period than we hold |
| CI PR failure (`sql-contracts`) | output-regressions / gold-quality `--strict` | a change thinned rows / dropped a column / introduced content rot |
| Publish aborted | `money_flow_refresh.yml` gate | a degraded run refused to ship (good — nothing bad published) |
| Pipeline halt + `quarantine/*.json` | data contract | classification/invariant drift in the procurement facts |
| `RowFloorViolation` in a run log | row-floor | a harvest came back too small; the write was refused (data is safe) |

**State files to read first** (all under `data/_meta/`): `source_health.json`,
`freshness.json`, `output_regressions.json`, `procurement_source_poll.json`,
`fetch_failures.json`, `gold_quality.json`, `heartbeats/*.json`.

---

## 1. Triage — classify the failure in 60 seconds

```
Is the source reachable at all? ───────────────► NO  → §2  Unreachable / link-rot
        │ yes
Did a harvest come back tiny/empty? (RowFloorViolation, EMPTIED) ─► YES → §3  Truncated harvest
        │ no
Is the source GONE for good (deleted, permanent 404, removed dataset)? ─► YES → §4  Permanent disappearance
        │ no
Rows present but wrong? (quality/contract fail, garbled, wrong cols) ─► YES → §5  Schema / layout drift
        │ no
Nothing failed loudly, but data is STALE? (heartbeat MISSING, freshness stale) ─► §6  Lapsed refresh
```

---

## 2. Source unreachable / link rot (404, WAF, bot-challenge, timeout)

**Symptoms:** Issue `sources`,`link-rot`; `fetch_failures.json` entries
(`http_403`/`bot_challenge`/`timeout`); poller status `UNREACHABLE`.

1. **Reproduce manually.** `curl -sSI <url>` (or a browser UA: `curl -A "Mozilla/5.0" ...`).
   Distinguish: (a) transient outage, (b) the URL moved, (c) a new WAF/JS gate, (d) the
   resource is genuinely gone (→ §4).
2. **Check the circuit breaker.** `fetch_failures.json` runs a 3-strike per-publisher breaker
   (`services/fetch_report.py`) — a tripped publisher is skipped, not failing loudly. The
   record carries `rows_in_gold` / `last_period_in_gold` so you know what you'd lose.
3. **If the URL moved:** update the source's config (the hardcoded URL lives in the extractor
   or `tools/build_source_registry.py`; for PDFs, the list in `pdf_infra/pdf_endpoint_check.py`).
   Re-run that one source with `--merge` (see §7) — never plain `--only`.
4. **If a new WAF/JS gate:** the source has become a "manual" publisher. Add it to the poller's
   manual-watch list (`tools/procurement_source_poller.py`) so it's at least flagged, and
   capture the file by hand into `data/bronze/...` for the next ingest.
5. **Do NOT** let an unreachable source trigger a destructive re-run. The existing data is
   fine; reachability ≠ data loss.

---

## 3. Truncated / empty harvest (the row-floor did its job)

**Symptoms:** `RowFloorViolation` raised; the write was **refused**, the canonical parquet is
**untouched**. Or `output_regressions.json` shows `EMPTIED` / `ROW_DROP`.

1. **Breathe — this is the system working.** No data was lost. The floor
   (`services/parquet_io.save_parquet(min_rows=)`) refused a small frame.
2. **Find why the harvest was small.** Usually an upstream change (§2/§5) or a scoped run
   (`--only X` *without* `--merge` — see §7). Check the run log and `fetch_failures.json`.
3. **Re-run correctly** once the source is fixed (§7 safe-ingest pattern). The floor will pass
   once the harvest is whole again.
4. **Only bypass the floor deliberately** — `DAIL_SKIP_ROW_FLOOR=1` — and only for an
   intentional bootstrap/scoped write where you *expect* fewer rows and have confirmed you are
   not about to wipe the canonical fact. This is the loaded-gun switch; treat it as such.
5. **If the smaller harvest is legitimately correct** (the dataset genuinely shrank), update
   the baseline: `python -m tools.check_output_regressions --update-baseline` and lower/adjust
   the `min_rows` constant in the extractor.

---

## 4. Permanent disappearance (the HSE scenario)

**Symptoms:** the source is gone for good — dataset deleted, permanent 404, not on Wayback,
not re-fetchable. **This already happened with HSE €20k payments** (deleted in HSE's 2026
rebuild; our parquet is the only surviving public copy).

**The data you already hold is now irreplaceable. The priority shifts from "refresh" to
"preserve and never overwrite."**

1. **Stop any scheduled/cron path that would re-harvest this source.** A re-run against the
   dead source can only produce garbage.
2. **Confirm the data is durable:** it must be (a) git-tracked (gold runtime slice is) AND/OR
   (b) in R2. Remember R2 holds **bronze + silver only** — if this is a gold-only artifact, it
   survives solely via git. Verify with `git ls-files <path>` and the R2 manifest
   (`data/_meta/backup_manifest.tsv`).
3. **Confirm the row-floor protects it** so no future re-run can shrink it. HSE's gold fact is
   floored at 150k; if your dead source's fact is *not* floored, add `min_rows=` now
   (see `archive/SOURCE_FRAGILITY_PLAN.md` P1.1).
4. **Repoint the dead source URL + add a caveat**, the way `tools/patch_hse_dead_source_url.py`
   did: point `source_file_url` at a live landing page and append a `source_caveat` explaining
   the source was removed and this is the surviving copy. Use that script as the template (it
   writes via `save_parquet(min_rows=current_count)` so the patch itself can't shrink the fact).
5. **Record it** in a memory note (like `project_hse_payments_only_public_copy`) and in the
   per-source table below, so nobody ever "helpfully" re-enables the dead harvest.

---

## 5. Schema / layout drift (rows present, but wrong)

**Symptoms:** CI `sql-contracts` fails on `COL_REMOVED`; `gold_quality.json` flags a 100%-null
column / dup multiplication / mojibake; the data contract halts the pipeline and writes
`data/_meta/quarantine/<name>_quarantine.parquet`; legal-diary health reports structural drift.

1. **Read the quarantine evidence.** For procurement facts, the offending rows are dumped to
   `quarantine/<name>_quarantine.parquet` **before** the halt — inspect them to see exactly
   which values went out-of-vocab (e.g. a silent `"unknown"` enum fallback) or which invariant
   broke (double-count, CRO-on-non-company, PII leak).
2. **For a PDF/HTML parser break** (a column shifted, a header is read as data): the source
   changed its layout. Fix the parser, then validate against the golden fixtures
   (`test/fixtures/...`) and the gold-quality baseline before re-promoting.
3. **The classic recurring one:** attendance sitting-day truncation (continuation-page header
   loss) has bitten **4 times**. If attendance counts look off, check the per-PDF reconcile
   guard against `data/_meta/official_sitting_days.csv` first — it's the canary for that bug.
4. **Don't `--update-baseline` to make a red contract green** unless you've confirmed the new
   shape is *correct*. The baseline is a ratchet; moving it hides the regression.

---

## 6. Lapsed refresh (nothing failed loudly, data is just stale)

**Symptoms:** Issue `data`,`freshness`; a `heartbeats/<lane>.json` is MISSING/LATE; the app
shows old "data as of" dates. Most likely for the **manual / off-box** sources.

1. **Identify the lane.** `data/_meta/heartbeats/*.json` + `freshness.json`. Registered lanes:
   `legal_diary_docx`, `legal_diary_openview`, `live_tenders`, `money_flow`, `pipeline`,
   `procurement_poller` (plus SIPO/diaries/planning once P0.1 lands).
2. **Manual sources that need a human to run something:**
   - **lobbying.ie** — manual CSV export (DevTools XHR, Playwright fallback). Drop into
     `LOBBYING_RAW_DIR`, run the lobbying chain.
   - **SIPO election finance** — off-box GPU OCR (PaddleOCR **crashes the local Windows box** —
     never run locally). Run off-box, copy silver back, promote to gold.
   - **Ministerial diaries** — gov.ie WAF scrape + off-box OCR. Same off-box pattern.
   - **Charities / SEAI / Tusla** — manual download (JS/WAF). Capture to bronze, ingest.
3. **Run the refresh, confirm the heartbeat updates**, confirm `freshness.json` goes green.
4. **If a lane is chronically late**, reconsider its `cadence_hours` (don't alarm on a source
   that's genuinely quarterly as if it were weekly).

---

## 7. Recovery primitives (the exact mechanics)

- **Safe re-ingest of one publisher** — `--only <X> --merge`. Plain `--only X` **replaces the
  whole fact with just X** (the floor will usually catch it, but don't rely on that). `--merge`
  folds X back into the existing fact (`procurement_public_body_extract.py`,
  `procurement_la_payments_extract.py`).
- **Row-floor bypass** — `DAIL_SKIP_ROW_FLOOR=1` env var. Deliberate, scoped writes only.
- **Restore a single good parquet from git** — `git checkout HEAD -- <path/to.parquet>`
  (or an earlier commit). The fastest "undo a bad write" for a git-tracked fact.
- **Restore from R2** (bronze/silver only) — see `doc/DISASTER_RECOVERY.md`; `rclone` pull,
  then re-verify with `tools/data_manifest.py --check` (re-hashes against
  `backup_manifest.tsv`). R2 is append-only (`--ignore-existing`), so prior captures survive.
- **Update a baseline after an intended change** —
  `python -m tools.check_output_regressions --update-baseline` (row/column baseline);
  the gold-quality baseline has the equivalent. Only after confirming the change is correct.
- **Reconciliation gate** (procurement) — if the consolidate aborts with a reconciliation
  error, gold failed to preserve a source fact's rows/€ exactly; the bug is in the
  silver→gold fold (concat/dedup/join), not the source.
- **Re-seed `held_through`** — after re-ingesting an orphan procurement publisher, bump its
  `held_through` in `tools/procurement_source_poller.py` or the poller will keep alarming.

---

## 8. Per-source quick reference

Fragility: **H**igh (scrape/OCR/manual/hardcoded-URL), **M**edium, **L**ow (stable API).
"Floored?" = protected by `min_rows`. "Off-box?" = needs a human / GPU off this machine.

| Source family | Type | Frag. | Floored? | Backup | If it breaks |
|---|---|---|---|---|---|
| Oireachtas API (members/votes/questions/bills) | API | L | no | git | refetch; schema-canary in endpoint check |
| Attendance PDFs | PDF | **H** | no | git+R2(silver) | §5 — check sitting-day reconcile guard first |
| PSA payments / interests PDFs | PDF | M | no | git+R2 | §5; hardcoded URL list in endpoint check |
| lobbying.ie | manual CSV | **H** | no | git+R2 | §6 — manual export to `LOBBYING_RAW_DIR` |
| eTenders / OGP awards | open-data CSV | L–M | **no → add (P1.1)** | git+R2 | §3/§5; CKAN canary in endpoint check |
| eTenders LIVE tenders | Playwright scrape | **H** | no | git | §2 WAF/JS; own GH Action `live_tenders_refresh` |
| TED EU | API | L | **no → add (P1.1)** | git+R2(silver) | §5; winner-history 0% pre-2024 is expected |
| CRO bulk register | CKAN zip | L–M | bronze floor (700k) | R2 | source-health file_age (live, 7d) |
| Charities register | manual XLSX | M | no | R2 | §6 manual drop; source-health file_age (180d) |
| **SIPO election finance** | **OCR off-box** | **H** | **no → add (P1.1, top)** | R2(silver) only | §6 off-box OCR; source PDFs already 403 — **irreplaceable** |
| Ministerial diaries | OCR + WAF scrape | **H** | gold floored | git+R2 | §6 off-box; extract is manual |
| Planning applications (national silver) | bulk static | M | no | R2 | §6 — standalone ingest, not in pipeline |
| Planning appeal outcomes (ArcGIS) | ArcGIS | **H** | yes (10k) | R2 | §5 ArcGIS schema drift; **add to baseline (P0.3)** |
| LA payments (31 councils) | portal+PDF | **H** | yes (60k) | git+R2 | §2 bot-walls; §7 `--merge` per council |
| LA AFS (Camelot) | PDF | **H** | NOAC/derelict floored | R2 | §5 Camelot layout drift |
| Public-body payments (depts/semi-states) | portal+PDF | **H** | yes (60k) | git+R2 | §2 `fetch_failures` circuit-breaker; §7 `--merge` |
| **HSE/Tusla payments** | OCR/FOI PDF | **H** | gold yes (150k), **silver no** | git+R2(silver) | **§4 — source DELETED, only public copy. Never re-harvest.** |
| NPHDB / NTA / SEAI | hardcoded-URL PDF | **H** | NTA/NPHDB yes | R2 | §2/§6; poller heartbeat; bump `held_through` after ingest |
| Iris Oifigiúil | bulk PDF poller | M | no | git+R2 | §5; freshness(iris) covers staleness |
| CSO PxStat | REST API | L | **no → add (P1.1)** | git | §5; **add to freshness (P1.2)** |
| Judiciary (sandbox) | one-off PDF | M | no | R2 | static 2026-06-04; sandbox, degrades gracefully |
| Legal Diary (.docx + OpenView) | portal scrape | **H** | no | git+R2 | §2/§5; `legal_diary_openview_health` canary; missed day is lost |
| Stateboards register | HTML scrape | M | no | git+R2 | source-health file_age + freshness |
| NOAC / derelict sites | PDF/XLSX | L–M | yes | git+R2 | §5; freshness covers staleness |
| News mentions (RSS) | RSS/scrape | M | no | git | §6 publisher-pattern break; **not in baseline (P0.3)** |
| Hand-curated CSVs (`data/_meta`) | manual | L | n/a | git | identity refs; edit by hand, no harvest |

> Keep this table in sync as sources are added/retired — it is the breadth-budget checklist
> (`archive/SOURCE_FRAGILITY_PLAN.md` Part C, item 6). A source with no row in this table is a source
> nobody knows how to recover.
