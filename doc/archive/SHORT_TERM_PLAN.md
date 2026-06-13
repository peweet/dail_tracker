# Short-term plan — full-time pacing, 8 weeks

_The fridge document. Refer to this when starting a session; ignore everything else unless this points you at it._

**Goal:** in the next 8 weeks of full-time work, get from "well-architected hobby project" to "publicly deployed civic-data tool with one named journalist using it, plus the deferred robustness items in place." Two phases:

- **Phase 1 (Weeks 1–4):** ship the minimum viable alpha. Same scope as the original 8-week evening plan, compressed to 4 weeks at full-time pace.
- **Phase 2 (Weeks 5–8):** the items previously deferred. Discovery probe, freshness SLOs, lobbying.ie auto-export, optionally one new dataset. First-user iteration runs throughout.

This plan is the distilled action sequence from `dail_tracker_improvements_v4.md`. Use that doc when this one points you to it; otherwise, work directly from this list.

---

## Pace and burnout (read this before Day 1)

You have full-time availability. That doesn't mean 40 productive hours per week. Realistic pace:

- **5–6 focused hours/day, 5 days/week ≈ 25–30 productive hours.** Past that, error rate goes up, decisions get worse.
- **One full day per week reserved as buffer.** Things will go wrong: a Streamlit Cloud config quirk, a GitHub Actions auth issue, a fixture that's harder to extract than expected. Without buffer, the schedule slips and you compensate by working harder, which compounds the slip.
- **One day per week off, fully.** Solo full-time work on a hobby project burns people out around weeks 4–6. Plan to not work weekends; it makes the 8 weeks sustainable.
- **Don't use the freed-up time to expand scope.** The temptation will be huge: "I have time, why not also add X?" Resist. The Tier D / "actively don't do" items in the prioritisation are still off-limits during Phase 1.

If you discover Day 5 of Week 1 that the work is going faster than planned, **don't pull Phase 2 forward.** Bank the buffer. Use it for testing more carefully, writing better commit messages, or stopping early. Compressed schedules that succeed always have buffer; ones that fail consumed it.

---

## Operating rules for the whole window

1. **Don't add new datasets during Phase 1.** SIPO, judicial, CRO — all parked until Week 7+ at earliest. Adding sources before the existing four are operationally sound is a known failure mode (see `pipeline_sandbox/learnings_from_civic_data_projects.md` §EveryPolitician).
2. **Don't refactor for elegance.** No dim/fact split, no UI redesign during Phase 1. If it's not in this plan, defer it.
3. **Don't perfect anything before the alpha is live.** Ship at "good enough"; iterate after a real user touches it.
4. **One PR per item.** Even solo, PRs create a review surface for tomorrow-you and let CI run before merge.
5. **If you're stuck for >3 hours on one item, skip it and come back.** Order matters less than momentum.
6. **Daily commit floor: at least one PR merged or one substantive WIP commit.** Sustains visible progress.

---

# Phase 1 — Minimum viable alpha (Weeks 1–4)

End-of-Phase-1 definition of done: a public Streamlit Community Cloud URL exists, refreshes itself on a cron, alerts you when it breaks, has tests that catch parser drift, validates upstream API schema on every fetch, has a versioned data release, has a journalist-readable methodology doc, and has been sent to one named journalist with a personal introduction.

---

## Week 1 — Ship-ready (Days 1–5)

Goal at end of week 1: the public URL exists, with provenance and freshness on three pages.

### Day 1 — Refresh CLI + manifest writer

- [ ] **1.1 Add `pipeline.py --refresh` flag** (~3 hrs). New CLI flag that runs the full pipeline against live upstream sources without resetting bronze. Done when `python pipeline.py --refresh` runs end-to-end locally and produces silver+gold identical to the manual sequence. Reference: v4 §3.3.
- [ ] **1.2 Write `utility/tools/write_run_manifest.py`** (~2 hrs). Emits per-mart `*.manifest.json` beside each gold parquet (fields per v4 §3.5). Done when running it after a pipeline run produces one manifest per gold mart.

### Day 2 — Cron + branch

- [ ] **1.3 Add GitHub Actions refresh workflow** (~2 hrs). `.github/workflows/refresh.yml` from v4 §3.3 with monthly cron + workflow_dispatch. Done when manual trigger from Actions UI runs the pipeline and pushes parquet to the `data` branch. Watch: needs `permissions: contents: write`.
- [ ] **1.4 Stand up `data` branch** (~30 min). Orphan branch holding only `data/gold/parquet/*.parquet` + manifests. Done when branch exists, contains current parquet, and 1.3 commits to it on success.
- [ ] **1.5 Manual integration test** (~2 hrs). Trigger the workflow manually, verify the data branch updates, verify Streamlit Cloud (next day) will be able to read it.

### Day 3 — Public deploy

- [ ] **1.6 Streamlit Community Cloud deploy** (~3 hrs incl. config debugging). Create the app, point at `main` for code, configure to read from `data` branch. Done when a public URL exists that loads today's data. Watch: Cloud sometimes needs `requirements.txt` explicitly pointed to.
- [ ] **1.7 Smoke-test the public URL** (~1 hr). Click through every page. Anything broken → note for later, don't fix in flight unless catastrophic.

### Day 4 — Provenance + freshness

- [ ] **1.8 Provenance helper** (~3 hrs). `render_provenance(manifest_path)` that reads the manifest from 1.2 and renders an expander. Reference: v4 §9.1.
- [ ] **1.9 Freshness badge helper** (~2 hrs). Top-of-page pill with green/amber/red based on `last_refresh`. Reference: v4 §9.2.

### Day 5 — Wire the helpers + buffer

- [ ] **1.10 Wire helpers on three pages** (~3 hrs). Pick the three most-used pages (member overview, payments, lobbying). Don't wire all eight yet.
- [ ] **Buffer / iteration** (~3 hrs). Anything that slipped from days 1–4 lands here.

**Week 1 definition of done:** public URL works, refreshes itself, three pages show real provenance and a freshness badge.

---

## Week 2 — Alarm + parser tests (Days 6–10)

Goal at end of week 2: when something breaks, you find out within hours. Parsers can't drift silently.

### Day 6 — Notification stack

- [ ] **2.1 Tier 1 GitHub Issues** (~1 hr). `if: failure()` step that opens a labelled issue with run ID + logs URL. Done when deliberately breaking the workflow (add `exit 1` temporarily) creates an issue. Reference: v4 §4.7.
- [ ] **2.2 Tier 2 Healthchecks.io** (~2 hrs). Sign up anonymous tier; one ping URL per source (4 total); add `curl -fsS https://hc-ping.com/<uuid>` at start+end of each refresh. Done when Healthchecks dashboard shows pings. Watch: store UUIDs in GitHub repo secrets.
- [ ] **2.3 Test the alarm chain end-to-end** (~2 hrs). Force a failure deliberately. Verify: issue opens, Healthchecks ping is missed on next run, you receive notification. **This validation step is not optional** — untested alarms might as well not exist.

### Day 7 — Payments golden file

- [ ] **2.4 Payments parser golden file** (~5 hrs). Commit one PDF (e.g. `2026-04-02_..._february-2026.pdf`) to `test/fixtures/payments/`. Run parser, save expected output as `.expected.parquet`. Write `test_payment_parser_golden.py` that re-runs and asserts equality. Most time is verifying the expected output is actually correct. Reference: v4 §5.2 + HANDS_OFF Phase B.

### Day 8 — Attendance golden file

- [ ] **2.5 Attendance parser golden file** (~5 hrs). Same pattern. Pick `pdf_2026` from `pdf_endpoint_check.py`.

### Day 9 — Interests golden file

- [ ] **2.6 Interests parser golden file** (~5 hrs). Same pattern. Pick `dail_member_interests_2025`. Watch: interests PDF is large, may need to trim to fixture-sized subset.

### Day 10 — Drift assertions + buffer

- [ ] **2.7 Row-count drift assertions** (~3 hrs). Wrap each silver write step with `check_drift(current_count, history)`. Hard-fail if delta > 15% from rolling-5-run average. Store history in `data/_meta/row_count_history/<dataset>.json`. Reference: v4 §5.3.
- [ ] **2.8 Synthetic drift test** (~1 hr). Inject a 30% drop, verify it fails the next refresh.
- [ ] **Buffer** (~2 hrs).

**Week 2 definition of done:** alarms wired and *tested*; three golden-file tests in CI; a 30% row drop fails the next refresh.

---

## Week 3 — API protection + vote fix + public-safe (Days 11–15)

Goal at end of week 3: upstream Oireachtas API changes can't silently corrupt data; vote history is no longer truncated; deploy is safe to share more widely.

### Day 11 — API schema validation

- [ ] **3.1 JSON schemas for Oireachtas API endpoints** (~4 hrs). For each endpoint the pipeline calls (members, legislation, debates, votes), commit a JSON Schema to `pipeline/schemas/<endpoint>.json` describing top-level shape + the fields actually read. Reference: v4 §6.5 + HANDS_OFF Phase C. Don't schema-validate every nested field — too brittle.
- [ ] **3.2 Validate-at-fetch wiring** (~2 hrs). Each fetch calls `jsonschema.validate(response, schema)` immediately. Synthetic test with manipulated sample triggers the validation error.

### Day 12 — Vote pagination Phase 1

- [ ] **3.3 Build paginated fetcher in sandbox** (~5 hrs). Implementation per `pipeline_sandbox/votes_pagination_plan.md` Phase 1. Sandbox module that loops with `skip` and `limit`, asserts on `head.totalResults`. Don't touch production yet.

### Day 13 — Vote pagination Phase 2 + 3

- [ ] **3.4 Side-by-side compare** (~3 hrs). Run old fetcher and new fetcher in parallel; compare outputs. Phase 2 acceptance criterion is non-negotiable.
- [ ] **3.5 Switch over** (~2 hrs). Single PR replacing the production call. Keep old code commented out for one cycle.

### Day 14 — Dependency hygiene + CI

- [ ] **3.6 Pin dependencies + lockfile** (~2 hrs). Add version specifiers to `pyproject.toml`, commit `uv.lock` (or `pip-compile` equivalent), add `pip-audit` to CI.
- [ ] **3.7 Minimum viable CI** (~3 hrs). `.github/workflows/ci.yml` with ruff, ruff-format, pytest, page-import smoke. Don't add live-network or slow e2e tests yet.

### Day 15 — Pipeline rearchitecture starts

- [ ] **3.8 Single HTTP helper** (~5 hrs). `pipeline/sources/_http.py` that owns User-Agent, conditional GET, throttle, jitter, robots.txt check, retry, timeouts. Reference: v4 §4.3. This is the foundation for the consolidation in Week 4.

**Week 3 definition of done:** API validation fires on each fetch; vote count matches API total; locked deps; CI green.

---

## Week 4 — Rearchitecture + citable + journalist (Days 16–20)

Goal at end of week 4: scraping code is in one place behind the polite HTTP helper; versioned data releases work; methodology doc readable in 10 minutes; one journalist has the URL.

### Day 16 — Consolidate scraping code

- [ ] **4.1 Move scraping into `pipeline/sources/`** (~6 hrs). Each source gets its own file (`payments.py`, `attendance.py`, etc.) using the `_http.py` helper. Reference: v4 §4.2.

### Day 17 — Finish rearchitecture + audit

- [ ] **4.2 Audit all imports in `utility/`** (~2 hrs). Confirm zero references to scraping/fetching code. Refactor any violators.
- [ ] **4.3 Update README** (~1 hr). Add the explicit "do not run scrapers from forks; pull the published artefact" notice.
- [ ] **4.4 Verify polite-bot behaviour** (~2 hrs). Trigger a refresh, inspect the request logs: User-Agent set, conditional GET firing, robots.txt checked, throttle respected.

### Day 18 — Versioned releases

- [ ] **4.5 Versioned data releases** (~4 hrs). Workflow step that, after each successful refresh, tags a release (`data-v2026.05.07`) and uploads parquet artefacts. Update Streamlit Cloud to point at the latest release rather than `data` branch (more reproducible).
- [ ] **4.6 Tag the first release** (~30 min). Verify it appears in GitHub Releases and the dashboard reads from it.

### Day 19 — Methodology doc

- [ ] **4.7 `methodology.md` first draft** (~5 hrs). One page per dataset describing what the numbers mean, what they don't, known caveats, worked example of a single record from source to chart. Reference: v4 §13.1. Aim for "journalist who has 10 minutes can read it."

### Day 20 — First user

- [ ] **4.8 Identify the journalist target** (~2 hrs research). One specific person at one of: Right To Know, The Journal Investigates, Noteworthy, Story.ie, an Irish Times investigative reporter, or a politics academic at TCD/UCD/DCU. One name, email, one-paragraph reason this person specifically would benefit.
- [ ] **4.9 Send the introduction** (~2 hrs). Email containing: project URL, one-paragraph pitch, link to methodology.md, one specific suggestion of a question they could investigate, your contact for issues. Don't oversell; acknowledge it's alpha.

**Week 4 definition of done:** polite-bot scraping consolidated; versioned release tagged; methodology readable; journalist contacted. **Phase 1 complete.**

---

# Phase 2 — Deferred items + first-user iteration (Weeks 5–8)

Goal: the items previously deferred during evening-pace planning. Discovery probe, freshness SLOs, lobbying.ie auto-export, optionally one new dataset. First-user feedback drives priority within the phase.

---

## Week 5 — Buffer + discovery probe (Days 21–25)

Goal at end of week 5: discovery probe builds new-PDF detection from manual to automatic.

### Day 21 — First-user iteration buffer

- [ ] **5.1 Iterate on journalist feedback** (~5 hrs). Whatever they reported by now becomes the priority. Skip the rest of the week's plan if necessary. If no response yet, send a one-line follow-up, then proceed to 5.2.

### Day 22 — Discovery probe Strategy 1

- [ ] **5.2 Resolve the index page 403** (~3 hrs). The `oireachtas.ie/en/publications/` topic-filtered index returned 403 to automated requests during URL verification. Investigate: User-Agent? Cookie? Session warm-up? Reference: `pipeline_sandbox/payment_pdf_discovery_notes.md`.
- [ ] **5.3 Index-parsing implementation** (~3 hrs). Given the topic-filtered HTML, extract publication entries with title + URL. Test against committed fixture HTML.

### Day 23 — Discovery probe Strategy 2 + 3

- [ ] **5.4 HEAD-spread fallback** (~3 hrs). Already partially in `pipeline_sandbox/payment_pdf_url_probe.py`; verify it works against live `data.oireachtas.ie` for known-good URLs.
- [ ] **5.5 Wider lag-window fallback** (~2 hrs). Tier 2 spread covering the 25–60 day window for outliers.
- [ ] **5.6 Diagnostic-failure handling** (~1 hr). Distinguish "within expected window, not yet published" from "past expected window, pattern probably broke."

### Day 24 — Discovery probe tests (HANDS_OFF Phase A)

- [ ] **5.7 Construction tests** (~2 hrs). Offline tests that verify candidate URLs include known historical URLs.
- [ ] **5.8 Index-parsing tests** (~2 hrs). Fixture-based tests against committed HTML snapshot.
- [ ] **5.9 Mocked-HTTP orchestration tests** (~2 hrs). Use `responses` or `requests-mock`; verify the strategy ordering works.

### Day 25 — Validation moment

- [ ] **5.10 Run probe against March 2026 PDF** (~1 hr). The success criterion. Probe should return the known March 2026 URL.
- [ ] **5.11 Wire weekly live canary** (~2 hrs). `@pytest.mark.live` test scheduled weekly, hits live Oireachtas, confirms probe still works. Always-emails-success week-by-week confirms the system is alive.
- [ ] **Buffer** (~3 hrs).

**Week 5 definition of done:** probe finds March 2026 PDF; tests pass in CI; weekly live canary scheduled.

---

## Week 6 — Freshness SLOs + lobbying auto-export (Days 26–30)

Goal at end of week 6: pipeline detects silent stalls; lobbying.ie ingestion is no longer manual.

### Day 26 — Freshness SLOs (HANDS_OFF Phase E)

- [ ] **6.1 Freshness state file** (~2 hrs). `data/_meta/source_freshness.json` per source: last_new_asset_at, expected_cadence_days, warn_after_days, fail_after_days. Reference: HANDS_OFF Phase E.
- [ ] **6.2 Daily SLO check job** (~3 hrs). Scheduled workflow that reads the state file, warns or errors based on age vs thresholds. Errors open issues per Tier 1.

### Day 27 — End-to-end smoke (HANDS_OFF Phase F)

- [ ] **6.3 Bronze fixture snapshot** (~3 hrs). Capture a small representative bronze snapshot under `test/fixtures/e2e/bronze/`.
- [ ] **6.4 E2E test** (~3 hrs). Run pipeline against fixture, assert gold output matches expected. Add to nightly CI (not per-PR — too slow).

### Day 28 — Lobbying.ie auto-export (start)

- [ ] **6.5 Investigate lobbying.ie XHR endpoints** (~2 hrs). DevTools network tab during a manual export. Memory note from earlier conversation: "check DevTools for XHR endpoint first". If a clean endpoint exists, use it; otherwise plan Playwright.
- [ ] **6.6 Implement the export job** (~4 hrs). Either direct XHR call or Playwright script. Output: CSV files in `data/bronze/lobbying/` matching the manual format.

### Day 29 — Lobbying.ie auto-export (finish + integrate)

- [ ] **6.7 Wire into refresh workflow** (~3 hrs). New step in the cron that runs the export job. Mark as "best-effort" — failure of this source must not block others.
- [ ] **6.8 Validate against last manual export** (~2 hrs). Compare auto-exported CSV to the most recent manual one. Identical (modulo timestamp) means it works.

### Day 30 — Iteration + buffer

- [ ] **6.9 Second journalist follow-up** (~1 hr). If first journalist hasn't responded by Day 30 (~10 days since contact), send a polite ping. If they've responded, deal with whatever they raised.
- [ ] **Buffer** (~5 hrs).

**Week 6 definition of done:** freshness SLOs alerting; lobbying.ie auto-imports; e2e test in nightly CI.

---

## Week 7 — Choose your own adventure (Days 31–35)

Goal: depending on first-user feedback and Phase 2 progress, pick **one** track for this week. Don't try all three.

### Track A — One new dataset (recommended if journalist asked for it)

- [ ] **7A.1–5 Add SIPO donations OR judicial appointments OR Iris Oifigiúil** (~5 days). Whichever the journalist most asked for. Reference: `ENRICHMENTS.md` §A.1 (SIPO) or §D.1 (judicial appointments) or `pipeline_sandbox/iris_oifigiuil_probe_findings.md` (Iris). **This is also the trigger to refactor into the v4 §4.6 pluggable scraper interface** — three implementations is enough to validate the abstraction.

  **Iris Oifigiúil scope decision** if Track A goes to Iris: do v1 only — the Member-Interest Supplement extractor + the SI metadata extractor (two CSV outputs). Skip the other 6 value categories (foreshore, tax defaulters, state-board appointments, pension events, charity/coop events, regulated-entity attrition feed) and leave them for Phase 3+. Rationale: the Member-Interest Supplement is the unique-to-Iris find that no other source carries; the SI metadata layer is what eISB doesn't expose. Everything else is value-additive but not differentiated. Probes already complete; downloader scaffolding exists at `iris_oifiiguil.py`. Estimated 5 days for v1 = parser (~1 day, fitz + pandas + hybrid splitter), member-resolver against `silver/flattened_members.csv` (~1 day), Streamlit member-interests page (~1 day), Streamlit SI directory page (~1 day), tests + provenance + manifest wiring (~1 day).

### Track B — Pluggable scraper interface refactor

- [ ] **7B.1–5 Refactor sources to pluggable shape** (~5 days). The interface from v4 §4.6: `discover() / fetch() / parse()` per source. Don't do this if Track A is happening; the new dataset is a better trigger for the refactor.

### Track C — Trust hardening (recommended if no journalist response yet)

- [ ] **7C.1 Per-page caveat banners** (~2 days). v4 §13.3. Where DATA_LIMITATIONS flags a known issue, render an inline banner.
- [ ] **7C.2 Update history per dataset** (~1 day). v4 §13.4. Each gold mart's page shows last 3 refresh events.
- [ ] **7C.3 Public CHANGELOG.md** (~1 day). v4 §13.5. Keep-a-Changelog format.
- [ ] **7C.4 Methodology external review** (~1 day). v4 §13.6. Hand methodology.md to one independent reader (not a journalist — a friend who knows nothing about the project). Their first 10 questions are gold.

---

## Week 8 — Polish + handover prep (Days 36–40)

Goal: the project survives you stepping back for 1–4 weeks without rotting.

### Day 36 — Refresh calendar

- [ ] **8.1 `doc/refresh_calendar.md`** (~3 hrs). v4 §15.2. Per source: when it refreshes, what the typical lag is, who at the source to contact if it breaks.
- [ ] **8.2 Update ENRICHMENTS.md** (~2 hrs). Resolve the parked URL verification list — at minimum the broken URLs you have replacements for. Or explicitly mark "still parked" if not.

### Day 37 — Handover note

- [ ] **8.3 `doc/handover.md`** (~5 hrs). v4 §15.4. The document a new maintainer reads if you stopped tomorrow: secrets and access locations, GitHub Actions schedule, known fragile parsers, open issues by priority, "start here" sequence for the first day.

### Day 38 — Monthly clean-rebuild dry run

- [ ] **8.4 Clean-room rebuild** (~5 hrs). v4 §15.5. Blow away local data, clone fresh, run `make bootstrap` (or equivalent), run `make refresh`. Anything that breaks is a sustainability bug — fix it now.

### Day 39 — Public methodology + CHANGELOG (if not done in Week 7C)

- [ ] **8.5** Anything from Track C in Week 7 that didn't ship.

### Day 40 — Iteration + reflection

- [ ] **8.6 Final journalist iteration** (~2 hrs). Whatever's outstanding from the user.
- [ ] **8.7 Plan next 8 weeks** (~3 hrs). Read v4 §18 Phase 2. Decide what's next based on what you learned. Update this document or write a new one.

**Week 8 definition of done:** project documented well enough for someone else to maintain; clean-room rebuild works; you know what's next.

---

## Decision rules

### When to deviate from this plan

- **Always:** if a critical bug breaks production, fix that first regardless of what week says.
- **Often (Phase 2):** if the journalist gives concrete feedback, prioritise it over the planned item. Their first three suggestions determine whether they come back.
- **Sometimes (Phase 1):** if a dependency is unexpectedly hard (e.g. Streamlit Cloud config gnarliness), spend up to one extra day on it. After that, raise as a known issue and skip ahead.
- **Rarely:** because something simpler emerges — but only if it's *actually* simpler, not "more elegant".
- **Never:** because something on the deferred list (Tier C/D in the prioritisation) suddenly seems interesting. Those are deferred for a reason.

### When to stop and reassess

- **End of Week 2 and you don't have alarms wired:** something fundamental is wrong with the development environment. Fix that before continuing.
- **End of Week 4 and the alpha isn't public:** the original 4-week timing was ambitious; reassess scope rather than pushing harder.
- **End of Week 6 and no journalist response:** consider widening the contact list (3 names, not 1) and accept that Phase 3 will be more about hardening than user feedback.

### When to pause work entirely

- **Three consecutive days of low-quality work** (visible in: things you wrote breaking, decisions you'd reverse the next morning, mounting frustration). Take a full day off.
- **End of Week 4 mood check.** Phase 1 is the hardest stretch. If you're depleted, take the weekend at minimum, ideally 3–5 days.
- **End of Week 6.** Phase 2 burnout is the more likely failure mode than Phase 1 burnout. Schedule downtime.

---

## What this plan deliberately doesn't include

These are good ideas that come *after* the 8-week window. Don't pull them forward.

- **More than one new dataset.** Even with full-time availability, adding multiple datasets in 8 weeks is the EveryPolitician failure mode at speed.
- **UI redesign.** v4 §9 (UI maturity items: cross-page nav, search, mobile, accessibility, onboarding). Defer until Phase 3.
- **Dim/fact/bridge data modelling refactor** (v4 §7).
- **Pre-2020 backfill.** Parser work, different project.
- **Tier 3 ntfy.sh push notifications.** Tier 1 + Tier 2 cover it.
- **Custom React frontend, backend API service, heavyweight orchestrator** (v4 §16.2).
- **Methodology review by a journalist** (different from Track C external review). Comes after they've used the tool, not before.

When the 8 weeks are done, open `dail_tracker_improvements_v4.md` §18 Phase 2 and pick the next thing.

---

## Quick checklist (the actual fridge version)

Copy this into a sticky note. Tick as you go.

```text
PHASE 1 — Minimum viable alpha

WEEK 1 — Ship
[ ] Day 1: pipeline.py --refresh + manifest writer
[ ] Day 2: GitHub Actions + data branch
[ ] Day 3: Streamlit Cloud deploy
[ ] Day 4: Provenance + freshness helpers
[ ] Day 5: Wire on 3 pages + buffer

WEEK 2 — Alarm + parser tests
[ ] Day 6: Tier 1 + Tier 2 notifications + test alarm chain
[ ] Day 7: Payments golden file
[ ] Day 8: Attendance golden file
[ ] Day 9: Interests golden file
[ ] Day 10: Row-count drift assertions

WEEK 3 — API + vote + public-safe
[ ] Day 11: API JSON schemas + validate-at-fetch
[ ] Day 12: Vote pagination Phase 1
[ ] Day 13: Vote pagination Phase 2 + 3
[ ] Day 14: Pin deps + CI
[ ] Day 15: HTTP helper

WEEK 4 — Rearch + citable + journalist
[ ] Day 16: Consolidate scraping into pipeline/sources/
[ ] Day 17: Audit + README warning + verify polite-bot
[ ] Day 18: Versioned releases
[ ] Day 19: methodology.md draft
[ ] Day 20: Identify + email journalist

PHASE 2 — Deferred items + iteration

WEEK 5 — Buffer + discovery probe
[ ] Day 21: First-user iteration buffer
[ ] Day 22: Probe Strategy 1 (index)
[ ] Day 23: Probe Strategy 2 + 3
[ ] Day 24: Probe tests
[ ] Day 25: March 2026 validation + live canary

WEEK 6 — Freshness + lobbying auto-export
[ ] Day 26: Freshness SLOs
[ ] Day 27: E2E smoke test
[ ] Day 28: Lobbying.ie auto-export start
[ ] Day 29: Lobbying.ie auto-export finish + integrate
[ ] Day 30: Iteration + buffer

WEEK 7 — Choose one track
[ ] Track A: Add SIPO donations OR judicial (if journalist asked)
[ ] Track B: Pluggable scraper refactor
[ ] Track C: Trust hardening (if no journalist response)

WEEK 8 — Handover-ready
[ ] Day 36: Refresh calendar + ENRICHMENTS cleanup
[ ] Day 37: Handover note
[ ] Day 38: Clean-room rebuild dry run
[ ] Day 39: Track C overflow
[ ] Day 40: Final iteration + plan next 8 weeks
```

---

## Sustainability checks (read at end of Weeks 2, 4, 6, 8)

Quick gut-check questions:

- **Am I sleeping properly?** If no, you're working too hard. Cut hours.
- **Am I enjoying this?** If not for >3 days, take a break.
- **Am I making decisions I'd reverse the next morning?** That's a tiredness signal.
- **Have I taken a full day off in the last 7 days?** If no, take one tomorrow.
- **Is the next item still motivating?** If not, you may have picked the wrong next item.

Solo full-time on a hobby project is unusual and harder than people expect. The plan assumes you'll do this sustainably, not heroically.

---

## Cross-references (for when this plan points you elsewhere)

- Architectural detail and rationale: `dail_tracker_improvements_v4.md`
- Future dataset ideas (deliberately out of scope here): `ENRICHMENTS.md`
- Extended testing strategy: `test/HANDS_OFF_TEST_PLAN.md`
- Existing test infrastructure: `test/TEST_SUITE.md`
- Data caveats (cite this in methodology.md): `doc/DATA_LIMITATIONS.md`
- Vote pagination implementation: `pipeline_sandbox/votes_pagination_plan.md`
- PDF discovery future work: `pipeline_sandbox/payment_pdf_discovery_notes.md`
- Civic-data project learnings: `pipeline_sandbox/learnings_from_civic_data_projects.md`
