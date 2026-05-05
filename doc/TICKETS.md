# Backlog tickets — Plateau 1 (full) + Plateau 2 (epic level)

_Jira-style tickets derived from `SHORT_TERM_PLAN.md` and `dail_tracker_improvements_v4.md`. Each ticket is self-contained and includes a helper prompt ready to paste into a Claude session._

## How to use this doc

1. **Pick the next ticket in dependency order.** Don't skip ahead unless the deferred discipline is broken.
2. **Read the references first.** The helper prompt assumes you've checked the relevant v4 / sandbox / test docs.
3. **Paste the helper prompt into a fresh Claude session** with the project context. Replace any `[bracketed]` placeholders with project specifics.
4. **Review the diff before merging.** v4 §17.4 — diff review discipline. Especially when Claude seems confident.
5. **Tick the acceptance criteria.** A ticket isn't done until every criterion is met.
6. **One PR per ticket.** Even solo. CI runs on PR, not on direct main commits.

## Ticket key

- **Ticket ID:** `DAIL-NNN` (sequential, no meaning beyond ordering).
- **Estimate:** focused work hours.
- **Priority:** P0 = blocks alpha; P1 = needed for plateau; P2 = nice-to-have within plateau; P3 = deferrable across plateau boundaries.
- **Labels:** phase-1 / phase-2 / plateau-2; subsystem (refresh / tests / ui / ops / etc.).
- **Dependencies:** ticket IDs that must be complete first.

---

# Phase 1 — Minimum viable alpha (Weeks 1–4, ~106 hours)

## Week 1 — Ship-ready

---

### DAIL-001 — Add `pipeline.py --refresh` CLI flag

- **Estimate:** 3h · **Priority:** P0 · **Dependencies:** none · **Labels:** phase-1, refresh

#### Description
Add a `--refresh` command-line flag to `pipeline.py` that runs the full pipeline against live upstream sources without resetting bronze or running dev fixtures. This is the foundational change that allows the GitHub Actions workflow (DAIL-003) to invoke the pipeline cleanly.

#### Acceptance criteria
- [ ] `python pipeline.py --refresh` runs end-to-end against live sources.
- [ ] Output silver+gold parquet is identical to the manual sequence.
- [ ] Existing default behaviour (no flag) is unchanged.
- [ ] Flag is documented in `python pipeline.py --help`.

#### References
- v4 §3.3 (auto-refresh GitHub Actions skeleton)
- existing `pipeline.py` orchestration

#### Helper prompt
```
Read pipeline.py and identify the orchestration entry point. Add a --refresh CLI flag using argparse that runs the full pipeline against live upstream sources but skips:
- bronze cleanup steps
- any dev-fixture loading paths
- interactive prompts

The flag should be off by default (existing behaviour preserved). When set, log a one-line "REFRESH MODE" banner at start.

Don't modify normalise_join_key.py, enrich.py, or any silver/gold writers. The change should be confined to pipeline.py and its argparse setup.

Acceptance test: python pipeline.py --refresh runs end-to-end and produces parquet files in data/silver/parquet/ and data/gold/parquet/ identical (modulo timestamps) to the manual sequence.

Reference: doc/dail_tracker_improvements_v4.md §3.3.
```

#### What NOT to do
- Don't change behaviour of any pipeline stages without the flag.
- Don't add new orchestration functions; reuse what's there.
- Don't touch silver/gold writer code.

---

### DAIL-002 — Per-mart manifest writer

- **Estimate:** 2h · **Priority:** P0 · **Dependencies:** none · **Labels:** phase-1, ops

#### Description
Create `utility/tools/write_run_manifest.py` that writes a `*.manifest.json` beside each gold parquet, capturing dataset name, grain, source versions, row count, run_id, git commit, and caveats.

#### Acceptance criteria
- [ ] Script exists at `utility/tools/write_run_manifest.py`.
- [ ] Run after pipeline produces one `<mart>.manifest.json` per gold parquet in `data/gold/parquet/`.
- [ ] Manifest fields match v4 §3.5 schema.
- [ ] Idempotent (re-running with same data produces same JSON modulo timestamps).

#### References
- v4 §3.5 (manifest JSON schema)
- existing `data/gold/parquet/*.parquet` for the list of marts

#### Helper prompt
```
Create utility/tools/write_run_manifest.py. It should walk data/gold/parquet/, and for each *.parquet file, write a sibling *.manifest.json with this schema:

{
  "dataset_name": str,
  "layer": "mart",
  "grain": str,
  "built_from": [str],
  "row_count": int,
  "run_id": str,
  "git_commit": str (short SHA),
  "source_versions": {source_name: ISO timestamp},
  "caveats": [str]
}

Source the run_id from an env var DAIL_RUN_ID (default to current UTC timestamp). Source the git commit from `git rev-parse --short HEAD`. The grain, built_from, and caveats fields should come from a small static lookup keyed by dataset_name (define this in the script — one entry per existing gold mart).

Don't modify any existing pipeline modules. This is a standalone tool invoked after pipeline.py runs.

Reference: doc/dail_tracker_improvements_v4.md §3.5.
```

#### What NOT to do
- Don't try to derive grain/caveats from data; use a static lookup.
- Don't make the script run the pipeline — it consumes pipeline output.

---

### DAIL-003 — GitHub Actions refresh workflow

- **Estimate:** 2h · **Priority:** P0 · **Dependencies:** DAIL-001, DAIL-002, DAIL-004 · **Labels:** phase-1, refresh, ci

#### Description
Add `.github/workflows/refresh.yml` that runs the pipeline on a monthly cron and on workflow_dispatch, then commits regenerated parquet to the `data` branch.

#### Acceptance criteria
- [ ] Workflow exists at `.github/workflows/refresh.yml`.
- [ ] Manual trigger from Actions UI runs the pipeline successfully.
- [ ] On success, parquet + manifests are committed to the `data` branch.
- [ ] `permissions: contents: write` is set.
- [ ] On failure, no commit happens.

#### References
- v4 §3.3 (full YAML skeleton)
- existing `pyproject.toml` for install command

#### Helper prompt
```
Create .github/workflows/refresh.yml using the YAML skeleton in doc/dail_tracker_improvements_v4.md §3.3. Adapt it to:

- Cron: monthly on the 5th at 04:00 UTC (cron: '0 4 5 * *')
- Also workflow_dispatch with a 'sources' input (default: 'all')
- Python 3.11
- Install with: pip install -e ".[dev,db]"
- Run python pipeline.py --refresh (DAIL-001 must be done first)
- Run python -m utility.tools.write_run_manifest (DAIL-002 must be done first)
- Commit changed parquet + manifest files to the `data` branch

Use the bot identity dail-tracker-bot for git config. Use github-actions[bot] noreply email.

Don't add any notification logic in this PR; that's DAIL-011. Don't add Healthchecks pings yet; that's DAIL-012.

Reference: doc/dail_tracker_improvements_v4.md §3.3 has the verbatim YAML to copy.
```

#### What NOT to do
- Don't add notifications inline; separate ticket.
- Don't push to main; only to `data` branch.
- Don't skip the `permissions: contents: write` line — it's why most first-time runs fail.

---

### DAIL-004 — Set up `data` branch

- **Estimate:** 0.5h · **Priority:** P0 · **Dependencies:** none · **Labels:** phase-1, refresh

#### Description
Create an orphan `data` branch holding only `data/gold/parquet/*.parquet` and `*.manifest.json` files. Streamlit Cloud (DAIL-006) reads from this; app code reads from `main`.

#### Acceptance criteria
- [ ] `data` branch exists in the repo.
- [ ] Branch contains current parquet + manifest files only (no code).
- [ ] Branch is pushed to origin.

#### References
- v4 §3.2 (reference architecture)

#### Helper prompt
```
Help me create an orphan `data` branch. The branch should contain only data/gold/parquet/*.parquet and the manifests from DAIL-002. No code, no docs, no tests.

Walk me through the git commands. I want to:
1. Create the orphan branch (no parent commit)
2. Add only the parquet + manifest files
3. Initial commit "chore(data): initial data branch"
4. Push to origin

Don't try to automate this — it's a one-off setup. Tell me each command, I'll run it and confirm before the next step.
```

#### What NOT to do
- Don't include any code, docs, or tests on this branch.
- Don't make this branch trackable from main.

---

### DAIL-005 — Manual integration test of refresh workflow

- **Estimate:** 2h · **Priority:** P0 · **Dependencies:** DAIL-003, DAIL-004 · **Labels:** phase-1, refresh, ops

#### Description
Trigger the refresh workflow manually from the GitHub Actions UI, verify it completes, verify the data branch updates, verify Streamlit Cloud (next ticket) will be able to read it.

#### Acceptance criteria
- [ ] Workflow completes without error in <60 minutes.
- [ ] `data` branch has a new commit from `dail-tracker-bot`.
- [ ] Parquet files reflect today's data (manually verify one row count).
- [ ] Manifest files exist for each gold mart.

#### References
- DAIL-003 workflow setup

#### Helper prompt
```
I've just merged DAIL-003. Walk me through:
1. How to manually trigger a workflow from the GitHub Actions UI.
2. What to look for in the logs to confirm each step succeeded.
3. How to inspect the data branch after the workflow runs.
4. How to verify a parquet's row count matches expectations (give me a one-liner using duckdb CLI or polars).

If any step fails, help me debug. The most common failures are:
- permissions error on push to data branch
- pip install fails because of an unpinned dependency
- pipeline.py errors on a source URL change

Don't suggest fixes that change scope (e.g. don't suggest adding a new feature).
```

#### What NOT to do
- Don't fix all warnings; focus on hard failures.
- Don't add features mid-debug; note them for later tickets.

---

### DAIL-006 — Streamlit Community Cloud deploy

- **Estimate:** 3h · **Priority:** P0 · **Dependencies:** DAIL-005 · **Labels:** phase-1, deploy

#### Description
Create the Streamlit Community Cloud app pointing at `main` for code and configured to read from the `data` branch.

#### Acceptance criteria
- [ ] Public URL exists.
- [ ] Page loads without error.
- [ ] Member overview page shows real data (not empty / not stale).
- [ ] Build logs show no warnings about missing dependencies.

#### References
- v4 §16 (hosting and cost)
- Streamlit Community Cloud docs (in v4 §19 reading list)

#### Helper prompt
```
I'm deploying a Streamlit app to Streamlit Community Cloud. The repo is structured as:

- main branch: app code (utility/app.py is the entrypoint), pyproject.toml, sql_views/, etc.
- data branch: data/gold/parquet/*.parquet + manifests

The app reads parquet from data/gold/parquet/. It should not write anything.

Walk me through:
1. Creating the app on share.streamlit.io
2. Configuring the entry point (utility/app.py)
3. Configuring how it accesses the data branch (probably by checking it out alongside main)
4. Setting up secrets if needed

Don't suggest forking the repo or duplicating the data into main.

After deploy, smoke-test plan: I'll click through every page and note anything broken. We'll only fix catastrophic issues now (whitescreen, crashes); cosmetic issues go in a follow-up ticket.
```

#### What NOT to do
- Don't fix non-catastrophic UI issues during deploy.
- Don't change app structure to fit Cloud limitations; flag any blockers for separate tickets.

---

### DAIL-007 — Smoke-test public URL

- **Estimate:** 1h · **Priority:** P0 · **Dependencies:** DAIL-006 · **Labels:** phase-1, deploy

#### Description
Click through every page on the deployed alpha. Note anything broken. Fix only catastrophic issues; defer the rest to a follow-up ticket.

#### Acceptance criteria
- [ ] All 8 pages load.
- [ ] No 500-level errors.
- [ ] No hard crashes.
- [ ] Issues noted in a follow-up GitHub issue (one per page, labelled `alpha-feedback`).

#### References
- existing `utility/pages_code/`

#### Helper prompt
```
I'm smoke-testing my deployed Streamlit app at [URL]. I'll click through each of the 8 pages: attendance, member overview, votes, interests, payments, lobbying, legislation, committees.

For each page, I'll report what I see. You help me categorise:
- CATASTROPHIC: white screen, hard error, missing all data → fix immediately
- COSMETIC: broken styling, missing label, layout issue → defer to follow-up issue
- DATA QUALITY: wrong number, suspicious join → defer to follow-up issue

For each catastrophic issue, suggest a one-line minimal fix. For deferrable issues, give me a one-line GitHub Issue title + label suggestion.

Don't try to fix non-catastrophic issues mid-flow. Don't ask me to redesign anything.
```

#### What NOT to do
- Don't fix cosmetic issues now.
- Don't refactor anything based on smoke test findings.

---

### DAIL-008 — Provenance helper

- **Estimate:** 3h · **Priority:** P0 · **Dependencies:** DAIL-002 · **Labels:** phase-1, ui

#### Description
Build `render_provenance(manifest_path)` helper that reads a mart manifest (DAIL-002 output) and renders an `st.expander` showing source, grain, last refresh, run_id, git commit, source versions, caveats.

#### Acceptance criteria
- [ ] Helper exists at `utility/components/provenance.py` (or similar).
- [ ] Given a manifest path, renders a "Source and methodology" expander.
- [ ] Expander shows all manifest fields clearly.
- [ ] Caveats render as a bulleted list.

#### References
- v4 §9.1 (provenance footer on every page)
- DAIL-002 manifest schema

#### Helper prompt
```
Create utility/components/provenance.py with a function:

    def render_provenance(manifest_path: Path | str) -> None

The function reads a JSON manifest file with the schema from DAIL-002 / v4 §3.5 and renders a Streamlit expander titled "Source and methodology" containing:
- Source mart name + grain
- Last refresh time (parsed and humanised, e.g. "2 days ago")
- Run ID + short git commit
- Source versions table
- Caveats as a bulleted list

If the manifest doesn't exist or fails to parse, render a small warning ("Provenance unavailable") rather than crashing.

Use st.expander, st.markdown, and st.dataframe for the source-versions table.

Don't add caching here; the manifest is small and Streamlit's default re-read is fine. Don't include any business logic — this is presentation only.

Reference: doc/dail_tracker_improvements_v4.md §9.1.
```

#### What NOT to do
- Don't compute anything from the manifest beyond display formatting.
- Don't make it modify state.

---

### DAIL-009 — Freshness badge helper

- **Estimate:** 2h · **Priority:** P0 · **Dependencies:** DAIL-002 · **Labels:** phase-1, ui

#### Description
Build a helper that renders a coloured pill at the top of each page showing data age relative to expected cadence. Green < 7d, amber 7–30d, red > 30d (configurable per source).

#### Acceptance criteria
- [ ] Helper exists at `utility/components/freshness_badge.py` (or co-located with provenance).
- [ ] Takes a manifest path + expected cadence.
- [ ] Renders a small pill (green/amber/red) with humanised age.
- [ ] Visible without needing to expand anything.

#### References
- v4 §9.2 (freshness badge per page)

#### Helper prompt
```
Create a render_freshness_badge(manifest_path, expected_cadence_days) helper. It reads the manifest from DAIL-002, computes age from the last refresh timestamp, and renders a coloured pill:

- Green: age < expected_cadence_days (data is current)
- Amber: expected_cadence_days <= age < expected_cadence_days * 3
- Red: age >= expected_cadence_days * 3

Use st.html or st.markdown with inline CSS for the pill. Match the existing card pattern in utility/styles/shared_css.py if there is one — re-use the colour palette.

Pill text should read: "Refreshed N days ago" (humanised).

Don't make this an expander — it's always visible at the top of the page.
Don't add any user interaction; pure display.

Reference: doc/dail_tracker_improvements_v4.md §9.2.
```

#### What NOT to do
- Don't add interactivity.
- Don't fetch upstream to verify freshness; trust the manifest.

---

### DAIL-010 — Wire provenance + freshness on three pages

- **Estimate:** 3h · **Priority:** P0 · **Dependencies:** DAIL-008, DAIL-009 · **Labels:** phase-1, ui

#### Description
Wire the helpers from DAIL-008 and DAIL-009 onto the three most-used pages (member overview, payments, lobbying). Defer the other 5 pages to a Plateau 2 ticket.

#### Acceptance criteria
- [ ] Member overview page shows freshness badge at top + provenance expander at bottom.
- [ ] Payments page same.
- [ ] Lobbying page same.
- [ ] Both helpers read from the right per-mart manifest.

#### References
- DAIL-008, DAIL-009 helpers
- existing `utility/pages_code/member_overview.py`, `payments.py`, `lobbying_2.py`

#### Helper prompt
```
Wire the helpers from DAIL-008 (render_provenance) and DAIL-009 (render_freshness_badge) into three pages:

- utility/pages_code/member_overview.py
- utility/pages_code/payments.py
- utility/pages_code/lobbying_2.py

For each page:
1. Identify the manifest file path for that page's primary gold mart.
2. Add render_freshness_badge(manifest_path, expected_cadence_days=N) at the very top of the page render function (before any other content).
3. Add render_provenance(manifest_path) at the very bottom (after all main content).

Expected cadence per page:
- member_overview: 30 days
- payments: 35 days
- lobbying: 130 days

Don't wire on the other 5 pages yet (attendance, votes, interests, legislation, committees) — that's a separate Plateau 2 ticket.

Don't refactor the page logic; only add the two helper calls.
```

#### What NOT to do
- Don't wire on more than 3 pages.
- Don't refactor page internals.

---

## Week 2 — Alarm + parser tests

---

### DAIL-011 — Tier 1 GitHub Issues on refresh failure

- **Estimate:** 1h · **Priority:** P0 · **Dependencies:** DAIL-003 · **Labels:** phase-1, ops, notifications

#### Description
Add `if: failure()` step to the refresh workflow that opens a labelled issue with the run ID and logs URL.

#### Acceptance criteria
- [ ] Step added to `.github/workflows/refresh.yml`.
- [ ] Deliberately breaking the workflow (temporary `exit 1`) creates a GitHub issue.
- [ ] Issue is labelled `refresh-failed` and `autogen`.
- [ ] Issue body contains run ID + logs URL.

#### References
- v4 §4.7 Tier 1 (GitHub Issues as alarm bell)

#### Helper prompt
```
Add an `if: failure()` step to .github/workflows/refresh.yml that uses actions/github-script@v7 to open a GitHub issue when the workflow fails.

The issue should have:
- Title: `[refresh-failed] run ${context.runId}`
- Body: include the link to the failed run logs
- Labels: ['refresh-failed', 'autogen']

If those labels don't exist in the repo, create them in the same PR (.github/labels.yml or similar — or just create them manually in the GitHub UI).

To test: temporarily add `exit 1` to one of the existing steps, push, trigger the workflow, verify an issue opens. Then revert the test change.

Reference: doc/dail_tracker_improvements_v4.md §4.7 Tier 1.

Don't add Healthchecks here; that's DAIL-012.
Don't try to dedupe issues — let GitHub create one per failed run for now.
```

#### What NOT to do
- Don't add deduplication of issues.
- Don't try to close issues automatically yet.

---

### DAIL-012 — Tier 2 Healthchecks.io ping per source

- **Estimate:** 2h · **Priority:** P0 · **Dependencies:** DAIL-003 · **Labels:** phase-1, ops, notifications

#### Description
Sign up for Healthchecks.io (anonymous tier or free signup), generate one ping URL per source, add `curl` calls at start and end of each refresh workflow.

#### Acceptance criteria
- [ ] Healthchecks account exists with 4 checks (payments, attendance, interests, lobbying).
- [ ] Each check's expected interval is set to match the source cadence.
- [ ] Workflow pings start + end of each source's refresh.
- [ ] UUIDs stored in GitHub repo secrets, not in workflow file.
- [ ] Intentionally skipping a refresh triggers Healthchecks alert.

#### References
- v4 §4.7 Tier 2 (Healthchecks.io as dead-man's-switch)

#### Helper prompt
```
Walk me through setting up Healthchecks.io for the dail_tracker pipeline:

1. I'll sign up for the free tier myself.
2. I'll create 4 checks: payments-refresh, attendance-refresh, interests-refresh, lobbying-refresh.
3. Help me decide expected intervals per check based on:
   - payments: monthly
   - attendance: weekly
   - interests: annual
   - lobbying: tri-annual

For each check, I'll get a UUID URL. I'll store these as GitHub secrets:
- HEALTHCHECK_PAYMENTS_UUID, HEALTHCHECK_ATTENDANCE_UUID, etc.

Then update .github/workflows/refresh.yml to add curl calls at the start and end of each source's refresh step:

curl -fsS -o /dev/null https://hc-ping.com/${{ secrets.HEALTHCHECK_PAYMENTS_UUID }}/start
... refresh code ...
curl -fsS -o /dev/null https://hc-ping.com/${{ secrets.HEALTHCHECK_PAYMENTS_UUID }}

Reference: doc/dail_tracker_improvements_v4.md §4.7 Tier 2.

Don't put UUIDs in the workflow file directly. Don't ping if the source step failed (use `if: success()` on the end-ping).
```

#### What NOT to do
- Don't put UUIDs in the workflow file.
- Don't ping the end if the source step failed.

---

### DAIL-013 — End-to-end alarm chain test

- **Estimate:** 2h · **Priority:** P0 · **Dependencies:** DAIL-011, DAIL-012 · **Labels:** phase-1, ops

#### Description
Force a deliberate failure and verify the full chain: workflow fails → Issue opens → end-of-run Healthchecks ping is missed → you receive the Healthchecks notification.

#### Acceptance criteria
- [ ] One deliberate failure injected.
- [ ] Issue opens automatically.
- [ ] Healthchecks shows missed ping after expected interval.
- [ ] You confirm receipt of the Healthchecks notification (in your chosen channel).
- [ ] Restore-to-green PR merged.

#### References
- DAIL-011, DAIL-012 wiring

#### Helper prompt
```
Help me end-to-end test the alarm chain.

Plan:
1. Create a branch `test/alarm-chain`. Add `exit 1` to one step in refresh.yml.
2. Force-trigger the workflow on that branch via workflow_dispatch.
3. Verify within ~5 minutes:
   - Workflow shows as failed
   - A GitHub Issue auto-opens
4. Wait for Healthchecks expected interval to elapse, verify:
   - Healthchecks marks the relevant check as DOWN
   - You receive notification in your chosen channel (email/discord/etc.)
5. Close the Issue, merge a revert PR, re-run successfully, verify Issue stays closed and Healthchecks goes back to UP.

If any step doesn't work, debug. Don't ship with broken alarms — they're worse than no alarms because they create false confidence.
```

#### What NOT to do
- Don't skip this validation step. Untested alarms are worse than none.
- Don't leave the `exit 1` change merged.

---

### DAIL-014 — Payments parser golden file test

- **Estimate:** 5h · **Priority:** P0 · **Dependencies:** none · **Labels:** phase-1, tests

#### Description
Commit one representative payments PDF to `test/fixtures/payments/`. Run the parser, save expected silver output as `.expected.parquet`. Write a test that re-runs and asserts equality.

#### Acceptance criteria
- [ ] PDF fixture committed at `test/fixtures/payments/<file>.pdf`.
- [ ] Expected output committed at `test/fixtures/payments/<file>.expected.parquet`.
- [ ] Test at `test/test_payment_parser_golden.py` passes.
- [ ] Deliberately editing the parser fails the test.

#### References
- v4 §5.2 (golden-file PDF regression tests)
- HANDS_OFF_TEST_PLAN Phase B
- existing `payments.py` parser

#### Helper prompt
```
Create a golden-file regression test for the payments parser.

1. Pick the most recent payments PDF from data/bronze/pdfs/payments/ (e.g. 2026-04-02_..._february-2026.pdf).
2. Copy it to test/fixtures/payments/<file>.pdf. (Keep file size <5MB; if larger, find a smaller representative one.)
3. Run the existing parser from payments.py against this fixture:

    from payments import process_payment_pdfs  # adjust import as needed
    out_df = parse_single_pdf(Path("test/fixtures/payments/<file>.pdf"))

4. Manually inspect the output. Confirm rows look correct (compare ~10 rows against the source PDF visually).
5. Save the verified output as test/fixtures/payments/<file>.expected.parquet.
6. Write test/test_payment_parser_golden.py with a single test:

    def test_payments_parser_february_2026():
        actual = parse_single_pdf(FIXTURE_PDF)
        expected = pl.read_parquet(EXPECTED_PARQUET)
        pl.testing.assert_frame_equal(actual, expected, check_row_order=False)

7. Verify: deliberately add a "+ 1" to one parser amount, test fails. Revert.

The parser may not have a single-PDF entry point currently — if not, add one (don't refactor the existing process_payment_pdfs which loops over a directory). New function: parse_single_pdf(pdf_path: Path) -> pl.DataFrame.

Reference: doc/dail_tracker_improvements_v4.md §5.2.
```

#### What NOT to do
- Don't refactor the existing parser; add a single-PDF entry point alongside it.
- Don't commit fixture PDFs >5MB.
- Don't skip the visual verification step in (4).

---

### DAIL-015 — Attendance parser golden file test

- **Estimate:** 5h · **Priority:** P0 · **Dependencies:** none · **Labels:** phase-1, tests

#### Description
Same pattern as DAIL-014 but for the attendance parser. Pick `pdf_2026` (2026-04-02 deputies-verification PDF).

#### Acceptance criteria
- [ ] Fixture PDF + expected parquet committed.
- [ ] `test/test_attendance_parser_golden.py` passes.
- [ ] Deliberate parser edit fails the test.

#### References
- v4 §5.2
- HANDS_OFF_TEST_PLAN Phase B
- existing `attendance.py` parser

#### Helper prompt
```
Create a golden-file regression test for the attendance parser.

Same pattern as DAIL-014. Specifics:
- Source PDF: data/bronze/pdfs/attendance/2026-04-02_..._01-january-2026-to-28-february-2026_en.pdf (or equivalent)
- Fixture path: test/fixtures/attendance/<file>.pdf
- Expected: test/fixtures/attendance/<file>.expected.parquet
- Test file: test/test_attendance_parser_golden.py

Add parse_single_attendance_pdf(pdf_path) entry point if one doesn't exist.

Visual verification step is critical — attendance PDFs have date-range slugs that are easy to mis-interpret. Confirm a sample of TDs and dates match the source PDF before saving expected output.

Reference: doc/dail_tracker_improvements_v4.md §5.2.
```

#### What NOT to do
- Don't refactor existing attendance.py.
- Don't skip visual verification.

---

### DAIL-016 — Interests parser golden file test

- **Estimate:** 5h · **Priority:** P0 · **Dependencies:** none · **Labels:** phase-1, tests

#### Description
Same pattern as DAIL-014/015 but for the interests parser. Pick `dail_member_interests_2025`.

#### Acceptance criteria
- [ ] Fixture PDF (or trimmed subset) + expected parquet committed.
- [ ] `test/test_interests_parser_golden.py` passes.
- [ ] Deliberate parser edit fails the test.

#### References
- v4 §5.2
- HANDS_OFF_TEST_PLAN Phase B
- existing `member_interests.py` parser

#### Helper prompt
```
Create a golden-file regression test for the member interests parser.

Same pattern as DAIL-014/015. Specifics:
- Source PDF: data/bronze/pdfs/interests/2026-02-25_..._dail-eireann-2025_en.pdf
- This is large (probably >10MB). Trim to first ~30 pages using PyMuPDF if needed:

    import fitz
    src = fitz.open(SOURCE_PDF)
    out = fitz.open()
    out.insert_pdf(src, from_page=0, to_page=29)
    out.save("test/fixtures/interests/dail_2025_first30pages.pdf")

- Fixture path: test/fixtures/interests/dail_2025_first30pages.pdf
- Expected: same path with .expected.parquet
- Test file: test/test_interests_parser_golden.py

Watch: the interests parser depends on the slug change between 2021 and 2022 — pick a 2022+ PDF to test the current slug variant.

Reference: doc/dail_tracker_improvements_v4.md §5.2.
```

#### What NOT to do
- Don't commit a 50MB PDF.
- Don't pick a pre-2022 PDF (different slug variant).

---

### DAIL-017 — Row-count drift assertions in silver writers

- **Estimate:** 3h · **Priority:** P0 · **Dependencies:** none · **Labels:** phase-1, tests, ops

#### Description
Wrap each silver write step with `check_drift(current_count, history)`. Hard-fail if delta > 15% from rolling-5-run average. Store history in `data/_meta/row_count_history/<dataset>.json`.

#### Acceptance criteria
- [ ] Helper `pipeline/quality/drift_check.py` exists.
- [ ] Each silver write call invokes the check.
- [ ] History persists between runs in `data/_meta/row_count_history/`.
- [ ] Synthetic 30% drop fails the next run.

#### References
- v4 §5.3 (row-count drift assertions)

#### Helper prompt
```
Implement row-count drift assertions:

1. Create pipeline/quality/drift_check.py:

    def check_row_count_drift(
        dataset_name: str,
        current_count: int,
        threshold_pct: float = 15.0,
        history_dir: Path = DEFAULT_HISTORY_DIR,
    ) -> None:
        """Raises RowCountDriftError if delta from rolling-5 average exceeds threshold_pct."""

2. Persist history at data/_meta/row_count_history/<dataset>.json:

    {
      "history": [
        {"run_id": "...", "row_count": 4221, "delta_pct": null, "timestamp": "..."},
        ...
      ]
    }

   Keep last 20 entries.

3. Identify all silver writers (search for `write_parquet` and `write_csv` in pipeline modules). Add the check before each write:

    check_row_count_drift("aggregated_payment_tables", len(df))
    df.write_parquet(SILVER_DIR / "...")

4. First run after a fresh history file should not raise (no baseline to compare).

Reference: doc/dail_tracker_improvements_v4.md §5.3.

Don't change the silver writers' shapes; just wrap them.
Don't add the check inside enrich.py or normalise_join_key.py.
```

#### What NOT to do
- Don't make threshold dataset-specific in this PR; uniform 15% is fine for now.
- Don't add to enrich.py or normalise_join_key.py.

---

### DAIL-018 — Synthetic drift test

- **Estimate:** 1h · **Priority:** P1 · **Dependencies:** DAIL-017 · **Labels:** phase-1, tests

#### Description
Unit test confirming the drift check raises on injected drift, ignores small drift, skips when no history.

#### Acceptance criteria
- [ ] `test/test_drift_check.py` has 3 test cases (raises on 30% drop, tolerates 5% drift, no-raise on empty history).
- [ ] All pass.

#### References
- DAIL-017 helper
- HANDS_OFF_TEST_PLAN Phase D

#### Helper prompt
```
Write test/test_drift_check.py with three tests for the helper from DAIL-017:

1. test_drift_check_raises_on_30pct_drop:
   - History: 5 runs of row_count=1000
   - Current: 700
   - Assert RowCountDriftError raised

2. test_drift_check_tolerates_5pct_drift:
   - History: 5 runs of 1000
   - Current: 950
   - Assert no raise

3. test_drift_check_skips_when_no_history:
   - History: empty
   - Current: 1000
   - Assert no raise

Use tmp_path for the history directory so tests don't pollute real history.

Don't test the persistence side here (covered by integration tests later).
```

#### What NOT to do
- Don't test persistence here.
- Don't read/write to real history files in tests.

---

## Week 3 — API protection + vote fix + public-safe

---

### DAIL-019 — JSON schemas for Oireachtas API endpoints

- **Estimate:** 4h · **Priority:** P0 · **Dependencies:** none · **Labels:** phase-1, tests, robustness

#### Description
For each Oireachtas API endpoint the pipeline calls (members, legislation, debates, votes), commit a JSON Schema describing the top-level shape and the fields the pipeline reads.

#### Acceptance criteria
- [ ] `pipeline/schemas/members.json`, `legislation.json`, `debates.json`, `votes.json` exist.
- [ ] Each schema validates a real saved sample response.
- [ ] Schemas are loose: head + top-level fields, not every nested optional.

#### References
- v4 §6.5 (type hints + pydantic note — apply to JSON validation here)
- HANDS_OFF_TEST_PLAN Phase C
- `members_api_service.py` for current endpoint usage

#### Helper prompt
```
Create JSON Schema files for each Oireachtas API endpoint we hit:

- pipeline/schemas/members.json
- pipeline/schemas/legislation.json
- pipeline/schemas/debates.json
- pipeline/schemas/votes.json

Each schema should describe:
- The top-level structure (head + results array)
- head.totalResults (integer) — required
- head.dateRange / head.skip / head.limit if present
- The shape of items in results, but only the fields the pipeline actually reads (search members_api_service.py and related modules for `["..."]` access patterns)

Schemas should be strict on top-level structure but permissive on nested optional fields. Think "would catch a renamed key" but "won't break on a new optional addition".

Save a sample response per endpoint at test/fixtures/api/<endpoint>_sample.json. Pull these from the most recent bronze JSON files (data/bronze/legislation/, data/bronze/votes/, etc.).

Validate each schema against its sample using:

    import jsonschema
    schema = json.load(open("pipeline/schemas/members.json"))
    sample = json.load(open("test/fixtures/api/members_sample.json"))
    jsonschema.validate(sample, schema)  # should not raise

Don't try to schema every nested field — too brittle.
Don't validate optional fields as required.

Reference: doc/dail_tracker_improvements_v4.md §6.5 + test/HANDS_OFF_TEST_PLAN.md Phase C.
```

#### What NOT to do
- Don't make every nested field required.
- Don't include fields the pipeline doesn't read.

---

### DAIL-020 — Validate-at-fetch wiring

- **Estimate:** 2h · **Priority:** P0 · **Dependencies:** DAIL-019 · **Labels:** phase-1, robustness

#### Description
Each fetch in `members_api_service.py` (and other API callers) calls `jsonschema.validate(response, schema)` immediately after parsing. Failure raises and is caught at workflow level (DAIL-011 issue auto-creation).

#### Acceptance criteria
- [ ] Every API fetch validates its response.
- [ ] Synthetic test (with manipulated response) raises ValidationError.
- [ ] Validation runs before any downstream processing.

#### References
- DAIL-019 schemas
- existing `members_api_service.py`

#### Helper prompt
```
Wire the schemas from DAIL-019 into the API fetch points.

1. Add a small helper at pipeline/api/_validate.py:

    def validate_response(endpoint_name: str, response_data: dict) -> None:
        schema = _load_schema(endpoint_name)
        jsonschema.validate(response_data, schema)
        # Raises jsonschema.ValidationError on drift

2. Find every API fetch in:
   - members_api_service.py
   - legislation.py (if it does API calls)
   - any other module that calls api.oireachtas.ie

3. After each `response.json()` (or equivalent), call validate_response(endpoint_name, payload).

4. Add a test test/test_api_schemas.py:

    def test_manipulated_response_fails_validation():
        sample = json.load(open("test/fixtures/api/members_sample.json"))
        del sample["head"]["totalResults"]
        with pytest.raises(jsonschema.ValidationError):
            validate_response("members", sample)

Don't change what the pipeline does with the data; only add the validation step.
Don't catch and ignore the ValidationError; let it propagate so the workflow fails loudly.

Reference: doc/dail_tracker_improvements_v4.md §6.5.
```

#### What NOT to do
- Don't catch ValidationError silently.
- Don't add validation downstream in silver writers; do it at fetch time.

---

### DAIL-021 — Vote pagination Phase 1 (paginated fetcher in sandbox)

- **Estimate:** 5h · **Priority:** P0 · **Dependencies:** DAIL-019 · **Labels:** phase-1, robustness

#### Description
Implement the paginated vote fetcher per `pipeline_sandbox/votes_pagination_plan.md` Phase 1. Sandbox module that loops with `skip`/`limit` and asserts on `head.totalResults`.

#### Acceptance criteria
- [ ] `pipeline_sandbox/votes_paginated_fetch.py` exists.
- [ ] Function `fetch_all_votes(start_date, end_date) -> list[dict]` returns all votes (count matches `head.totalResults`).
- [ ] Unit tests against mocked API response cover the loop logic.
- [ ] Existing production call is untouched.

#### References
- `pipeline_sandbox/votes_pagination_plan.md` (Phase 1 detail)

#### Helper prompt
```
Build the paginated vote fetcher per pipeline_sandbox/votes_pagination_plan.md Phase 1.

Create pipeline_sandbox/votes_paginated_fetch.py with:

    def fetch_all_votes(start_date: str, end_date: str, page_size: int = 100) -> list[dict]:
        """Paginated fetch of vote records. Loops until total accounted for or page is short."""

Key requirements (per the plan):
- page_size = 100 (not 1000)
- Pass explicit sort=date and order=desc params
- Read total from head.totalResults on first response
- Loop: skip += page_size each iteration
- Break when len(page["results"]) < page_size OR skip >= total
- Final assert len(all_results) >= total - tolerated_drift (use tolerated_drift = 5)

Use the same HTTP helper / requests session pattern as members_api_service.py.

Write test/test_votes_paginated_fetch.py with:
- mocked response sequence covering: small total (1 page), medium total (3 pages), drift case (total changes mid-fetch)

Don't modify members_api_service.py or transform_votes.py. This is sandbox-only for Phase 1.

Reference: pipeline_sandbox/votes_pagination_plan.md.
```

#### What NOT to do
- Don't switch over the production fetcher yet (that's DAIL-023).
- Don't change page size to 1000; the plan specifies 100.
- Don't omit the explicit sort/order params.

---

### DAIL-022 — Vote pagination Phase 2 (side-by-side compare)

- **Estimate:** 3h · **Priority:** P0 · **Dependencies:** DAIL-021 · **Labels:** phase-1, robustness

#### Description
Run old fetcher (limit=1000) and new paginated fetcher in parallel; compare outputs. Per the plan, Phase 2 is non-negotiable before switch-over.

#### Acceptance criteria
- [ ] Comparison script `pipeline_sandbox/votes_compare.py` exists.
- [ ] Run produces `data/_canary/votes_comparison_<date>.json` with: count_old, count_new, in_new_not_old (sample), in_old_not_new (sample).
- [ ] `in_old_not_new` is empty (or every entry has documented reason).
- [ ] `in_new_not_old` is non-empty (since Phase 0 confirmed truncation).
- [ ] Manual verification of 5 sample new records against the Oireachtas web UI.

#### References
- `pipeline_sandbox/votes_pagination_plan.md` Phase 2

#### Helper prompt
```
Build the side-by-side comparison per pipeline_sandbox/votes_pagination_plan.md Phase 2.

Create pipeline_sandbox/votes_compare.py:

    def compare_fetchers(start_date: str, end_date: str) -> dict:
        old = fetch_votes_existing(start_date, end_date)  # current limit=1000 path
        new = fetch_all_votes(start_date, end_date)       # DAIL-021

        old_keys = {(r["division"]["voteId"]) for r in old}
        new_keys = {(r["division"]["voteId"]) for r in new}

        return {
            "count_old": len(old),
            "count_new": len(new),
            "in_new_not_old": list(new_keys - old_keys)[:20],
            "in_old_not_new": list(old_keys - new_keys)[:20],
        }

Save the comparison output to data/_canary/votes_comparison_<isodate>.json.

After running:
- Verify in_old_not_new is empty (or document why each entry is there).
- Manually verify ~5 records from in_new_not_old against the live Oireachtas web UI (vote ID → URL pattern from transform_votes.py).
- If both check out, you're cleared for DAIL-023.

Don't proceed to DAIL-023 (switch over) until this passes.
Don't modify production code in this PR.

Reference: pipeline_sandbox/votes_pagination_plan.md Phase 2.
```

#### What NOT to do
- Don't proceed to switch-over before this comparison passes.
- Don't manually verify zero records — five is the minimum.

---

### DAIL-023 — Vote pagination Phase 3 (switch over)

- **Estimate:** 2h · **Priority:** P0 · **Dependencies:** DAIL-022 · **Labels:** phase-1, robustness

#### Description
Replace the production vote fetch call with the paginated version from DAIL-021. Single PR. Keep old code commented out for one cycle.

#### Acceptance criteria
- [ ] Production fetch uses paginated version.
- [ ] Old code path commented out (not deleted) with a reference to this ticket.
- [ ] Refresh runs successfully with new fetcher.
- [ ] Vote silver row count matches API total.

#### References
- DAIL-021, DAIL-022
- `pipeline_sandbox/votes_pagination_plan.md` Phase 3

#### Helper prompt
```
Switch the production vote fetcher to the paginated version from DAIL-021.

1. Find the existing limit=1000 vote fetch (likely in members_api_service.py or a similar module).
2. Replace it with a call to pipeline_sandbox/votes_paginated_fetch.py:fetch_all_votes (or move that function into the production module).
3. Comment out the old code with a header:

    # OLD limit=1000 fetcher — kept for one cycle in case rollback needed
    # See DAIL-023; remove after 2026-06-XX (one month after merge)
    # def fetch_votes_old(...): ...

4. Run python pipeline.py --refresh end-to-end. Confirm vote silver row count > old count (proves truncation was real and is now fixed).
5. Single PR. Easy rollback: revert the PR.

Don't delete the old code yet (Phase 4 / cleanup ticket).
Don't change downstream consumers (transform_votes.py works on whatever is in VOTES_RAW_DIR — should be transparent).

Reference: pipeline_sandbox/votes_pagination_plan.md Phase 3.
```

#### What NOT to do
- Don't delete the old code in this PR.
- Don't change transform_votes.py.

---

### DAIL-024 — Pin dependencies + commit lockfile

- **Estimate:** 2h · **Priority:** P0 · **Dependencies:** none · **Labels:** phase-1, ci

#### Description
Add version specifiers to `pyproject.toml`. Generate and commit a lockfile. Add `pip-audit` check to CI (DAIL-025).

#### Acceptance criteria
- [ ] All dependencies in `pyproject.toml` have version specifiers.
- [ ] Lockfile committed (e.g. `uv.lock` or `requirements.lock`).
- [ ] Fresh clone + lockfile install reproduces the dev environment.

#### References
- v4 §6.3 (pin and lock dependencies)

#### Helper prompt
```
Pin the project's dependencies and commit a lockfile.

1. Read current pyproject.toml. Identify dependencies without version specifiers.
2. For each, find the currently-installed version (`pip show <pkg>` or `pip freeze | grep <pkg>`).
3. Add appropriate specifiers:
   - For libraries we directly use: ">=X.Y,<X+1" (cap at next major)
   - For tooling (ruff, pytest): "~=X.Y" (compatible release)

4. Generate a lockfile. Pick one approach:
   - uv: `uv pip compile pyproject.toml -o requirements.lock`
   - pip-tools: `pip-compile pyproject.toml`
   Commit the lockfile.

5. Verify reproducibility: in a fresh venv, install from the lockfile and confirm the project still imports + runs.

Don't pin to exact versions on libraries — that creates upgrade churn. Use compatible-release ranges.
Don't commit a lockfile that includes dev dependencies if they're optional in pyproject.toml.

Reference: doc/dail_tracker_improvements_v4.md §6.3.
```

#### What NOT to do
- Don't pin to exact versions on libraries (churny).
- Don't skip the verification step in (5).

---

### DAIL-025 — Minimum viable CI workflow

- **Estimate:** 3h · **Priority:** P0 · **Dependencies:** DAIL-024 · **Labels:** phase-1, ci

#### Description
`.github/workflows/ci.yml` running ruff, ruff-format check, pytest, and a page-import smoke test on every PR.

#### Acceptance criteria
- [ ] Workflow runs on every PR + main pushes.
- [ ] Failing ruff/format/pytest blocks the PR.
- [ ] Page-import smoke test (every file in `utility/pages_code/` imports cleanly without data) passes.
- [ ] Branch protection requires CI to pass before merge (configure in repo settings).

#### References
- v4 §6.1 (minimum viable CI)
- HANDS_OFF_TEST_PLAN per-PR section

#### Helper prompt
```
Create .github/workflows/ci.yml that runs on every PR and main push:

Steps (sequential):
1. Checkout, setup Python 3.11, cache pip
2. Install from lockfile (DAIL-024): pip install -r requirements.lock
3. ruff check .
4. ruff format --check .
5. pytest test/ -m "not e2e and not live" --maxfail=3
6. Page-import smoke test: a small inline Python that does `for f in glob(utility/pages_code/*.py): importlib.import_module(f)` and fails if any errors. (Or: write a test/test_page_imports.py that does this.)

The job should run on:
- pull_request to main
- push to main

Set timeout-minutes: 10 to prevent runaway costs.

After merging, configure branch protection on main:
- Require status checks to pass before merging
- Require ci to pass

Don't add live-network tests or e2e tests in this CI workflow (those go in nightly per HANDS_OFF Phase A/F).
Don't add lobbying-related tests if lobbying tests don't exist yet.

Reference: doc/dail_tracker_improvements_v4.md §6.1.
```

#### What NOT to do
- Don't include live-network tests in per-PR CI.
- Don't make CI slow (>5min); split slow tests to nightly.

---

### DAIL-026 — Single HTTP helper

- **Estimate:** 5h · **Priority:** P0 · **Dependencies:** none · **Labels:** phase-1, refactor, robustness

#### Description
Create `pipeline/sources/_http.py` that owns User-Agent, conditional GET, throttle, jitter, robots.txt check, retry, timeouts. Foundation for the consolidation in DAIL-027/028.

#### Acceptance criteria
- [ ] `pipeline/sources/_http.py` exists with `http_get(url, conditional=True, ...)` and `http_head(url, ...)`.
- [ ] User-Agent string set per v4 §4.3.
- [ ] Conditional GET via `If-Modified-Since` / `If-None-Match`.
- [ ] Throttle: max 1 req/sec/host with 0–500ms jitter.
- [ ] robots.txt checked once per session per host.
- [ ] Retry with exponential backoff on 429/503.
- [ ] Connect/read timeouts: 10s / 60s.
- [ ] Tests cover: throttle, conditional GET, robots.txt block, retry behaviour.

#### References
- v4 §4.3 (web-citizenship hygiene)
- v4 §11.6 (rate-limit and identify the bot)

#### Helper prompt
```
Create pipeline/sources/_http.py — a single shared HTTP helper that all source modules use.

Requirements (all from doc/dail_tracker_improvements_v4.md §4.3):

- User-Agent: "dail-tracker-bot/0.1 (+https://github.com/<owner>/dail-extractor; mailto:<contact-email>)" (use a config constant for the contact)
- Conditional GET: store last Last-Modified and ETag per URL in an in-memory cache, send If-Modified-Since and If-None-Match on subsequent requests
- Throttle: max 1 req/sec per host (track per-host last-request timestamps), 0–500ms random jitter
- Retry: exponential backoff on 429 and 503 (use tenacity), max 3 retries
- Timeouts: connect=10s, read=60s
- robots.txt: check once per session per host using urllib.robotparser; if a path is excluded, raise PolitenessError (don't fetch)

Public API:
    def http_get(url: str, *, conditional: bool = True, **kwargs) -> requests.Response
    def http_head(url: str, **kwargs) -> requests.Response

Don't replace any existing fetch calls in this PR — that's DAIL-027/028.
Don't add caching of response bodies; only headers for conditional GET.

Tests at test/test_http_helper.py:
- test_user_agent_set
- test_throttle_enforces_1_per_sec
- test_conditional_get_sends_if_modified_since
- test_robots_txt_blocks_excluded_path
- test_retry_on_429
```

#### What NOT to do
- Don't replace existing fetches yet.
- Don't cache response bodies.
- Don't make the throttle global; per-host.

---

## Week 4 — Rearchitecture + citable + journalist

---

### DAIL-027 — Consolidate scraping into `pipeline/sources/`

- **Estimate:** 6h · **Priority:** P0 · **Dependencies:** DAIL-026 · **Labels:** phase-1, refactor

#### Description
Move all upstream-fetching code into `pipeline/sources/<source>.py`. Each source uses `_http.py`. This is the largest single piece of work in Phase 1; allocate the time.

#### Acceptance criteria
- [ ] `pipeline/sources/payments.py`, `attendance.py`, `interests.py`, `lobbying.py`, `oireachtas_api.py` exist.
- [ ] Each uses `pipeline/sources/_http.py`.
- [ ] No `requests.get` / `requests.head` outside `pipeline/sources/`.
- [ ] Existing pipeline.py orchestrates from new locations.

#### References
- v4 §4.2 (publish-don't-crawl, separate ingest from app)

#### Helper prompt
```
Consolidate all upstream-fetching code into pipeline/sources/<source>.py.

Plan:
1. Create the directory structure:

    pipeline/sources/
        __init__.py
        _http.py        # already exists from DAIL-026
        payments.py     # extract from payments.py + pdf_endpoint_check.py
        attendance.py   # extract from attendance.py + pdf_endpoint_check.py
        interests.py    # extract from member_interests.py + pdf_endpoint_check.py
        lobbying.py     # extract from lobby_processing.py
        oireachtas_api.py  # extract from members_api_service.py

2. Each source module should have:
    - Constants: URL bases, expected formats
    - Discovery functions: find new assets (uses _http.py)
    - Fetch functions: download with conditional GET, hash check (uses _http.py)
    - Re-export anything pipeline.py currently imports

3. Update pipeline.py to import from new locations. Don't change orchestration logic; just imports.

4. Leave the old payments.py / attendance.py etc. as-is for now — they contain parsing logic too. Only the FETCHING code moves.

5. After: `grep -r "requests.get\|requests.head" utility/ pipeline.py *.py` should return zero results outside pipeline/sources/.

Don't move parsing logic in this PR; that stays where it is.
Don't refactor inside the moved code; only relocate.
Don't change signatures that pipeline.py depends on.

Reference: doc/dail_tracker_improvements_v4.md §4.2.
```

#### What NOT to do
- Don't move parsing logic; just fetching.
- Don't refactor inside the moved code.
- Don't change signatures the rest of the codebase depends on.

---

### DAIL-028 — Audit `utility/` imports for scraping references

- **Estimate:** 2h · **Priority:** P0 · **Dependencies:** DAIL-027 · **Labels:** phase-1, refactor

#### Description
Confirm zero scraping/fetching references in `utility/`. Refactor any violators to read from gold parquet only.

#### Acceptance criteria
- [ ] `grep -r "requests\|httpx\|fetch_\|http_" utility/` returns zero results (or only comments).
- [ ] No `utility/*.py` imports from `pipeline/sources/`.
- [ ] Streamlit app continues to work after the audit.

#### References
- v4 §4.2

#### Helper prompt
```
Audit utility/ for any references to upstream scraping or fetching.

1. Grep for these patterns in utility/:
   - import requests
   - import httpx
   - from pipeline.sources
   - urllib.request

2. For each finding, determine:
   - Is it dead code? Remove it.
   - Is it actually fetching upstream? Refactor: the data should come from the published parquet (data/gold/parquet/), not from a live scrape.
   - Is it just for type hints / module reference? Move to a non-utility location.

3. After cleanup, smoke-test the Streamlit app locally to confirm nothing broke.

Goal: utility/ should be pure presentation, with all data flowing in from gold parquet.

Don't add any "while I'm here" refactors.
Don't change page logic; only remove fetching code.

Reference: doc/dail_tracker_improvements_v4.md §4.2.
```

#### What NOT to do
- Don't refactor page logic.
- Don't add features.

---

### DAIL-029 — README warning about forks running scrapers

- **Estimate:** 1h · **Priority:** P0 · **Dependencies:** DAIL-027 · **Labels:** phase-1, docs

#### Description
Update README.MD with explicit warning: forks should not run the scrapers; they should consume the published parquet artefacts.

#### Acceptance criteria
- [ ] README.MD has a clearly-marked section: "If you fork this repo".
- [ ] Section explains: don't run the refresh workflow against `data.oireachtas.ie`; pull the published parquet release instead.
- [ ] Links to the latest GitHub Release (or the data branch).

#### References
- v4 §4.2

#### Helper prompt
```
Update README.MD with a "If you fork this repo" section, placed prominently near the top (after the project description, before Installation).

Content:

> ## If you fork this repo
>
> The pipeline in this repo scrapes public Oireachtas and lobbying.ie data.
> If you fork it and run the refresh workflow from your fork, you'll be
> generating duplicate load on Irish public services.
>
> **Don't.** Instead, consume the published parquet artefacts from
> [GitHub Releases][releases] (or the `data` branch).
>
> If you have a use case the published artefacts don't support, open an
> issue here so we can discuss what to expose.
>
> [releases]: <link to releases>

Adjust wording for the README's existing tone. Don't add anything else; this is a single-section update.

Reference: doc/dail_tracker_improvements_v4.md §4.2.
```

#### What NOT to do
- Don't restructure the README.
- Don't add other sections.

---

### DAIL-030 — Verify polite-bot behaviour

- **Estimate:** 2h · **Priority:** P0 · **Dependencies:** DAIL-027, DAIL-026 · **Labels:** phase-1, ops, verification

#### Description
Trigger a refresh, inspect request logs: User-Agent set, conditional GET firing, robots.txt checked, throttle respected.

#### Acceptance criteria
- [ ] Logs show User-Agent on every outbound request.
- [ ] At least one 304 response observed (proves conditional GET works).
- [ ] robots.txt fetched once per host per session.
- [ ] Time between consecutive requests to same host ≥1s.

#### References
- DAIL-026, DAIL-027

#### Helper prompt
```
Verify the polite-bot behaviour after DAIL-026 and DAIL-027 are done.

1. Trigger python pipeline.py --refresh locally with verbose HTTP logging:

    HTTP_LOG_LEVEL=DEBUG python pipeline.py --refresh 2>&1 | tee verify.log

2. Inspect verify.log for:
   - User-Agent header on every request (grep "dail-tracker-bot")
   - At least one 304 Not Modified response (proves conditional GET works for at least one URL)
   - robots.txt fetched once per host (grep "robots.txt")
   - Per-host time between requests >= 1 second (parse timestamps in log)

3. If any of those is missing, debug:
   - Missing User-Agent: _http.py session config wrong
   - No 304: cache state not being saved between requests
   - No robots.txt: skipped check, not initialised properly
   - Throttle too fast: per-host tracking missing

4. If all four check out, write a short note in pipeline/sources/_http.py docstring confirming the verification was done on <date>.

Don't fix issues by widening the spec; if conditional GET isn't firing, find why.

Reference: doc/dail_tracker_improvements_v4.md §4.3.
```

#### What NOT to do
- Don't widen the spec to mask a bug.
- Don't skip the per-finding check.

---

### DAIL-031 — Versioned data releases

- **Estimate:** 4h · **Priority:** P0 · **Dependencies:** DAIL-005 · **Labels:** phase-1, distribution

#### Description
Add a step to the refresh workflow that, after each successful refresh, tags a release `data-v2026.05.07` and uploads the parquet artefacts.

#### Acceptance criteria
- [ ] Workflow tags releases with date-based names.
- [ ] Each release contains gold parquet + manifests.
- [ ] Streamlit Cloud reads from the latest release (not just the data branch).
- [ ] First release tagged successfully.

#### References
- v4 §12.1 (versioned data releases)

#### Helper prompt
```
Add a "Tag and publish release" step to .github/workflows/refresh.yml.

After the existing commit-to-data-branch step, add:

    - name: Create dated release
      if: success()
      env:
        GH_TOKEN: ${{ github.token }}
      run: |
        TAG="data-v$(date -u +%Y.%m.%d)"
        gh release create "$TAG" \
          --title "Data refresh $(date -u +%Y-%m-%d)" \
          --notes "Auto-generated from run ${{ github.run_id }}" \
          data/gold/parquet/*.parquet \
          data/gold/parquet/*.manifest.json

If a release with that tag already exists (same-day re-run), the step should fail gracefully, not block the workflow.

After this is in place:
1. Test by triggering a refresh manually.
2. Verify release appears at github.com/<owner>/<repo>/releases.
3. Update the Streamlit Cloud config to pull parquet from the latest release rather than the data branch (more reproducible). This may require a small bootstrap script in the app.

Don't change the data branch logic — keep it as a fallback.
Don't include source code in the release.

Reference: doc/dail_tracker_improvements_v4.md §12.1.
```

#### What NOT to do
- Don't include code in the release.
- Don't remove the data-branch path.

---

### DAIL-032 — Tag the first release

- **Estimate:** 0.5h · **Priority:** P0 · **Dependencies:** DAIL-031 · **Labels:** phase-1, distribution

#### Description
Manually trigger the workflow, verify the first release is tagged, verify the dashboard reads from it.

#### Acceptance criteria
- [ ] First release exists at github.com/<owner>/<repo>/releases.
- [ ] Release contains expected parquet + manifests.
- [ ] Dashboard URL loads data sourced from this release.

#### References
- DAIL-031

#### Helper prompt
```
Manually trigger the refresh workflow to create the first dated release.

1. Trigger via Actions UI workflow_dispatch.
2. Wait for completion.
3. Visit github.com/<owner>/<repo>/releases — verify a tag like data-v2026.05.10 exists.
4. Download one parquet file from the release; spot-check row count matches what's in the data branch.
5. Visit the Streamlit URL — verify the data displayed reflects this release.

If the dashboard doesn't reflect the release: check the app's data-loading logic. The bootstrap should pull from the latest release tag, falling back to the data branch only if release fetch fails.

Don't manually create a release; the workflow does it.
Don't try to fix issues that aren't actually breaking the dashboard.
```

#### What NOT to do
- Don't bypass the workflow with a manual release.
- Don't fix non-blocking issues.

---

### DAIL-033 — `methodology.md` first draft

- **Estimate:** 5h · **Priority:** P0 · **Dependencies:** none · **Labels:** phase-1, docs, trust

#### Description
First draft of `doc/methodology.md` — one page per dataset describing what the numbers mean, what they don't, known caveats, worked example. Aimed at a journalist with 10 minutes.

#### Acceptance criteria
- [ ] `doc/methodology.md` exists.
- [ ] One section per dataset (attendance, payments, interests, lobbying, votes).
- [ ] Each section has: what the data is, what it doesn't capture, known caveats, one worked example from source to chart.
- [ ] Readable by a journalist in <10 minutes.

#### References
- v4 §13.1 (journalist-readable methodology)
- existing `doc/DATA_LIMITATIONS.md` (engineer-quality version)

#### Helper prompt
```
Write doc/methodology.md aimed at a journalist with 10 minutes.

Structure: one section per dataset. For each:

## <dataset name>

**What it is:** one paragraph plain-English description of the source.

**What it captures:** bullet list of what the data tells you.

**What it doesn't capture:** bullet list of what's NOT in the data (cross-reference DATA_LIMITATIONS.md).

**Worked example:** pick one specific record. Show: source PDF/API → row in silver → row in gold → how it appears on the dashboard. Maybe ~6 lines per stage.

**Caveats for citing:**
- nil vs missing
- before/after specific dates
- joins that may collide

Datasets to cover (in this order):
1. Attendance
2. Payments (Parliamentary Standard Allowance)
3. Member interests
4. Lobbying contacts
5. Votes / divisions
6. Cross-references between datasets (one short section)

Tone: plain English, no jargon, no engineering details. Imagine the reader is a journalist who's never seen the project before.

Source the caveats from DATA_LIMITATIONS.md but rewrite, don't copy.

Don't make it longer than 5 pages total.
Don't include implementation details.

Reference: doc/dail_tracker_improvements_v4.md §13.1 + doc/DATA_LIMITATIONS.md for source caveats.
```

#### What NOT to do
- Don't copy from DATA_LIMITATIONS verbatim; rewrite for non-technical reader.
- Don't include code or schema details.

---

### DAIL-034 — Identify journalist target

- **Estimate:** 2h · **Priority:** P0 · **Dependencies:** none · **Labels:** phase-1, outreach

#### Description
Pick one specific named person at one of the Irish investigative outlets. One name, email, one-paragraph reason.

#### Acceptance criteria
- [ ] One named individual identified.
- [ ] Contact email confirmed.
- [ ] Written one-paragraph rationale: why this person specifically.
- [ ] Recorded in `doc/outreach.md` (private — gitignore if needed).

#### References
- v4 §15.5 (first user)

#### Helper prompt
```
Help me pick one named Irish investigative journalist or politics academic to send the alpha to first.

Criteria:
- Has demonstrated interest in parliamentary accountability, lobbying, or political finance topics
- Active in 2025–2026 (publishing or teaching now)
- Reachable: public email or via a known publication contact form
- Likely to give 30 minutes to look at the tool

Candidate categories (pick one specific person from one):
1. Right To Know
2. The Journal Investigates / Noteworthy
3. Story.ie
4. Irish Times investigative reporter
5. Politics academic at TCD / UCD / DCU
6. Civil society: Open Knowledge Ireland, Transparency International Ireland

Help me think through:
- Who specifically (one name, one role)
- Why that person specifically (what story or work suggests they'd benefit)
- What's the one specific question I'd suggest they investigate using the tool
- Best contact route (direct email, publication contact, LinkedIn intro, etc.)

Don't pick a media celebrity / TV presenter — they don't have time for tools.
Don't pick someone who's already shipped multiple tools (they have their own).

Save the result in doc/outreach.md (gitignore that file).
```

#### What NOT to do
- Don't pick high-profile media figures.
- Don't pick people with their own tools.

---

### DAIL-035 — Send introduction email

- **Estimate:** 2h · **Priority:** P0 · **Dependencies:** DAIL-034, DAIL-033, DAIL-032 · **Labels:** phase-1, outreach

#### Description
Send the introduction email to the journalist from DAIL-034. Include URL, methodology link, one specific suggestion, contact info.

#### Acceptance criteria
- [ ] Email sent.
- [ ] Subject line is specific (not "civic data tool").
- [ ] Body acknowledges alpha state.
- [ ] Includes one concrete question they could investigate.
- [ ] Sent date logged in `doc/outreach.md`.

#### References
- DAIL-034 target

#### Helper prompt
```
Help me draft an introduction email to [name] at [outlet].

Context I'll provide:
- Their recent work / why I'm reaching out to them specifically (from DAIL-034)
- The project URL
- The methodology link
- One specific question they could investigate using the tool
- My contact for issues

Email principles:
- Subject line: specific (e.g. "Tool for cross-referencing TD lobbying contacts and votes — would value your eyes on alpha"), not generic.
- 4 paragraphs max.
  1. Why I'm contacting them (their work)
  2. What the tool is + URL
  3. The specific question / story angle they could investigate
  4. The ask (try it, tell me what's missing)
- Acknowledge it's alpha and DATA_LIMITATIONS.md is honest about gaps. Treat the doc as a feature, not a bug.
- Make it easy to ignore (no pressure for response).

Help me draft and iterate. I'll send when I'm happy with it.

Don't oversell. Don't promise features that aren't built.
Don't mass-send to multiple journalists; this is one specific person.
```

#### What NOT to do
- Don't oversell.
- Don't mass-send.

---

# Phase 2 — Discovery probe + freshness + lobbying + iteration (Weeks 5–8, ~107 hours)

## Week 5 — Buffer + discovery probe

---

### DAIL-101 — First-user iteration buffer

- **Estimate:** 5h · **Priority:** P0 · **Dependencies:** DAIL-035 · **Labels:** phase-2, outreach

#### Description
Whatever the journalist reports by Day 21 becomes the priority. Skip the rest of the week's plan if necessary.

#### Acceptance criteria
- [ ] All journalist feedback addressed or explicitly noted as "won't fix in alpha" with reasoning.
- [ ] One reply sent acknowledging the feedback.

#### Helper prompt
```
The journalist (DAIL-034/035) has responded with feedback. Help me categorise:

1. Critical: blocks them from using the tool → fix this week
2. Important: degrades the experience but doesn't block → next-week ticket
3. Wishlist: nice-to-have → defer to Phase 2 backlog
4. Out of scope: not what the project does → reply explaining politely

For each item:
- One-line summary
- Category (above)
- Estimated hours to fix (if critical)
- One-line response I'll send back

If they reported nothing: send a one-line follow-up "any first impressions?" then proceed to DAIL-102.

Don't promise features beyond Phase 2. Don't agree to scope creep.
```

#### What NOT to do
- Don't agree to scope creep.
- Don't fix wishlist items now.

---

### DAIL-102 — Resolve discovery probe index page 403

- **Estimate:** 3h · **Priority:** P1 · **Dependencies:** DAIL-026 · **Labels:** phase-2, discovery

#### Description
The Oireachtas publications topic-filtered index returned 403 to automated requests during URL verification. Investigate and resolve.

#### Acceptance criteria
- [ ] Identified root cause of the 403.
- [ ] Index page successfully fetched from code with bot User-Agent.
- [ ] Documented the resolution in `pipeline_sandbox/payment_pdf_discovery_notes.md`.

#### References
- `pipeline_sandbox/payment_pdf_discovery_notes.md` (Strategy 1 caveat)

#### Helper prompt
```
The Oireachtas publications index returned 403 to my automated requests during URL verification. URL pattern:

  https://www.oireachtas.ie/en/publications/?topic[]=parliamentary-allowances&resultsPerPage=50

Investigation plan:

1. Reproduce the 403 with curl from my machine using the bot User-Agent (DAIL-026):

   curl -v -H "User-Agent: dail-tracker-bot/0.1 (...)" "<URL>"

2. If still 403, try:
   - With Accept and Accept-Language headers
   - With a Cookie from a browser session (cookie warmup)
   - From a different IP (some WAFs block known scraper ranges)
   - With a real browser User-Agent (to rule out UA-based block)

3. Determine which fix works (if any). Document findings.

4. If a session warm-up is needed, design how that fits into the polite-bot pattern (DAIL-026).

5. If nothing works automatically, note that and consider:
   - Email Oireachtas IS to ask about API/feed access
   - Use the data.oireachtas.ie direct-PDF approach (which works) and skip index-first

Don't bypass 403 with anything that violates ToS (no faked browser fingerprint, no IP rotation).
Don't proceed to DAIL-103 until this is resolved or explicitly deferred.

Reference: pipeline_sandbox/payment_pdf_discovery_notes.md.
```

#### What NOT to do
- Don't violate ToS to bypass.
- Don't fake browser fingerprints.

---

### DAIL-103 — Index-parsing implementation

- **Estimate:** 3h · **Priority:** P1 · **Dependencies:** DAIL-102 · **Labels:** phase-2, discovery

#### Description
Implement the HTML parser for the topic-filtered publications index. Test against committed fixture HTML.

#### Acceptance criteria
- [ ] `pipeline_sandbox/payment_pdf_url_probe.py:discover_via_index` works against live index.
- [ ] Parser extracts: title, publication date, link.
- [ ] Test against fixture HTML at `test/fixtures/api/publications_index_parliamentary_allowances.html`.

#### References
- `pipeline_sandbox/payment_pdf_url_probe.py` (existing skeleton)
- `pipeline_sandbox/payment_pdf_discovery_notes.md`

#### Helper prompt
```
Implement the discover_via_index function in pipeline_sandbox/payment_pdf_url_probe.py.

The function should:
1. Fetch the topic-filtered publications index URL using DAIL-026's http_get.
2. Parse the HTML with BeautifulSoup.
3. For each publication entry, extract:
   - Title text
   - Publication date
   - Link to the PDF (resolve relative to https://www.oireachtas.ie)
4. Filter to entries matching a target month/year (e.g. "march 2026").
5. Return the matching URL (or None).

Selectors will need adjustment after inspecting live HTML. Save a working snapshot at test/fixtures/api/publications_index_parliamentary_allowances.html for fixture-based testing later.

Don't paginate beyond the first page in this PR; recent items only.
Don't try to handle slug variants here; that's the index's job (it knows the real slug).

Reference: pipeline_sandbox/payment_pdf_url_probe.py existing code + pipeline_sandbox/payment_pdf_discovery_notes.md.
```

#### What NOT to do
- Don't paginate; first page only for now.
- Don't reverse-engineer slug patterns; trust the index.

---

### DAIL-104 — HEAD-spread fallback verification

- **Estimate:** 3h · **Priority:** P1 · **Dependencies:** DAIL-026 · **Labels:** phase-2, discovery

#### Description
The construction-based candidate list (Tier 1) already exists in `payment_pdf_url_probe.py`. Verify it works end-to-end against live `data.oireachtas.ie`.

#### Acceptance criteria
- [ ] Probe finds at least one historical PDF from candidate construction.
- [ ] Uses `_http.py` for HEAD requests.
- [ ] Sequential with early exit (not parallel burst).
- [ ] Logs each candidate attempted with status.

#### References
- `pipeline_sandbox/payment_pdf_url_probe.py` Tier 1 logic

#### Helper prompt
```
Verify the existing HEAD-spread strategy in pipeline_sandbox/payment_pdf_url_probe.py works against live data.oireachtas.ie.

1. Run the probe for a known-existing PDF: `python pipeline_sandbox/payment_pdf_url_probe.py 2026 2`
   (Should find the February 2026 PDF, which we know exists.)

2. Adjust the probe to use the http_helper from DAIL-026 (currently uses requests directly).

3. Confirm the loop is sequential with early exit (stops on first 200), not parallel.

4. Add structured logging per candidate: URL, status, time. Use json-formatted log lines for easy parsing later.

5. Run for a known-not-yet-published PDF (e.g. April 2026 with today's date if it's not yet published) and confirm the probe correctly returns None with diagnostic logging.

Don't make this parallel.
Don't add features beyond verification.

Reference: pipeline_sandbox/payment_pdf_url_probe.py.
```

#### What NOT to do
- Don't make the probe parallel.
- Don't add new features.

---

### DAIL-105 — Wider lag-window fallback

- **Estimate:** 2h · **Priority:** P1 · **Dependencies:** DAIL-104 · **Labels:** phase-2, discovery

#### Description
Tier 2 spread covering +25 to +60 days from data-month-end for outlier publications.

#### Acceptance criteria
- [ ] Tier 2 logic exists.
- [ ] Triggers only after Tier 1 misses.
- [ ] Adds ~30 candidate URLs at most.

#### References
- `pipeline_sandbox/payment_pdf_discovery_notes.md` (Strategy 3)

#### Helper prompt
```
Add Tier 2 (wider window) to the probe in payment_pdf_url_probe.py.

Logic:
- After Tier 1 (existing 8-candidate priority list) returns no match
- Generate Tier 2 candidates: +25 to +60 days from data-month-end, excluding offsets already in Tier 1
- HEAD-check each in date order (earliest first, since faster publication is more likely than 60-day late)
- Stop on first 200

Tier 2 should NOT trigger if Tier 1 found a match.

Don't change Tier 1 logic.
Don't expand beyond +60 days.

Reference: pipeline_sandbox/payment_pdf_discovery_notes.md Strategy 3.
```

---

### DAIL-106 — Diagnostic-failure handling

- **Estimate:** 1h · **Priority:** P1 · **Dependencies:** DAIL-105 · **Labels:** phase-2, discovery, ops

#### Description
Distinguish "within expected lag window, not yet published" from "past expected window, pattern probably broke."

#### Acceptance criteria
- [ ] Probe returns a structured result indicating which case applies.
- [ ] If past expected window with no match, an issue is auto-opened.
- [ ] If within window, log normally and exit clean.

#### Helper prompt
```
Add diagnostic-failure handling to the probe.

After all tiers miss, classify:
- If today is within the expected lag window (data_month_end + 25 to data_month_end + 60): log "not yet published" and return None cleanly.
- If today is past the lag window (>60 days after data_month_end): log "URL pattern probably broke" and (in production mode) trigger a GitHub Issue auto-creation per DAIL-011.

Return a structured ProbeResult dataclass:

    @dataclass
    class ProbeResult:
        url: str | None
        status: Literal["found", "not_yet_published", "pattern_broken"]
        candidates_tried: int
        elapsed_seconds: float

Don't open issues from sandbox runs; only when invoked from production refresh.
Don't conflate the two missing cases — they're different failure modes.

Reference: pipeline_sandbox/payment_pdf_discovery_notes.md Strategy 4.
```

---

### DAIL-107 to DAIL-109 — Probe tests (HANDS_OFF Phase A)

- **Estimate:** 2h each (6h total) · **Priority:** P1 · **Dependencies:** DAIL-103, DAIL-105 · **Labels:** phase-2, tests

#### Description
Three test files: construction tests (offline), index-parsing tests (fixture), mocked-HTTP orchestration tests.

#### Acceptance criteria
- [ ] `test/test_probe_construction.py` — known-historical-URL coverage.
- [ ] `test/test_probe_index.py` — fixture HTML parsing.
- [ ] `test/test_probe_orchestration.py` — mocked HTTP, all four strategy paths covered.

#### Helper prompt (combined)
```
Write three test files for the discovery probe (DAIL-107/108/109):

1. test/test_probe_construction.py — offline tests:
   - Known historical URLs (Feb 2026, Jan 2026, Dec 2025) appear in their respective candidate lists
   - Tier 1 generates exactly the expected priority order
   - Tier 2 covers +25 to +60 day window
   - Both folder variants (psa/, caighdeanOifigiul/) appear

2. test/test_probe_index.py — fixture-based:
   - Use test/fixtures/api/publications_index_parliamentary_allowances.html
   - Confirm parser extracts known publication entries
   - Confirm parser ignores non-payment entries if the fixture has them

3. test/test_probe_orchestration.py — mocked HTTP:
   - Use responses or requests-mock
   - Test: index 200 → returns from index (no HEAD calls)
   - Test: index 403 → falls back to HEAD-spread
   - Test: all 404 within lag window → returns "not_yet_published"
   - Test: all 404 past lag window → returns "pattern_broken"

Reference: test/HANDS_OFF_TEST_PLAN.md Phase A.

Don't hit live network in any of these.
```

---

### DAIL-110 — March 2026 probe validation

- **Estimate:** 1h · **Priority:** P1 · **Dependencies:** DAIL-103, DAIL-104 · **Labels:** phase-2, validation

#### Description
The success criterion: probe returns the known March 2026 URL.

#### Acceptance criteria
- [ ] `python pipeline_sandbox/payment_pdf_url_probe.py 2026 3` returns the correct URL.
- [ ] Whichever strategy fires (index or HEAD-spread), it works.

#### Helper prompt
```
Validate the probe by running it for March 2026:

    python pipeline_sandbox/payment_pdf_url_probe.py 2026 3

Expected URL:
    https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2026/2026-04-30_parliamentary-standard-allowance-payments-to-deputies-for-march-2026_en.pdf

If found via index (Strategy 1): perfect.
If found via HEAD-spread (Strategy 2): also fine, but means index access still has issues.
If not found at all: debug — the URL is known to exist, so something's wrong with the probe.

This is the milestone moment. After this passes, the discovery probe is production-ready.
```

---

### DAIL-111 — Weekly live canary

- **Estimate:** 2h · **Priority:** P1 · **Dependencies:** DAIL-110 · **Labels:** phase-2, ops, ci

#### Description
Schedule a weekly live test that probes for a known-recent PDF. Always emails on completion (success or failure).

#### Acceptance criteria
- [ ] `.github/workflows/live_canary.yml` runs weekly.
- [ ] Marked `@pytest.mark.live`, excluded from per-PR CI.
- [ ] Sends ntfy.sh push or GitHub Issue on failure.
- [ ] Sends a "system alive" beat on success (weekly).

#### References
- v4 §4.7 Tier 3 (ntfy.sh)
- HANDS_OFF Phase A (live canary)

#### Helper prompt
```
Create .github/workflows/live_canary.yml:

- Schedule: weekly Monday 03:00 UTC
- Runs: pytest test/ -m "live" --maxfail=1
- The single live test confirms the probe finds the most recent known PDF (e.g. February 2026 - which we know exists)

On success: send a one-line ntfy.sh push: "live canary OK week ${ISO_WEEK}"
On failure: open GitHub Issue (use the same actions/github-script pattern from DAIL-011)

Setup ntfy: pick a UUID-grade random topic name, save as GitHub secret NTFY_TOPIC.

The test (test/test_live_canary.py with @pytest.mark.live):

    @pytest.mark.live
    def test_probe_finds_recent_known_pdf():
        result = find_payment_pdf(2026, 2)  # known to exist
        assert result.status == "found"
        assert "2026-04-02" in result.url

Don't add this to default pytest collection. The "live" marker excludes it.

Reference: doc/dail_tracker_improvements_v4.md §4.7 Tier 3.
```

---

## Week 6 — Freshness + lobbying auto-export

---

### DAIL-112 — Freshness state file

- **Estimate:** 2h · **Priority:** P1 · **Dependencies:** DAIL-002 · **Labels:** phase-2, ops

#### Description
Per-source freshness state at `data/_meta/source_freshness.json` with last_new_asset_at, expected_cadence_days, warn/fail thresholds.

#### Acceptance criteria
- [ ] State file exists with one entry per source.
- [ ] Helper functions to read/update state.
- [ ] Refresh workflow updates state on success.

#### References
- HANDS_OFF Phase E

#### Helper prompt
```
Create the freshness state file system.

1. data/_meta/source_freshness.json initial structure:

    {
      "payments": {
        "last_new_asset_at": null,
        "last_new_asset_url": null,
        "expected_cadence_days": 35,
        "warn_after_days": 50,
        "fail_after_days": 90
      },
      "attendance": {...},
      "interests_dail": {...},
      "interests_seanad": {...},
      "lobbying": {...}
    }

2. Helper at pipeline/quality/freshness.py:

    def update_freshness(source_name: str, asset_url: str) -> None:
        """Called after a new asset is successfully ingested."""

    def check_freshness(source_name: str) -> Literal["fresh", "warn", "fail"]:
        """Used by the daily SLO check (DAIL-113)."""

3. Wire update_freshness into each source's ingestion path. Only update when a NEW asset is fetched (hash changed).

Don't open issues from this helper; that's DAIL-113.
Don't make the state file user-editable; only the helper modifies it.

Reference: test/HANDS_OFF_TEST_PLAN.md Phase E.
```

---

### DAIL-113 — Daily SLO check job

- **Estimate:** 3h · **Priority:** P1 · **Dependencies:** DAIL-112 · **Labels:** phase-2, ops

#### Description
Scheduled workflow that reads freshness state daily and warns/errors based on age.

#### Acceptance criteria
- [ ] `.github/workflows/freshness_check.yml` runs daily.
- [ ] Reads each source's state, compares age to thresholds.
- [ ] Warn → daily digest entry. Fail → opens an issue.
- [ ] Tested: synthetic stale state triggers the right alert.

#### References
- HANDS_OFF Phase E
- DAIL-112 helper

#### Helper prompt
```
Create .github/workflows/freshness_check.yml.

- Schedule: daily 09:00 UTC
- Run: python -m pipeline.quality.check_freshness_all_sources

The script:
1. Reads data/_meta/source_freshness.json
2. For each source, computes age = now - last_new_asset_at
3. If age > fail_after_days: open Issue (per DAIL-011 pattern)
4. If age > warn_after_days: write to data/_run_summaries/freshness_warnings_<date>.json (the daily digest input)
5. If fresh: log only, no notification

Test:
- test/test_freshness_slo.py with synthetic time travel (use freezegun)
- Synthetic stale state should trigger fail → issue
- Synthetic warn-zone state should write to digest, not open issue

Don't email digests yet; that's a future ticket.
Don't make this run on the same trigger as the refresh; it's independent.

Reference: test/HANDS_OFF_TEST_PLAN.md Phase E.
```

---

### DAIL-114 — Bronze fixture snapshot

- **Estimate:** 3h · **Priority:** P1 · **Dependencies:** none · **Labels:** phase-2, tests

#### Description
Capture a small representative bronze snapshot for end-to-end testing.

#### Acceptance criteria
- [ ] `test/fixtures/e2e/bronze/` contains one cycle's worth of source data.
- [ ] Total size <50 MB.
- [ ] Includes one PDF per parser + one API JSON per endpoint.

#### References
- HANDS_OFF Phase F

#### Helper prompt
```
Create a bronze fixture snapshot for end-to-end tests.

Pick:
- Latest payments PDF (already in test/fixtures/payments/ from DAIL-014 — copy)
- Latest attendance PDF
- One latest interests PDF (Dáil)
- One sample lobbying CSV (smallest representative)
- One Oireachtas API response per endpoint

Place under test/fixtures/e2e/bronze/ mirroring the data/bronze/ structure.

Total size budget: 50 MB. If over, trim PDFs as in DAIL-016.

Add test/fixtures/e2e/README.md explaining what's there and when last refreshed.

Don't include real lobbying data with personal info beyond what's already public.
Don't commit to git LFS unless absolutely necessary.
```

---

### DAIL-115 — End-to-end smoke test

- **Estimate:** 3h · **Priority:** P1 · **Dependencies:** DAIL-114 · **Labels:** phase-2, tests

#### Description
Test that runs the pipeline against the fixture and asserts gold output matches expected.

#### Acceptance criteria
- [ ] `test/test_pipeline_e2e.py` exists, marked `@pytest.mark.e2e`.
- [ ] Runs in nightly CI, not per-PR.
- [ ] Passes against committed fixtures.
- [ ] Failure produces actionable diff output.

#### References
- HANDS_OFF Phase F

#### Helper prompt
```
Write test/test_pipeline_e2e.py:

    @pytest.mark.e2e
    def test_full_pipeline_against_fixture(tmp_path):
        # Copy bronze fixture to tmp_path
        # Run pipeline in fixture mode (set DAIL_FIXTURE_MODE=1)
        # Compare resulting gold to test/fixtures/e2e/expected_gold/
        # Ignore manifest fields that vary (run_id, git_commit, timestamps)

You'll need to add a DAIL_FIXTURE_MODE flag to pipeline.py that:
- Reads bronze from a configurable path (the tmp_path)
- Writes silver/gold to that same tmp_path
- Doesn't touch real data/

Add to nightly CI (`.github/workflows/nightly.yml` if not already present):

    pytest test/ -m "e2e and not live"

Don't add this to per-PR CI; too slow.
Don't compare every byte — exclude varying fields explicitly.

Reference: test/HANDS_OFF_TEST_PLAN.md Phase F.
```

---

### DAIL-116 — Investigate lobbying.ie XHR endpoints

- **Estimate:** 2h · **Priority:** P1 · **Dependencies:** none · **Labels:** phase-2, lobbying

#### Description
Open browser DevTools during a manual lobbying.ie export. Identify if a clean XHR endpoint can be called directly (preferable to Playwright).

#### Acceptance criteria
- [ ] Investigation result documented in `pipeline_sandbox/lobbying_export_findings.md`.
- [ ] If XHR endpoint exists: documented with example request/response.
- [ ] If not: confirmed Playwright is needed.

#### References
- v4 §3.4 (lobbying.ie cadence note)

#### Helper prompt
```
Investigate lobbying.ie's export mechanism.

1. Open https://www.lobbying.ie/app/home/search in a browser.
2. Open DevTools → Network tab.
3. Manually perform an export (the same one I do today).
4. Watch the network traffic. Look for XHR/Fetch requests that return CSV-formatted data.
5. Document:
   - The endpoint URL
   - The HTTP method (GET/POST)
   - The query params or POST body
   - Response format
   - Any cookies/auth required

If a clean XHR exists: write up the findings — DAIL-117 will use direct HTTP calls, no Playwright needed.

If everything's bundled into a server-rendered HTML page or requires complex session state: Playwright is the path. Document why.

Save findings at pipeline_sandbox/lobbying_export_findings.md.

Don't try to actually call the endpoint from code yet; that's DAIL-117.
```

---

### DAIL-117 — Implement lobbying export job

- **Estimate:** 4h · **Priority:** P1 · **Dependencies:** DAIL-116 · **Labels:** phase-2, lobbying

#### Description
Either direct XHR call or Playwright script. Output CSVs to `data/bronze/lobbying/`.

#### Acceptance criteria
- [ ] Script exists at `pipeline/sources/lobbying.py:download_export`.
- [ ] Output CSV matches the format I download manually.
- [ ] Runs without manual intervention.
- [ ] Marked best-effort: failure doesn't block other sources.

#### References
- DAIL-116 findings

#### Helper prompt
```
Implement lobbying.ie auto-export based on DAIL-116 findings.

Path A (XHR endpoint exists):
- Use http_get from pipeline/sources/_http.py
- Send the request with the right params (from the findings doc)
- Save response to data/bronze/lobbying/lobbying_<isodate>.csv

Path B (Playwright needed):
- Add playwright as an optional dependency
- Headless browser script that navigates to the search page, clicks export, downloads CSV
- Save to same path
- Run in CI with the Playwright GitHub Action

Either way:
- Mark this source as best-effort in the refresh workflow (the workflow continues on failure)
- Update freshness state file (DAIL-112) on success

Don't violate ToS; check robots.txt first.
Don't add interactivity; this must run unattended in CI.

Reference: pipeline_sandbox/lobbying_export_findings.md.
```

---

### DAIL-118 — Wire lobbying into refresh workflow

- **Estimate:** 3h · **Priority:** P1 · **Dependencies:** DAIL-117 · **Labels:** phase-2, lobbying, ops

#### Description
Add lobbying export step to the refresh workflow with `continue-on-error: true`.

#### Acceptance criteria
- [ ] Step added to `.github/workflows/refresh.yml`.
- [ ] Failure of this step doesn't block other sources.
- [ ] Healthchecks lobbying ping fires only on success.

#### Helper prompt
```
Add lobbying export to .github/workflows/refresh.yml.

Place the step after the other sources, with continue-on-error: true:

    - name: Lobbying.ie export
      continue-on-error: true
      id: lobbying
      run: python -m pipeline.sources.lobbying

    - name: Healthchecks ping (lobbying)
      if: steps.lobbying.outcome == 'success'
      run: curl -fsS https://hc-ping.com/${{ secrets.HEALTHCHECK_LOBBYING_UUID }}

So a lobbying failure doesn't block the rest of the workflow but is still surfaced via Healthchecks.

Don't make lobbying failure block other sources.
Don't ping Healthchecks on failure.
```

---

### DAIL-119 — Validate auto-export against last manual

- **Estimate:** 2h · **Priority:** P1 · **Dependencies:** DAIL-117 · **Labels:** phase-2, lobbying

#### Description
Compare auto-exported CSV to the most recent manual one. Confirm equivalent (modulo timestamp).

#### Acceptance criteria
- [ ] Comparison script run.
- [ ] Auto-export matches manual export row-for-row.
- [ ] Any discrepancies documented.

#### Helper prompt
```
Compare the auto-exported lobbying CSV (DAIL-117) to my last manual export.

1. Identify the most recent manual export at data/bronze/lobbying_csv_data/.
2. Run the auto-export.
3. Compare:
   - Row count (should match within ~1% if timing is similar)
   - Column count + names (should be identical)
   - Sample 10 random rows by primary_key — should match exactly

If they don't match:
- Different column → format mismatch in DAIL-117
- Missing rows → filtering issue
- Extra rows → maybe new returns since last manual export (good thing)

Document the comparison result and any discrepancies in pipeline_sandbox/lobbying_export_findings.md.

Don't ship auto-export to production unless this check passes.
```

---

### DAIL-120 — Second journalist follow-up (optional)

- **Estimate:** 1h · **Priority:** P2 · **Dependencies:** DAIL-035 · **Labels:** phase-2, outreach

#### Description
If first journalist hasn't responded by Day 30 (~10 days since contact), send a polite ping.

#### Helper prompt
```
Help me draft a follow-up to the journalist from DAIL-035 if they haven't responded after 10 days.

One paragraph max. Acknowledge they're busy. Re-share the URL. No pressure.

Subject: Re: <original subject>

Body draft:
"Hi [name], just a quick follow-up on the tool I shared 10 days ago — no pressure to respond, but happy to walk you through any specific question if you're interested. URL again: <link>. Best, <me>"

Don't send if they've already replied.
Don't follow up more than once.
```

---

## Week 7 — Pick one track

These are alternatives. Pick **one** based on whether the journalist responded and what they asked for.

### Track A — One new dataset (DAIL-130 to DAIL-135, total ~25h)

If journalist asked for it. Most likely: SIPO donations or judicial appointments. See `ENRICHMENTS.md` §A.1 or §D.1 for the data source detail. Each ticket follows the same shape:

- DAIL-130 (4h): Build scraper using new interface
- DAIL-131 (8h): Parse PDFs (annual reports format)
- DAIL-132 (3h): Schema validation + silver writer
- DAIL-133 (4h): Gold mart for the new dataset
- DAIL-134 (3h): New page contract + page implementation
- DAIL-135 (3h): Tests + methodology entry

#### Helper prompt template (use for DAIL-130–135)
```
I'm adding [SIPO donations / judicial appointments / other] as a new source. Reference: ENRICHMENTS.md §[A.1 / D.1 / etc].

Plan the work in 6 sub-steps:
1. Scraper that finds new assets (use pipeline/sources/_http.py from DAIL-026)
2. Parser for the source format (PDF / API / CSV)
3. Silver writer with schema validation (use the pandera pattern from test/TEST_SUITE.md)
4. Gold mart SQL view in sql_views/
5. Page contract + Streamlit page (follow the existing 8-page patterns in utility/pages_code/)
6. Methodology.md entry for the new dataset

For each step, give me a one-paragraph plan, the files I'll touch, and the acceptance criterion. Don't write code yet — let me approve the plan first.

Don't reuse existing source patterns blindly; new dataset may have different cadence/format.
Don't add UI features beyond the standard page pattern.
```

### Track B — Pluggable scraper interface refactor (~25h)

Don't do this if Track A is happening — the new dataset triggers the refactor naturally. Defer Track B until adding the second new source.

### Track C — Trust hardening (DAIL-140 to DAIL-143, total ~25h)

If no journalist response yet. Items: per-page caveat banners, update history per dataset, public CHANGELOG.md, methodology external review. See v4 §13.

---

## Week 8 — Polish + handover prep

---

### DAIL-150 — Refresh calendar doc

- **Estimate:** 3h · **Priority:** P1 · **Dependencies:** none · **Labels:** phase-2, docs, sustainability

#### Helper prompt
```
Write doc/refresh_calendar.md.

Per source:
- When does the source publish?
- What's the typical lag from data-period-end to publication?
- When does our refresh fire?
- Who at the source can be contacted if it breaks (email, project page)?
- What does typical breakage look like (URL change, format change, schema drift)?

Sources to cover: payments, attendance, interests, lobbying, plus any others added in Week 7.

Tone: written for future-me coming back after a break.

Don't include implementation details.
Don't make it longer than 2 pages.

Reference: doc/dail_tracker_improvements_v4.md §15.2.
```

---

### DAIL-151 — ENRICHMENTS.md URL cleanup

- **Estimate:** 2h · **Priority:** P2 · **Dependencies:** none · **Labels:** phase-2, docs

#### Helper prompt
```
Resolve the parked URL verification list at the top of doc/ENRICHMENTS.md.

For each "broken — re-search needed" row: do a web search, find the correct URL, update the relevant card and the verification list. If still can't find it, mark "still broken".

For 403 entries: open in a browser. If the URL works for human users, mark "verified working in browser; bot-blocked". If not, treat as broken.

For unchecked entries: do a quick browser-or-WebFetch check, mark accordingly.

After: the verification status section should be empty (or just a note "all URLs verified as of <date>").

Don't expand any source cards beyond URL fixes.
```

---

### DAIL-152 — Handover note

- **Estimate:** 5h · **Priority:** P1 · **Dependencies:** none · **Labels:** phase-2, docs, sustainability

#### Helper prompt
```
Write doc/handover.md — the document a new maintainer reads if I stopped tomorrow.

Sections:
1. What this project is (1 paragraph, link to v4)
2. How it runs today (auto-refresh schedule, deploy URL, alarm channels)
3. Where the secrets live (GitHub repo secrets list — don't include values)
4. Known fragile parsers (which PDFs are most likely to break)
5. Open issues by priority (link to GitHub Issues with current labels)
6. The "first day as new maintainer" sequence (clone, install, run a refresh, ship a small change)
7. Who to contact at upstream sources (from refresh_calendar.md)

This will never be read by me. It exists for someone else.

Reference: doc/dail_tracker_improvements_v4.md §15.4.

Don't include code snippets longer than 5 lines.
Don't include sensitive data.
```

---

### DAIL-153 — Clean-room rebuild dry run

- **Estimate:** 5h · **Priority:** P1 · **Dependencies:** all of Phase 1 · **Labels:** phase-2, sustainability

#### Helper prompt
```
Run a clean-room rebuild to confirm sustainability:

1. Blow away local data/ entirely (back it up first).
2. Delete .venv.
3. Fresh clone of the repo into a temp dir.
4. Run the bootstrap process (whatever DAIL-153 dependencies say it should be).
5. Run python pipeline.py --refresh.
6. Run pytest test/ -m "not live and not e2e".
7. Run streamlit run utility/app.py and verify the dashboard loads.

Anything that breaks is a sustainability bug. Open an issue per breakage with label `sustainability` and fix in priority order.

Don't paper over breakages; the point is to find them.
```

---

### DAIL-154 — Track C overflow / final iteration / planning

- **Estimate:** 8h split · **Priority:** P2

#### Helper prompt
```
End-of-Phase-2 catch-all:
- Anything from Track C in Week 7 that didn't ship
- Anything the journalist surfaced that wasn't critical
- Plan the next 8 weeks (toward Plateau 2)

Read doc/dail_tracker_improvements_v4.md §18 Phase 2 sequence and pick the top 4 items to schedule next based on:
- What the journalist asked for (if applicable)
- What the discovery probe revealed (if applicable)
- Plateau 2 priorities from the prioritisation doc

Update doc/SHORT_TERM_PLAN.md or write a new SHORT_TERM_PLAN_NEXT_8_WEEKS.md.

Don't expand scope. Don't promise the journalist anything beyond the next 4 weeks.
```

---

# Cron-readiness audit (2026-05-05)

Surfaced during the pipeline data-freshness audit on 2026-05-05. These tickets close the gap between "interactive pipeline that produces a good gold layer when run by hand" and "scheduled job whose green run actually means new data landed." Cross-referenced from `doc/DATA_LIMITATIONS.md` §12.1.

Order is roughly highest blast-radius first. `DAIL-163` and `DAIL-164` block the rest from being meaningful — until the pipeline reports failures honestly, every other ticket is hidden behind silent green runs.

---

### DAIL-160 — Replace `output_exists` skip in Oireachtas API steps with windowed refetch

- **Estimate:** 4h · **Priority:** P0 · **Dependencies:** none · **Labels:** cron-readiness, refresh
- **Affected files:** `services/oireachtas_api_main.py`, `services/storage.py`, `services/votes.py`

#### Description
`services/oireachtas_api_main.run_member_scenario` and `run_votes` short-circuit when the output JSON already exists. On a recurring schedule this means members, legislation, questions, and votes never refresh. Replace the all-or-nothing skip with an explicit "always refetch the last N days, merge by primary key" mode.

#### Acceptance criteria
- [ ] Each scenario accepts a refresh window (default 30 days) instead of an `overwrite` boolean only.
- [ ] Existing per-output JSON merge keys identified per dataset (`voteId` for votes, `billNo+billYear` for legislation, `memberCode` for members, `questionId` for questions).
- [ ] Re-running the pipeline twice in succession against unchanged upstream produces a row-identical gold layer (idempotent).
- [ ] Re-running with an upstream change inside the window updates the affected rows; rows outside the window are preserved.
- [ ] CLI flag `--full-refresh` retained for clean-room rebuilds.

#### References
- `doc/DATA_LIMITATIONS.md` §12.1 (cron-staleness traps)
- existing `services/oireachtas_api_main.py:21,40` for the skip pattern

---

### DAIL-161 — Auto-discover new PSA payment / attendance / interests PDFs

- **Estimate:** 6h · **Priority:** P1 · **Dependencies:** DAIL-160 · **Labels:** cron-readiness, refresh
- **Affected files:** `pdf_endpoint_check.py`, `pdf_backfill_scraper.py`, `pipeline_sandbox/payment_pdf_url_probe.py`

#### Description
`pdf_endpoint_check.py` is a hand-maintained URL list. `pipeline_sandbox/payment_pdf_url_probe.py` already implements the construct-then-index-fallback discovery pattern. Promote it out of sandbox and use it to extend the URL list automatically each run; do the same for attendance (annual + ad-hoc) and member interests (annual). The new Iris poller (`pipeline_sandbox/iris_oifigiuil_poller.py`) is the template.

#### Acceptance criteria
- [ ] Monthly run discovers a newly-published PSA payment PDF without code edit.
- [ ] Member-interests register PDF (Dáil + Seanad) discovered automatically when published in late February.
- [ ] Discovery failure (every candidate URL 4xx) emits a structured warning and a non-zero step exit code.
- [ ] No regression in `pdf_downloader.py` idempotency.
- [ ] `member_interests.PDF_PATHS` rebuilt from the discovered file list, not hand-maintained.

---

### DAIL-162 — Detect and ingest re-issued PDFs at the same URL

- **Estimate:** 3h · **Priority:** P1 · **Dependencies:** none · **Labels:** cron-readiness, integrity
- **Affected files:** `pdf_downloader.py`, new `data/bronze/pdfs/_checksums.json`

#### Description
`pdf_downloader.py` skips when the destination filename exists. The Oireachtas occasionally re-publishes a corrected PDF at the same URL. Today the corrected version is never ingested.

#### Acceptance criteria
- [ ] On every download attempt, HEAD the URL and compare `Content-Length` and `Last-Modified` (and/or `ETag`) against the stored checksum manifest.
- [ ] On change, re-download and replace; archive the previous bytes to `data/bronze/pdfs/_archive/<original-stem>__<old-fingerprint>.pdf`.
- [ ] Log a structured `PDF_REISSUE_DETECTED` line so the cron output surfaces it.
- [ ] Manifest write is atomic (`.part` rename), as in the Iris shard pattern.

---

### DAIL-163 — `pipeline.py` continues past per-step failures with summary exit code

- **Estimate:** 2h · **Priority:** P0 · **Dependencies:** none · **Labels:** cron-readiness, ops
- **Affected files:** `pipeline.py`

#### Description
The orchestrator currently `break`s on the first failure (`pipeline.py:44`), which means one flaky step poisons every downstream step for that run. On a cron, this turns a transient single-source error into a full-pipeline outage.

#### Acceptance criteria
- [ ] Replace the `break` with `continue`; collect failures into the existing `broken_steps` list.
- [ ] Wrap the loop in a `main()` function and only run on `if __name__ == "__main__":` so importing the module no longer triggers the pipeline.
- [ ] Exit code: `0` if all steps succeeded, non-zero if any failed.
- [ ] Final stdout summary lists succeeded vs failed steps.
- [ ] Manifest is closed exactly once per run regardless of failures.

---

### DAIL-164 — Gate `pipeline.py` on the endpoint checker's broken-URL signal

- **Estimate:** 2h · **Priority:** P1 · **Dependencies:** DAIL-163 · **Labels:** cron-readiness, ops
- **Affected files:** `pdf_endpoint_check.py`, `pdf_downloader.py`, `pipeline.py`

#### Description
`endpoint_checker` returns a list of broken URLs but `pdf_downloader.py` does not surface that list to `pipeline.py`. A run where every PDF URL 404s currently still produces a green pipeline.

#### Acceptance criteria
- [ ] `pdf_downloader.py` exits non-zero when more than X% of URLs are broken (X configurable; default 10%).
- [ ] Broken URL list serialised to `logs/endpoint_check_<run_id>.json` for the manifest to reference.
- [ ] `pipeline.py` summary surfaces the count of broken URLs as a top-level line item alongside step status.

---

### DAIL-165 — Promote Iris incremental shards out of sandbox

- **Estimate:** 4h · **Priority:** P1 · **Dependencies:** none · **Labels:** cron-readiness, performance
- **Affected files:** `iris_oifiguil_etl.py`, `pipeline_sandbox/iris_incremental_shards.py`, `pipeline.py`

#### Description
The active Iris ETL re-extracts every PDF on every run. `pipeline_sandbox/iris_incremental_shards.py` demonstrates per-PDF parquet shards keyed on `(mtime_ns, size, EXTRACTOR_VERSION)`. Wire it into `iris_oifiguil_etl.py` as documented in that file's INTEGRATION SKETCH section.

#### Acceptance criteria
- [ ] Cold cache run is byte-identical to the current run output (modulo deterministic sort order).
- [ ] Warm cache run skips PyMuPDF extraction for unchanged PDFs (verified via stdout step counter).
- [ ] Bumping `EXTRACTOR_VERSION` triggers a full re-stage on the next run.
- [ ] Atomic `.part` writes preserve the cache through interrupted runs.
- [ ] Wire-up adds an `iris_oifigiuil_poller` step to `pipeline.py STEPS` so the cron picks up new issues before extraction.

---

### DAIL-166 — Per-source freshness manifest at gold

- **Estimate:** 3h · **Priority:** P1 · **Dependencies:** DAIL-160, DAIL-161 · **Labels:** cron-readiness, ui, ops
- **Affected files:** `manifest.py`, `enrich.py`, `data/gold/_freshness.json`

#### Description
`manifest.py` records run start/end only. There is no per-dataset "last fetched from upstream" timestamp, so neither the UI provenance footer nor a monitoring job can distinguish "data is fresh" from "this run did nothing because every source short-circuited."

#### Acceptance criteria
- [ ] Each source step writes a freshness entry: `{ "source": "...", "fetched_at": "...", "rows_in": N, "rows_out": M, "fingerprint": "..." }`.
- [ ] Aggregated into `data/gold/_freshness.json` at end of run.
- [ ] Streamlit provenance footers read from this file rather than file mtimes.
- [ ] Stale sources (>30d for monthly cadence, >2d for Iris) flagged in the UI.

---

### DAIL-167 — Lobbying acquisition automation

- **Estimate:** see existing track DAIL-116..DAIL-119 · **Priority:** P1 · **Labels:** cron-readiness, refresh

#### Description
Cross-reference only — the existing `DAIL-116` (XHR investigation) and `DAIL-117`–`DAIL-119` (export job + workflow + validation) cover this work. Listed here so the cron-readiness audit is complete in one place.

---

# Plateau 2 — Mature single-purpose tool (epics, not full tickets)

These are at "epic-level detail" — expand into full tickets when you reach them. Each maps to a v4 section.

| Epic | Hours | Priority | v4 Reference |
|---|---|---|---|
| **DAIL-200**: Tidy-up + dead code removal | 16 | P1 | (no v4 section; cleanup) |
| **DAIL-210**: Full CI/CD beyond minimum | 11 | P1 | v4 §6, §10 |
| **DAIL-220**: Dim/fact/bridge data modelling refactor | 37 | P1 | v4 §7 |
| **DAIL-230**: UI maturity (cross-page nav, search, mobile, accessibility) | 39 | P1 | v4 §9.6–9.9 |
| **DAIL-240**: Distribution (RSS, DuckDB-WASM, permalinks, citation) | 21 | P1 | v4 §12 |
| **DAIL-250**: Trust hardening (full methodology, banners, changelog, external review) | 21 | P1 | v4 §13 |
| **DAIL-260**: Developer experience (bootstrap, CONTRIBUTING, module discipline) | 13 | P1 | v4 §14 |
| **DAIL-270**: Pluggable scraper interface | 16 | P2 | v4 §4.6 |
| **DAIL-280**: One full new dataset (SIPO donations, exemplar) | 38 | P1 | ENRICHMENTS §A.1 |
| **DAIL-290**: dbsect debate-payload integration | 21 | P2 | `pipeline_sandbox/dbsect_integration_plan.md` |

#### Plateau 2 helper prompt template (paste when expanding any epic)
```
I'm expanding the epic DAIL-XXX [name] from doc/TICKETS.md into full tickets.

The epic is referenced from v4 §[N]. Read that section.

Break the epic into 4-8 sub-tickets. For each:
- Title and short description (1-2 sentences)
- Estimate (focused work hours)
- Priority (P1/P2)
- Dependencies (other DAIL-XXX tickets)
- Acceptance criteria (3-5 checkboxes)
- Reference (v4 section + any relevant sandbox/test docs)
- Helper prompt for Claude (10-15 lines, self-contained)
- What NOT to do (2-3 bullets)

Match the format used by Phase 1/Phase 2 tickets above.

Don't expand other epics in the same response; one at a time.
Don't change the total hour estimate at the epic level (the breakdown should sum to roughly the epic's hours).
```

---

## Per-prompt usage guide

### When to use a ticket's helper prompt

Paste the prompt into a fresh Claude session at the start of working on the ticket. Replace `[bracketed]` placeholders. Always include: "Read [the references] first; don't write code until I confirm the plan."

### When to NOT use a helper prompt

- If you're debugging something specific that doesn't fit the ticket scope
- If you've already started and just want a code review
- If the prompt would distract from the actual work

### Prompts that need iterating

The helper prompts here are starting points. After 1–2 sessions on a similar task, you'll know what works for you and the prompts can shrink.

### Reviewing Claude's output

Per v4 §17.4: every AI-generated change is reviewed as a diff before merge. Especially when Claude seems confident. The faster you ship with AI assistance, the more important diff review becomes.

---

## Cross-references

- Plan: `doc/SHORT_TERM_PLAN.md` (the reading order for these tickets)
- Architecture: `doc/dail_tracker_improvements_v4.md`
- Future datasets: `doc/ENRICHMENTS.md`
- Test plan: `test/HANDS_OFF_TEST_PLAN.md`
- Existing test suite: `test/TEST_SUITE.md`
- Civic-data project learnings: `pipeline_sandbox/learnings_from_civic_data_projects.md`
