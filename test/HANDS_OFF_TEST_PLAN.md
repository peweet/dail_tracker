# Hands-off test plan

_Companion to [`TEST_SUITE.md`](TEST_SUITE.md). That doc covers the existing Pandera schema and unit test infrastructure for the pipeline's outputs. **This doc covers the additional testing and alerting needed to make the pipeline genuinely hands-off** for months at a time, including the discovery probe, drift detection, and email notifications._

The tests in `TEST_SUITE.md` confirm that a pipeline run produced valid data. The tests in this doc confirm that **a pipeline run that didn't happen, didn't fail loudly, or quietly produced wrong data will reach the maintainer's inbox** rather than rotting in silence.

---

## Goal

> *Build a pipeline that can run for 12+ months untouched, where the only reasons a human gets pulled in are: (a) something demonstrably broke, (b) a known periodic event (election, slug change), or (c) an explicit feature request.*

For that to be true, the test suite has to do more than verify a single run produced clean data. It has to:

1. Verify the **discovery layer** finds new PDFs as they're published.
2. Verify the **parsers** still produce the same output for the same input (no silent drift).
3. Verify the **silver schema** doesn't drift — column names, types, nullability.
4. Verify the **gold marts** stay within historical row-count bounds.
5. Verify the **pipeline runs to completion** at scheduled cadence — and notify if it doesn't.
6. **Notify a human** when any of the above fails, but **not** when nothing's wrong.

The notification layer is what makes the difference between hands-off and silently-rotting.

---

## Test layers (extending `TEST_SUITE.md`)

`TEST_SUITE.md` covers layers 1–3 below. Layers 4–9 are what this doc adds.

| Layer | Catches | Status | Doc |
|---|---|---|---|
| 1. Unit | Logic bugs in pure functions | ✓ Exists | TEST_SUITE.md |
| 2. Schema (output) | Column drift in pipeline outputs | ✓ Exists | TEST_SUITE.md |
| 3. Cardinality / range | Bad parses (e.g. attendance = 999) | ✓ Exists | TEST_SUITE.md |
| 4. Discovery probe | URL pattern change, index page change | **Missing** | This doc §3 |
| 5. Golden-file parser | Layout-change-induced silent drift | **Missing** | This doc §4 |
| 6. Schema (source) | Upstream API/PDF schema drift | **Missing** | This doc §5 |
| 7. Row-count drift | "30% fewer rows than usual" | **Missing** | This doc §6 |
| 8. Freshness SLO | "No new attendance data for 6 weeks" | **Missing** | This doc §7 |
| 9. End-to-end smoke | Pipeline runs cleanly against fixtures | Partial | This doc §8 |

---

## 3. Discovery probe tests

The probe in [`pipeline_sandbox/payment_pdf_url_probe.py`](../pipeline_sandbox/payment_pdf_url_probe.py) is the single load-bearing component for new-PDF detection. If it silently stops finding new PDFs, the pipeline freezes without any obvious error. **The probe needs more test coverage than anything else in the system.**

### What to test

#### 3.1 URL construction tests (offline, no network)

```python
# Pseudocode shape
def test_payment_url_construction_known_cases():
    """Given a (data_year, data_month) pair, the construction strategy
    should produce URLs whose top candidate matches a known historical URL."""
    candidates = list(construct_candidates(2026, 2))
    expected = (
        "https://data.oireachtas.ie/.../psa/2026/"
        "2026-04-02_parliamentary-standard-allowance-payments-"
        "to-deputies-for-february-2026_en.pdf"
    )
    assert any(c.url == expected for c in candidates[:5])

def test_lag_window_covers_known_outliers():
    """Dec 2025 published 46 days late. Tier 2 must reach +46."""
    candidates = list(construct_candidates(2025, 12))
    offsets = [(c.pub_date - date(2025, 12, 31)).days for c in candidates]
    assert max(offsets) >= 46

def test_both_folder_variants_produced():
    """psa/ and caighdeanOifigiul/ both appear in the candidate list."""
    candidates = list(construct_candidates(2025, 11))
    variants = {c.folder_variant for c in candidates}
    assert {"psa", "alt"} <= variants
```

These run in milliseconds and catch a regression where the candidate set silently shrinks.

#### 3.2 Index-parsing tests (offline, with HTML fixture)

Commit a snapshot of the publications index page as `test/fixtures/publications_index_parliamentary_allowances.html`. The probe's index-parsing function is tested against it:

```python
def test_index_parser_extracts_known_publications():
    fixture = read_fixture("publications_index_parliamentary_allowances.html")
    entries = parse_publications_index(fixture)
    titles = [e.title for e in entries]
    assert any("February 2026" in t for t in titles)
    assert any("January 2026" in t for t in titles)

def test_index_parser_ignores_non_payment_topics():
    """If the index ever leaks an attendance entry into the payments topic,
    the parser should filter it out."""
    fixture = read_fixture("publications_index_with_mixed_topics.html")
    entries = parse_publications_index(fixture, expected_topic="parliamentary-allowances")
    for e in entries:
        assert "attendance" not in e.title.lower()
```

When Oireachtas redesigns the index page, this fixture-based test fails on the next test run rather than silently in production. **That's the value: failures show up in CI, not three weeks later.**

#### 3.3 Probe-orchestration tests (mocked HTTP)

Use `responses` or `requests-mock` to simulate the full probe flow:

```python
def test_probe_uses_index_first_when_available(mocked_http):
    """When the index returns a known entry, the probe uses it without
    making any HEAD requests to the construction candidates."""
    mocked_http.get(PUBLICATIONS_INDEX, body=fixture("index_with_march_2026.html"))
    url = find_payment_pdf(2026, 3)
    assert url == EXPECTED_MARCH_2026_URL
    assert mocked_http.head_call_count == 0

def test_probe_falls_back_to_head_spread_on_index_403(mocked_http):
    mocked_http.get(PUBLICATIONS_INDEX, status=403)
    mocked_http.head(EXPECTED_MARCH_2026_URL, status=200)
    url = find_payment_pdf(2026, 3)
    assert url == EXPECTED_MARCH_2026_URL
    assert mocked_http.head_call_count >= 1

def test_probe_returns_none_when_pdf_not_published_yet(mocked_http):
    """All candidates 404; the probe returns None cleanly, not raising."""
    mocked_http.get(PUBLICATIONS_INDEX, body=fixture("index_without_target.html"))
    mocked_http.head(any_url, status=404)
    url = find_payment_pdf(2026, 12)  # future
    assert url is None
```

#### 3.4 Live probe smoke test (network, opt-in only)

One test that hits live Oireachtas and confirms the probe works end-to-end. Marked `@pytest.mark.live`, excluded from default test runs, executed once a week against production:

```python
@pytest.mark.live
def test_probe_finds_most_recent_known_pdf():
    """Against live Oireachtas, the probe must find the most recent
    PDF that's already in our codebase. If this fails, something
    upstream changed."""
    url = find_payment_pdf(2026, 2)  # known to exist
    assert url is not None
    assert "2026-04-02" in url
```

This is the canary. When it fails, the maintainer is paged.

---

## 4. Golden-file parser tests

`TEST_SUITE.md` mentions these as deferred. They are non-deferrable for hands-off operation. **A PDF layout change is the most likely silent-failure mode, and it has zero protection without golden-file tests.**

### Setup

For each parser (attendance, payments, interests):

1. Pick one representative PDF that has been parsed correctly by the current code.
2. Commit it to `test/fixtures/<source>/<filename>.pdf`.
3. Run the parser against it. Save the output as `test/fixtures/<source>/<filename>.expected.parquet`.
4. Write a test that re-runs the parser and asserts the output matches the fixture exactly.

```python
def test_payment_parser_2026_february_golden_file():
    pdf = Path("test/fixtures/payments/2026-04-02_..._february-2026_en.pdf")
    expected = pl.read_parquet(
        "test/fixtures/payments/2026-04-02_..._february-2026_en.expected.parquet"
    )
    actual = parse_payment_pdf(pdf)
    pl.testing.assert_frame_equal(actual, expected)
```

### Coverage

Per parser, ideally:

- One PDF with the **current layout** (covers the common case).
- One PDF with the **previous layout** that still parses (regression protection).
- One PDF that's **known-malformed** (e.g. `pdf_2024_gap` for attendance — covers gap handling).
- One **empty** PDF (covers degenerate input).

Four fixtures × three parsers = 12 fixture files. Total fixture size probably <50 MB — fine for git.

### When fixtures need updating

Layout change confirmed → update the parser → update the fixture → commit both atomically. The PR diff shows the parsing change next to the data change.

If the diff is large/unexpected: that's the "silent drift caught" moment.

---

## 5. Source-side schema validation

`TEST_SUITE.md` validates *output* schemas (silver/gold). This layer validates *input* schemas — what we receive from upstream — to catch upstream changes immediately.

### Oireachtas API responses

Validate every API response against a JSON schema **at fetch time**, not at parse time. A schema mismatch raises a loud error before any downstream processing starts.

```python
import jsonschema

MEMBERS_API_SCHEMA = {
    "type": "object",
    "required": ["head", "results"],
    "properties": {
        "head": {
            "type": "object",
            "required": ["totalResults"],
            "properties": {"totalResults": {"type": "integer"}}
        },
        "results": {"type": "array", "items": {...}}
    }
}

def fetch_members(...):
    response = http_get(MEMBERS_URL)
    jsonschema.validate(response.json(), MEMBERS_API_SCHEMA)  # raises on drift
    return response.json()
```

Schemas live in `pipeline/schemas/<source>.json`. Validation runs on every fetch.

Test:

```python
def test_members_response_validates_against_committed_schema():
    """Check that a saved sample response (in test/fixtures/api/) validates
    against the schema currently in pipeline/schemas/."""
    sample = json.loads(read_fixture("api/members_sample.json"))
    jsonschema.validate(sample, load_schema("members"))
    # If this fails, either the sample or the schema has drifted —
    # one of them needs updating, atomically.
```

### PDF source schemas

PDFs have no formal schema. The closest equivalent is **shape assertion**: after parsing, the silver DataFrame must have an expected column count and column-name set. Already partly covered in `TEST_SUITE.md` §1; extend to cover the input side by asserting on raw extracted rows before transformation.

---

## 6. Row-count drift assertions

Inside the silver write step, compare current row count against the rolling history:

```python
def write_silver_with_drift_check(df, dataset_name, manifest_path):
    history = load_row_count_history(manifest_path)
    current = len(df)
    if history:
        recent_avg = mean(h["row_count"] for h in history[-5:])
        delta_pct = (current - recent_avg) / recent_avg * 100
        if abs(delta_pct) > 10:
            log.error("ROW_COUNT_DRIFT", dataset=dataset_name,
                      current=current, recent_avg=recent_avg,
                      delta_pct=delta_pct)
            raise RowCountDriftError(...)
    history.append({"run_id": current_run_id, "row_count": current,
                    "delta_pct": delta_pct, "timestamp": utc_now()})
    save_row_count_history(manifest_path, history)
    df.write_parquet(...)
```

Test:

```python
def test_drift_check_raises_on_30pct_drop():
    history = [{"row_count": 1000}] * 5
    with pytest.raises(RowCountDriftError):
        check_drift(current=700, history=history)

def test_drift_check_tolerates_5pct_drift():
    history = [{"row_count": 1000}] * 5
    check_drift(current=950, history=history)  # no raise

def test_drift_check_skips_when_no_history():
    """First run after a fresh start has no history; should not raise."""
    check_drift(current=1000, history=[])  # no raise
```

Tunable threshold per dataset. Critical datasets (master_td_list) tighter; less stable ones (lobbying activities exploded rows) looser.

---

## 7. Freshness SLO tests

The pipeline can run successfully every week and still be silently broken if it's not finding any new data. A pipeline that hasn't ingested a new payment PDF in 12 weeks is broken even if every individual run looked fine.

### What to monitor

Per source, maintain a "last successful new-asset detection" timestamp:

```text
data/_meta/source_freshness.json:
{
  "payments": {
    "last_new_asset_at": "2026-04-02T04:00Z",
    "last_new_asset_url": "https://.../2026-04-02_..._february-2026_en.pdf",
    "expected_cadence_days": 35,
    "warn_after_days": 50,
    "fail_after_days": 90
  },
  "attendance": {...},
  "interests_dail": {...},
  "interests_seanad": {...}
}
```

A scheduled test reads this file daily and:

- **Warn** if `now - last_new_asset_at > warn_after_days`.
- **Fail** (email + open issue) if `now - last_new_asset_at > fail_after_days`.

```python
def test_payment_freshness_slo():
    fresh = load_freshness_state("payments")
    age_days = (utc_now() - fresh["last_new_asset_at"]).days
    if age_days > fresh["fail_after_days"]:
        raise FreshnessError(
            f"No new payment PDF detected for {age_days} days. "
            f"Expected one every {fresh['expected_cadence_days']} days. "
            "Either Oireachtas hasn't published, or the discovery probe is broken."
        )
    elif age_days > fresh["warn_after_days"]:
        warn(...)
```

### Why this catches the silent-rot case

A discovery probe that's been silently blocked by upstream WAF rules for 8 weeks looks identical to a probe that's working but has nothing new to find — *unless* you have an external clock saying "you should have found something by now". Freshness SLOs are that clock.

---

## 8. End-to-end smoke tests

A test that runs the full pipeline (or a meaningful subset of it) against a known-good fixture set and asserts the output matches expected.

### Setup

`test/fixtures/e2e/` contains:

- A snapshot of `data/bronze/` for one cycle.
- The expected `data/silver/` output for that bronze.
- The expected `data/gold/` output for that silver.

Test:

```python
@pytest.mark.e2e
def test_full_pipeline_against_fixture():
    bronze_fixture = Path("test/fixtures/e2e/bronze/")
    expected_gold = Path("test/fixtures/e2e/expected_gold/")

    with temp_dir() as work_dir:
        copy(bronze_fixture, work_dir / "bronze")
        run_pipeline(work_dir, mode="fixture")
        actual_gold = work_dir / "gold"
        assert_directory_trees_equal(actual_gold, expected_gold,
                                      ignore=["manifest.run_id", "manifest.git_commit"])
```

This is slow (~minutes). Run on every PR, but cache the bronze fixture extraction.

When a parser is updated, the expected-gold fixture needs updating in the same PR. The diff is reviewable.

---

## Email notification scheme

This is the part that turns silent failures into the maintainer noticing.

### Severity levels

| Level | Trigger | Email? | Open GH issue? |
|---|---|---|---|
| INFO | Successful run completed | No | No |
| INFO | Probe found nothing new (within expected window) | No | No |
| INFO | New PDF discovered and ingested | No | No |
| WARN | Freshness SLO exceeded warn threshold | Daily digest | No |
| WARN | Row count drift between 5–15% | Per-run summary | No |
| WARN | Probe needed Tier 2/3 fallback (Tier 1 missed) | Per-run summary | No |
| ERROR | Pipeline run failed | Immediate | Yes |
| ERROR | Schema validation failed at silver write | Immediate | Yes |
| ERROR | Golden-file parser test failed in CI | Immediate (CI) | Yes |
| ERROR | Freshness SLO exceeded fail threshold | Immediate | Yes |
| ERROR | Row count drift > 15% | Immediate | Yes |
| ERROR | Live probe smoke test failed (weekly canary) | Immediate | Yes |

### What NOT to email about

This is more important than what to email about. **Email noise destroys email signal.** Specifically don't email on:

- Every successful run (success is uninteresting).
- "No new PDFs this week" (expected outcome).
- A single transient network failure (let retry handle it).
- A test failure on a feature branch (only email on `main`).
- An opened/closed issue (GitHub already does this).

If the maintainer's inbox averages more than one email per week from the pipeline during steady-state operation, the thresholds are wrong.

### Digest vs immediate

Two channels:

- **Immediate** for ERROR conditions. The maintainer should investigate within a day.
- **Daily digest** for WARN conditions. Single email at 09:00 UTC summarising any WARNs from the past 24 hours. Empty? No email.

The daily digest prevents WARN spam while still surfacing slow degradation.

### Email content (template)

```text
Subject: [dail-tracker ERROR] Schema drift detected in payments parser

Run ID: 2026_05_03_001
Time:   2026-05-03T04:14Z
Severity: ERROR

What happened:
  Schema validation failed when writing silver/aggregated_payment_tables.parquet.
  Expected column 'Date_Paid' (type str), found NaN-only values.

Likely cause:
  Upstream PDF layout changed; the parser is reading the wrong column.

Where to look:
  - test/fixtures/payments/<latest>.pdf (compare to the new live PDF)
  - payments.py:53 (Date_Paid extraction)
  - GitHub issue auto-opened: #<number>

Run logs:
  https://github.com/<you>/dail-extractor/actions/runs/<id>

To silence this alert:
  Acknowledge in the open GitHub issue. Subsequent runs against the
  same PDF won't re-email until the issue is closed.
```

The email tells the maintainer **what broke, where to look, and how to silence the alert** while they investigate. Vague alerts get ignored; specific ones get acted on.

### Implementation options

| Option | Pros | Cons |
|---|---|---|
| GitHub Actions native (workflow notifications) | Free, no secrets management | Limited to "workflow failed" — not granular per-source |
| GitHub Actions + custom action (`dawidd6/action-send-mail`) | Granular per-step, full control | Need SMTP credentials in secrets |
| Mailgun / SendGrid free tier | Reliable, generous free quota | Account setup, API keys |
| Plain Python `smtplib` | Zero dependencies | Brittle, often blocked by Gmail |
| **Recommended: GitHub Actions + `dawidd6/action-send-mail` via SendGrid free tier** | Reliable, free, granular | Two services to manage |

Concrete shape (illustrative, not implementation):

```yaml
# .github/workflows/refresh.yml
- name: Notify on error
  if: failure()
  uses: dawidd6/action-send-mail@v3
  with:
    server_address: smtp.sendgrid.net
    server_port: 587
    username: apikey
    password: ${{ secrets.SENDGRID_API_KEY }}
    subject: "[dail-tracker ERROR] Refresh failed: ${{ github.run_id }}"
    to: ${{ secrets.NOTIFY_EMAIL }}
    from: dail-tracker-bot
    body: |
      Run ID: ${{ github.run_id }}
      Logs: ${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}
      […]
```

For digests, a separate scheduled workflow runs daily, reads the past 24h of run summaries (`data/_run_summaries/*.json`), and emails any WARNs.

### Configuration

Email recipients and thresholds in `test/config/notifications.yaml`:

```yaml
recipients:
  - p.glynn18@gmail.com  # primary

thresholds:
  row_count_drift_warn_pct: 5
  row_count_drift_error_pct: 15
  freshness_warn_factor: 1.5  # warn at 1.5× expected cadence
  freshness_error_factor: 3.0 # error at 3× expected cadence

silence_during:
  # Don't email between these times (e.g. holidays); queue for later
  - { from: "2026-12-23", to: "2026-12-30", reason: "Christmas" }
```

---

## CI integration

Three test runs at three cadences:

### Per-PR (every push)

Fast tests only. Should complete in <5 minutes.

```yaml
# .github/workflows/ci.yml
- pytest test/ -m "not e2e and not live" --maxfail=3
```

Includes: unit tests, schema tests, parser-construction tests (offline), drift-check unit tests. **Excludes**: live probe (no network), e2e (slow), parser golden-file (slow).

### Nightly (scheduled at 02:00 UTC)

Full test suite excluding live network calls.

```yaml
# .github/workflows/nightly.yml
- pytest test/ -m "not live" --maxfail=5
```

Includes everything from per-PR, plus golden-file tests, e2e tests, freshness SLO checks. **Email on failure**.

### Weekly (Monday 03:00 UTC)

The live canary. Confirms upstream URLs still work, the index page still returns data we can parse, the probe still finds known-recent PDFs.

```yaml
# .github/workflows/live_canary.yml
- pytest test/ -m "live" --maxfail=1
```

**Always email**, even on success — a one-line "live canary passed, week N" confirms the system is alive. This is the *only* per-run "success" email; everything else is failure-only.

### Per-cron-refresh (scheduled with the data refresh)

Embedded inside the refresh workflow, after the pipeline runs:

```yaml
- name: Validate refresh outputs
  run: pytest test/post_refresh/ -v

- name: Notify if pipeline broke
  if: failure()
  uses: dawidd6/action-send-mail@v3
  ...
```

Tests under `test/post_refresh/` are critical-path-only: schema validation on outputs, row-count drift checks, freshness SLOs. They run after every refresh and email the maintainer if anything failed.

---

## Test data strategy

### Fixtures committed

- Small representative PDFs (<5 MB each, total <50 MB).
- Sample API responses (JSON, ~10 KB each).
- Sample HTML index pages (~100 KB each).
- Expected silver/gold outputs as parquet (small).

All under `test/fixtures/`. Total size budget: <100 MB. If approaching that, switch to git LFS or a separate `test-fixtures` branch.

### Fixtures NOT committed

- Real production data (no — privacy, size).
- Live API responses captured ad-hoc (no — they go stale).
- Anything member-identifying beyond what's already in upstream public records (no — same caveat as the live data).

### Updating fixtures

When a parser is updated:

1. Re-run parser against committed fixture PDF.
2. New output may differ from old expected-output fixture.
3. Update expected-output fixture in the same PR.
4. PR diff shows: parser code change + expected-output change. Reviewer can confirm the change is intentional.

When a new fixture is added (e.g. for a newly observed slug variant):

1. Find a representative example in production.
2. If it contains personal data beyond what's public, anonymise it.
3. Commit PDF + expected output together.
4. Add a comment noting why this fixture exists.

---

## Anti-patterns to avoid

Specifically to call out, because they're common in civic-data projects:

- **Tests that hit live upstream in default test run.** Flaky, slow, makes CI fragile. Live tests are opt-in (`@pytest.mark.live`) and run on schedule, not per-PR.
- **Tests that depend on the current date.** Use `freezegun` or pass dates in. A test that passes on 2026-05-01 and fails on 2026-05-02 is not a test, it's a clock.
- **Tests that depend on file system state across tests.** Use `tmp_path`. Each test gets a fresh directory; tests don't talk to each other.
- **One giant test that does everything.** Hard to debug, slow to run. Per-test scope is per-behaviour.
- **Email on every successful run.** Inbox noise. Success is uninteresting.
- **Test functions named `test_thing`.** Name tests after the behaviour, not the unit (`test_drift_check_raises_on_30pct_drop` vs `test_check_drift`).
- **Mock the function under test.** If you mock the function being tested, you're testing your mock.
- **Tests that pass when the implementation is missing.** A test for `parse_pdf` that uses `monkeypatch.setattr(parse_pdf, lambda x: VALID_OUTPUT)` doesn't test parsing.

---

## Build-out checklist

Phased. Each phase produces a working state — don't try to build everything at once.

### Phase A — discovery probe tests (wraps Phase 1 of the probe build)
- [ ] Commit `test/fixtures/api/publications_index_parliamentary_allowances.html`.
- [ ] Write `test_payment_pdf_url_probe.py` with construction tests (offline).
- [ ] Write index-parsing tests against the fixture.
- [ ] Write probe-orchestration tests with mocked HTTP.
- [ ] Mark live smoke test `@pytest.mark.live` and add weekly schedule.

### Phase B — golden-file parser tests
- [ ] Commit `test/fixtures/payments/2026-04-02_february-2026.pdf` + expected parquet.
- [ ] Commit `test/fixtures/attendance/<one>.pdf` + expected parquet.
- [ ] Commit `test/fixtures/interests/dail/<one>.pdf` + expected parquet.
- [ ] Write `test_payment_parser_golden.py`, `test_attendance_parser_golden.py`, `test_interests_parser_golden.py`.

### Phase C — source schema validation
- [ ] Commit JSON schemas for each Oireachtas API endpoint (`pipeline/schemas/`).
- [ ] Commit one sample API response per endpoint (`test/fixtures/api/`).
- [ ] Wire `jsonschema.validate` into each fetch step.
- [ ] Write `test_api_schemas.py`.

### Phase D — drift detection
- [ ] Implement `write_silver_with_drift_check`.
- [ ] Wire into each silver writer.
- [ ] Write `test_drift_check.py` with synthetic-drift scenarios.
- [ ] Add `data/_meta/row_count_history/` to bronze.

### Phase E — freshness SLOs
- [ ] Implement freshness state file maintenance.
- [ ] Write daily scheduled job that reads state and warns/errors.
- [ ] Write `test_freshness_slo.py` with synthetic time travel.

### Phase F — end-to-end smoke
- [ ] Capture a small bronze snapshot as `test/fixtures/e2e/bronze/`.
- [ ] Run pipeline against it, capture expected gold.
- [ ] Write `test_pipeline_e2e.py` marked `@pytest.mark.e2e`.
- [ ] Add to nightly CI.

### Phase G — email notifications
- [ ] Set up SendGrid (or chosen provider) free tier.
- [ ] Add `SENDGRID_API_KEY` and `NOTIFY_EMAIL` to GitHub repo secrets.
- [ ] Write the `notify-on-failure` action and reuse across workflows.
- [ ] Write the daily-digest workflow.
- [ ] Write the weekly canary email workflow.

### Phase H — silence and tuning
After 4 weeks of operation:
- [ ] Review email frequency — adjust thresholds if too noisy or too quiet.
- [ ] Add silence-during ranges for known-out-of-office periods.
- [ ] Tune freshness SLO factors per source based on observed cadence.

---

## Specific test files to create

Concrete file list, in build order:

```text
test/
  fixtures/
    api/
      members_sample.json
      legislation_sample.json
      votes_sample.json
      publications_index_parliamentary_allowances.html
      publications_index_record_of_attendance.html
      publications_index_register_of_members_interests.html
    payments/
      2026-04-02_..._february-2026.pdf
      2026-04-02_..._february-2026.expected.parquet
    attendance/
      2026-04-02_..._jan-feb-2026.pdf
      2026-04-02_..._jan-feb-2026.expected.parquet
    interests/
      2026-02-25_..._dail-2025.pdf
      2026-02-25_..._dail-2025.expected.parquet
    e2e/
      bronze/...
      expected_gold/...
  config/
    notifications.yaml
  test_payment_pdf_url_probe.py        # Phase A
  test_payment_parser_golden.py        # Phase B
  test_attendance_parser_golden.py     # Phase B
  test_interests_parser_golden.py      # Phase B
  test_api_schemas.py                  # Phase C
  test_drift_check.py                  # Phase D
  test_freshness_slo.py                # Phase E
  test_pipeline_e2e.py                 # Phase F
  post_refresh/
    test_silver_schema_post_refresh.py # critical-path
    test_drift_post_refresh.py         # critical-path
    test_freshness_post_refresh.py     # critical-path
```

---

## Cross-references

- This doc extends [`TEST_SUITE.md`](TEST_SUITE.md) for hands-off operation.
- The probe being tested is [`pipeline_sandbox/payment_pdf_url_probe.py`](../pipeline_sandbox/payment_pdf_url_probe.py).
- Discovery strategy and cadence are in [`pipeline_sandbox/payment_pdf_discovery_notes.md`](../pipeline_sandbox/payment_pdf_discovery_notes.md).
- The architectural shape these tests support is in [`doc/dail_tracker_improvements_v4.md`](../doc/dail_tracker_improvements_v4.md):
  - §4 (rearchitecture) — what runs where
  - §5 (robustness) — schema/golden file/drift testing
  - §10 (observability) — run summaries, freshness SLOs, issue auto-creation
- The civic-data testing patterns lifted into this doc come from [`pipeline_sandbox/learnings_from_civic_data_projects.md`](../pipeline_sandbox/learnings_from_civic_data_projects.md).

---

## What this doc does NOT cover

- **UI/Streamlit testing.** Page rendering tests already in `steamlit_test_example.py`. The bold-redesign skill covers UI review.
- **Performance regression testing.** Out of scope until query times become a problem.
- **Security/penetration testing.** The deployed app is read-only against committed parquet; the attack surface is minimal. Revisit if/when an API or write path is added.
- **Cross-platform testing.** Pipeline runs on Linux in GitHub Actions; local dev runs on whatever the maintainer has. Don't test Windows compatibility unless someone reports a problem.

The point is: testing serves the goal of "hands-off pipeline that emails when something's wrong." Anything outside that scope is deferred until it's needed.
