# dail_tracker Cron-Only Longevity Assessment and Hardening Plan

## Purpose

Assess how long `dail_tracker` could reasonably run unattended if maintained mostly by scheduled cron/GitHub Actions jobs, and identify what must be fixed to make unattended operation safer.

This assumes the project is run from the current committed-data model:

- source pollers / pipeline jobs refresh data;
- generated parquet/metadata are validated;
- refreshed data is committed or published through a controlled process;
- public app reads committed/static parquet and metadata.

This is not an assessment of a fully managed production data platform.

---

## Short Answer

If left mostly alone with cron jobs:

| Scenario | Likely unattended lifespan | Blunt assessment |
|---|---:|---|
| Current state, cron probes only, no auto-publish | 3–6 months | App can remain useful if committed data is already fresh, but sources will age unless refreshed manually. |
| Current state, cron refreshes with manual review before publish | 6–12 months | Realistic and safe if health reports are watched occasionally. |
| Auto-refresh and auto-publish without more guards | 2–8 weeks before a bad/stale/thinned publish risk | Too risky. One parser/source drift can silently degrade data. |
| Hardened cron with health checks, row-count/schema guards, metadata coverage, and PR-based publishing | 12–24 months with light maintenance | Plausible if CRO/charity/manual sources are explicitly handled. |
| Fully unattended indefinitely | Not realistic | Public sources drift, websites change, documents move, and manual review will eventually be needed. |

Best realistic target:

> A mostly unattended system that can run for 6–12 months with only light monthly review, and 12–24 months if cron jobs create reviewable PRs with strong validation instead of publishing directly.

---

## Current Data Health Assumption

The project appears to have several healthy properties already:

- real pipeline orchestration;
- pollers and refresh scripts;
- freshness metadata;
- source coverage metadata;
- parquet gold outputs;
- row-count diff tooling;
- GitHub Actions workflows for CI/freshness/probes;
- structured DuckDB/Parquet app architecture;
- important source caveats for SI legal state and procurement.

The main issue is not whether cron jobs can run.

The main issue is whether cron jobs can run **safely**, detect source drift, avoid publishing bad data, and flag sources that require manual refresh.

---

## Main Risk Areas

### 1. CRO is a manual-refresh risk

CRO enrichment appears to depend on a manually obtained/bulk source file.

Risk:

- CRO data becomes stale;
- corporate/procurement/lobbying entity matches degrade;
- app still looks healthy unless CRO-specific freshness is tracked.

Recommendation:

- mark CRO as `manual_source` in metadata;
- add `cro_latest_source_date`;
- add `cro_days_since_source_refresh`;
- add health warnings if stale;
- do not let CRO staleness fail the whole pipeline unless the page depends critically on it;
- expose CRO freshness separately in UI.

Suggested metadata:

```json
{
  "source": "CRO",
  "refresh_mode": "manual",
  "latest_source_file": "...",
  "latest_source_date": "...",
  "last_normalised_at": "...",
  "status": "manual_refresh_required",
  "staleness_days": 0
}
```

---

### 2. Charity register may also be semi-manual

If charity enrichment depends on a downloaded XLSX/register file, treat it like CRO.

Recommendation:

- classify as manual or semi-manual;
- track latest source file date;
- track normalisation date;
- warn if stale;
- avoid failing unrelated chains.

---

### 3. Freshness coverage is incomplete

Freshness metadata should cover every app-facing dataset, not just the major pipeline families.

Add freshness tracking for:

- payments;
- Seanad payments;
- attendance;
- Seanad attendance;
- interests;
- appointments;
- judicial appointments, if added;
- current judge roster, if added;
- corporate notices;
- CRO xrefs;
- CBI xrefs;
- charity xrefs;
- lobbying;
- lobbying/CRO/charity enrichment;
- procurement awards;
- procurement actual spend / payments if added;
- procurement/lobbying overlap;
- statutory instruments;
- SI current state;
- LRC SI enrichment, if added;
- legislation;
- votes;
- Seanad votes;
- members;
- committees;
- freshness metadata itself.

Recommendation:

- extend `tools/check_freshness.py`;
- add `data/_meta/dataset_health.json`;
- add one row per dataset/source.

---

### 4. Auto-publishing is riskier than auto-running

Running cron jobs is fine.

Publishing their outputs automatically is the risky part.

Risk examples:

- source website changes layout;
- parser returns zero rows;
- cold checkout misses cached bronze files;
- temporary network error produces partial data;
- changed schema silently drops columns;
- upstream source publishes malformed PDF/HTML;
- GitHub runner environment differs from local;
- manual source like CRO is stale but pipeline still succeeds.

Recommendation:

Use this model:

```text
Scheduled cron run
    ↓
Generate refreshed data
    ↓
Run validation checks
    ↓
Create data-refresh branch
    ↓
Open PR with row-count/schema/freshness diff
    ↓
Human review or strict auto-merge only if all checks pass
```

Do not push directly to the public branch.

---

## Expected Longevity By Chain

### Members / Oireachtas base data

Likely unattended durability:

> 6–18 months with endpoint checks.

Risks:

- API schema changes;
- endpoint movement;
- PDF/source link changes;
- member identity edge cases after elections/by-elections.

Cron suitability:

- High.

Needed guards:

- row-count checks;
- schema checks;
- endpoint health checks;
- by-election/new-member detection;
- alert if member count changes unexpectedly.

---

### Votes

Likely unattended durability:

> 6–18 months.

Risks:

- new vote API shape;
- Seanad/Dáil chamber differences;
- missing chamber labels;
- source API delays.

Cron suitability:

- High.

Needed guards:

- Dáil and Seanad row counts separately;
- latest vote date by chamber;
- duplicate division IDs;
- source URL preservation;
- chamber-specific schema tests.

---

### Attendance

Likely unattended durability:

> 3–9 months.

Risks:

- PDF format changes;
- source URL changes;
- Seanad/Dáil differences;
- missing PDFs;
- parser drift.

Cron suitability:

- Medium-high if parser fixtures are strong.

Needed guards:

- latest sitting date by chamber;
- PDF source checks;
- parser fixture tests;
- row-count diff by year/chamber;
- clear UI caveat for missing/unavailable attendance records.

---

### Payments

Likely unattended durability:

> 3–9 months.

Risks:

- PDF format changes;
- publication cadence changes;
- annual/monthly naming changes;
- Seanad/Dáil source differences;
- source tables change layout.

Cron suitability:

- Medium-high.

Needed guards:

- latest payment period by chamber;
- payment category schema checks;
- totals sanity checks;
- row-count checks;
- parser fixture tests;
- no-data vs unavailable state.

---

### Interests

Likely unattended durability:

> 3–12 months.

Risks:

- annual PDF/register format changes;
- names/roles change;
- parser drift;
- source publication delays.

Cron suitability:

- Medium.

Needed guards:

- latest register year;
- expected minimum member coverage;
- parser fixture tests;
- warning if register year is older than expected.

---

### Lobbying

Likely unattended durability:

> 3–9 months, unless wave/date logic is fully automated.

Risks:

- manual `NEXT_WAVE_DATE` style constants;
- large batch/rate-limit issues;
- period/wave publication changes;
- organization matching drift;
- CRO/charity enrichment staleness.

Cron suitability:

- Medium.

Needed guards:

- remove manual wave date if possible;
- derive next expected wave from current date and source data;
- period coverage checks;
- org count checks;
- return count checks;
- warning if current lobbying period is missing;
- separate enrichment freshness from raw lobbying freshness.

---

### Iris Oifigiúil / Appointments / Corporate / SIs

Likely unattended durability:

> 9–18 months.

Risks:

- Iris publication format/link changes;
- PDF/HTML issue structure changes;
- source cadence changes;
- extraction misses an issue;
- SI/corporate notice patterns change.

Cron suitability:

- High.

Needed guards:

- latest Iris issue date;
- issue gap detection;
- expected Tue/Fri cadence check;
- appointment/corporate/SI row counts per issue;
- source PDF/hash preservation;
- extraction confidence flags;
- alert on missed issue.

---

### Statutory Instruments current-state / eISB legal-state layer

Likely unattended durability:

> 6–18 months if freshness-gated.

Risks:

- eISB page layout changes;
- directory structure changes;
- legal-state text changes;
- partial revocation/amendment parsing edge cases;
- unsafe legal wording.

Cron suitability:

- Medium-high.

Needed guards:

- coverage percentage threshold;
- join coverage to gold SI table;
- status distribution diff;
- null/unmatched never treated as in force;
- source URL preservation;
- parser tests for revoked/amended/partially revoked/affected states;
- legal caveat check in UI/tests.

---

### Legislation

Likely unattended durability:

> 6–18 months.

Risks:

- Oireachtas API drift;
- schema changes;
- bill stage changes;
- chamber-specific edge cases.

Cron suitability:

- High.

Needed guards:

- latest bill update date;
- row-count checks;
- status/stage vocabulary drift warnings;
- source URL checks.

---

### Corporate notices

Likely unattended durability:

> 6–12 months.

Risks:

- Iris extraction changes;
- corporate notice text patterns drift;
- CRO/CBI enrichment staleness;
- false-positive entity matches.

Cron suitability:

- Medium-high for notices, lower for enrichment.

Needed guards:

- notice count by issue/month;
- entity extraction confidence;
- CRO freshness;
- CBI source health;
- match-confidence reporting;
- manual-review counts.

---

### CBI enrichment

Likely unattended durability:

> 3–9 months.

Risks:

- ASP.NET/search interface changes;
- PDF/register link changes;
- partial fetch failures;
- register taxonomy changes;
- false matches.

Cron suitability:

- Medium-low.

Needed guards:

- source-specific success/failure counts;
- per-register fetch status;
- row-count checks;
- match-confidence thresholds;
- mark as experimental where appropriate.

---

### CRO enrichment

Likely unattended durability:

> 1–3 months without manual refresh.

Risks:

- manual source download required;
- stale company register file;
- matching quality degrades over time;
- false positives if stale data used.

Cron suitability:

- Low unless source fetch is automated.

Needed guards:

- explicit manual-source warning;
- stale-after threshold;
- do not pretend CRO is fresh;
- fail only CRO enrichment, not whole app.

---

### Charity enrichment

Likely unattended durability:

> 1–6 months, depending on source refresh automation.

Risks:

- manual XLSX download;
- source format changes;
- charity name matching drift;
- stale register.

Cron suitability:

- Low-medium.

Needed guards:

- explicit refresh mode;
- latest file date;
- stale warning;
- match-confidence flags.

---

### Procurement awards / eTenders

Likely unattended durability:

> 3–12 months.

Risks:

- source CSV URL or schema changes;
- large file download timeouts;
- cache path issues;
- value semantics errors;
- supplier privacy/sole-trader handling;
- framework/DPS values mis-summed.

Cron suitability:

- Medium.

Needed guards:

- remove local Windows-style cache paths;
- use repo-local or temp cache;
- schema checks;
- row-count checks;
- latest award date;
- source retrieved date;
- supplier classification checks;
- `value_safe_to_sum` enforced;
- privacy quarantine checks;
- no naive sum in UI.

---

### Procurement/lobbying overlap

Likely unattended durability:

> 6–12 months if procurement + lobbying are healthy.

Risks:

- depends on upstream procurement/lobbying/CRO matching;
- false-positive entity matches;
- users infer causation.

Cron suitability:

- Medium-high as a derived join.

Needed guards:

- source freshness inherited from both sides;
- co-occurrence caveat required;
- match-confidence threshold;
- row-count diff;
- no causation language in UI.

---

## Recommended Unattended Architecture

### Tier 1 — Cron probes

Safe to run now or soon:

```text
freshness check
endpoint health check
row-count inventory
schema validation
selected pipeline chains without publishing
```

Output:

- GitHub Actions artifact;
- issue on failure;
- no commit.

---

### Tier 2 — Cron refresh branches

After probes stabilize:

```text
scheduled pipeline run
generate parquet + metadata
validate
create branch
open PR
attach health report
```

Output:

- reviewable data-refresh PR.

Do not auto-merge unless checks are strict and the diff is low-risk.

---

### Tier 3 — Conditional auto-merge

Only for low-risk refreshes.

Auto-merge allowed only when:

- no schema changes;
- no required column loss;
- row counts within thresholds;
- latest dates moved forward or unchanged for acceptable reason;
- source coverage did not drop;
- no manual-source stale warning;
- no high-risk parser warnings;
- no privacy-quarantine failures;
- caveat files unchanged or present;
- tests pass.

High-risk sources should never auto-merge initially:

- CRO;
- charity;
- CBI;
- Legal Diary, if added;
- judgments, if added;
- PDF parser format changes;
- any fuzzy entity-resolution update.

---

## Suggested Health Metadata

Create:

```text
data/_meta/dataset_health.json
```

Suggested schema:

```json
{
  "generated_at": "2026-06-03T00:00:00Z",
  "overall_status": "ok",
  "datasets": [
    {
      "dataset": "payments",
      "domain": "payments",
      "refresh_mode": "automated",
      "latest_source_date": "2026-05-31",
      "latest_local_date": "2026-05-31",
      "last_successful_refresh_at": "2026-06-03T00:00:00Z",
      "row_count": 12345,
      "status": "ok",
      "staleness_days": 3,
      "stale_after_days": 45,
      "source_url": "...",
      "coverage_note": "...",
      "manual_action_required": false
    }
  ]
}
```

Recommended statuses:

```text
ok
stale
source_unavailable
parser_failed
schema_changed
row_count_drop
manual_refresh_required
partial
experimental
not_checked
```

---

## Publish Guard Checklist

Before refreshed data is published, require:

### Global checks

- pipeline exit status ok or acceptable partial;
- freshness metadata generated;
- dataset health generated;
- no critical stale datasets;
- no schema-breaking changes;
- no major row-count collapses;
- no missing source URLs in public-facing rows;
- no missing provenance for enriched rows.

### Row-count checks

Flag if:

- row count drops by more than 50%;
- public-facing dataset becomes zero rows;
- latest date moves backward;
- expected new period disappears;
- join coverage drops sharply;
- manual-review count spikes.

### Schema checks

Flag if:

- required column missing;
- column type changed;
- date parse fails;
- numeric parse fails;
- source/provenance column missing;
- confidence/manual-review column missing in enrichment tables.

### Domain-specific checks

Procurement:

- `value_safe_to_sum` present;
- unsafe values not summed;
- individual/sole-trader quarantine present;
- overlap caveat present.

SI legal state:

- null/unmatched not treated as in force;
- source URL present;
- coverage threshold maintained;
- state confidence present.

Lobbying/procurement overlap:

- causation language absent;
- match confidence present;
- source coverage present.

Corporate:

- entity match confidence present;
- CRO/CBI freshness visible;
- experimental matches labelled.

Judiciary, if added:

- no performance/ranking/bias score;
- every fact source-linked;
- fuzzy matches require manual review.

---

## GitHub Actions Recommendation

### Keep existing jobs

Keep:

- CI;
- freshness canary;
- endpoint health;
- pipeline probe;
- SQL contract tests;
- dependency sync;
- lint/type checks.

### Add jobs

Add:

```text
dataset-health.yml
```

Runs:

```bash
python tools/check_freshness.py
python tools/build_dataset_health.py
python tools/gold_rowcounts.py --output data/_meta/gold_rowcounts_current.json
pytest -m "not integration and not sources"
```

Add:

```text
pipeline-refresh-pr.yml
```

Manual first, scheduled later.

Flow:

1. checkout repo;
2. setup uv;
3. run selected pipeline chains;
4. generate health report;
5. run validation;
6. create branch;
7. commit only parquet/metadata changes;
8. open PR;
9. attach summary.

### Do not add direct auto-publish yet

Avoid:

```text
scheduled cron → run pipeline → commit to main
```

Use PRs until the system has proven stable over several runs.

---

## Manual Review Cadence

If cron probes only:

- review weekly.

If cron refresh PRs exist:

- review weekly or fortnightly.

If hardened with strict checks:

- review monthly.

Manual tasks likely still needed:

- CRO source refresh;
- charity register refresh;
- parser fixes after source drift;
- reviewing major row-count/schema changes;
- resolving entity-match ambiguity;
- validating high-risk legal/procurement/corporate changes.

---

## How Long Could It Last?

### Without touching anything except existing cron/freshness checks

Estimate:

> 3–6 months before important data becomes stale or a source drift goes unnoticed.

Reason:

- committed data may remain useful;
- official/public data updates continuously;
- manual CRO/charity sources age;
- freshness does not yet cover all app-facing datasets.

### With cron probes but no auto-publish

Estimate:

> 6–12 months of safe public operation, provided the app clearly displays freshness.

Reason:

- probes can warn you;
- public data remains transparent;
- stale datasets are visible rather than silently wrong.

### With cron refresh PRs and validation

Estimate:

> 12–24 months with light monthly maintenance.

Reason:

- data can refresh regularly;
- failures become PR/check failures;
- humans only review suspicious diffs.

### With fully automatic publish

Estimate:

> not recommended.

A fully automatic system might run for weeks or months, but one bad parser/source change could publish misleading data.

---

## Minimum Hardening Before Leaving It Alone

Do these before relying on unattended cron:

1. Expand freshness coverage to every app-facing dataset.
2. Add `dataset_health.json`.
3. Mark CRO and charity as manual/semi-manual sources.
4. Add row-count collapse guards.
5. Add schema guards for every public parquet.
6. Add source/provenance completeness checks.
7. Add domain-specific guards for SI/procurement/lobbying/corporate.
8. Fix any local-only cache paths in pipeline scripts.
9. Make cron create PRs, not direct commits.
10. Surface freshness and stale warnings in the app UI.

---

## Recommended Roadmap

### PR 1 — Health metadata

- Add `tools/build_dataset_health.py`.
- Extend freshness model.
- Generate `data/_meta/dataset_health.json`.
- Add tests for freshness/dataset health.

### PR 2 — Publish guards

- Add schema contracts for public parquets.
- Add row-count collapse checks.
- Add source/provenance completeness checks.
- Add failure thresholds.

### PR 3 — Manual-source flags

- Add CRO health metadata.
- Add charity health metadata.
- Add UI warnings for manual-source staleness.

### PR 4 — Refresh PR workflow

- Add GitHub Action to run selected chains.
- Generate health report.
- Open data-refresh PR.
- Do not auto-merge.

### PR 5 — Safe auto-merge for low-risk datasets

Only after multiple successful PR runs.

Allowed candidates:

- freshness metadata;
- endpoint health;
- low-risk Oireachtas API outputs;
- maybe Iris issue polling once stable.

Excluded from auto-merge initially:

- CRO;
- charity;
- CBI;
- procurement;
- legal-state parsing;
- entity-resolution outputs;
- judiciary data, if added.

---

## Final Verdict

The project can probably survive as a mostly unattended cron-backed civic app for **6–12 months** if cron jobs are used mainly for probes, freshness checks, and reviewable refresh PRs.

With better dataset-health metadata, row-count/schema guards, manual-source tracking, and PR-based publishing, it could plausibly run for **12–24 months with light monthly maintenance**.

It should not be left as a fully automatic “cron publishes to main” system yet.

The most important distinction:

> Automating refresh is fine. Automating publication without strong guards is the risk.
