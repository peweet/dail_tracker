# dail_tracker `seanad-app-parity` — Consolidated Suggestions and Fixes

Static audit recommendations for the `seanad-app-parity` branch.

> Scope note: These recommendations come from a static repository audit. Tests, Streamlit runtime behavior, CI logs, and parquet internals were not executed locally.

---

## Critical Before Public Alpha

### Stop hiding data-access failures as empty DataFrames

Many data-access helpers catch exceptions and return empty DataFrames. For a public civic-data app, this is risky because a backend failure can look like “there is no data.”

Affected areas include:

- `utility/data_access/member_overview_data.py`
- `utility/data_access/lobbying_data.py`
- `utility/data_access/corporate_data.py`
- `utility/data_access/legislation_data.py`
- `utility/data_access/attendance_data.py`
- `utility/data_access/votes_data.py`
- `utility/data_access/interests_data.py`
- `utility/data_access/payments_data.py`
- `utility/data_access/appointments_data.py`
- `utility/data_access/committees_data.py`
- `utility/data_access/procurement_data.py`

Suggested fix: introduce a small result wrapper so the UI can distinguish successful empty results from unavailable data.

```python
from dataclasses import dataclass
import pandas as pd

@dataclass(frozen=True)
class QueryResult:
    data: pd.DataFrame
    ok: bool
    error: str | None = None
    unavailable_reason: str | None = None
```

The UI should distinguish successful query with no matching records, source unavailable, SQL registration failure, parquet missing, feature not checked, chamber unsupported, and data not yet covered.

Start with Member Overview, Statutory Instruments, Lobbying, Corporate Notices, Payments, Attendance, and Votes.

---

### Add visible public caveats and disclaimers

The app handles legal, procurement, lobbying, payments, corporate, and public-office data. Public users need caveats close to the data, not buried only in docs.

Affected areas:

- `utility/pages_code/glossary.py`
- `utility/pages_code/statutory_instruments.py`
- `utility/pages_code/lobbying_3.py`
- `utility/pages_code/corporate.py`
- `utility/pages_code/payments.py`
- future procurement page
- `README.md`
- `doc/DATA_LIMITATIONS.md`

The public caveat language should cover that the app is a discovery and transparency tool, not official legal advice; data may be incomplete, delayed, stale, or limited by source coverage; “no data” does not always mean “no activity”; lobbying/procurement overlap is co-occurrence only, not causation; procurement award value is not expenditure; SI legal-state results are source-linked discovery aids only; corporate, CBI, CRO, charity, and procurement enrichment may include match uncertainty; and entity-resolution confidence should be checked before relying on results.

Suggested implementation: use the Glossary as the central public explanation page, then add compact caveat boxes at the top of high-risk pages.

---

### Surface freshness and source coverage metadata in the app

A public civic-data app needs to show when data was last refreshed, what sources were covered, and where gaps remain.

Affected areas:

- `data/_meta/freshness.json`
- `data/_meta/*coverage*`
- all public pages
- all major data-access modules

Suggested fix: create a shared Streamlit component such as `render_source_status(domain: str)`.

It should show last refresh date, source family, coverage date range, row counts where available, known limitations, source links, methodology link, and whether data is complete, partial, experimental, unchecked, or unavailable.

Apply this to Members, Votes, Attendance, Payments, Interests, Lobbying, Legislation, Statutory Instruments, SI legal state, Corporate Notices, Procurement, Appointments, and Committees.

---

### Preserve safe SI legal-state wording

The SI legal-state feature is valuable but legally sensitive. It must not imply legal certainty.

Affected areas:

- `utility/pages_code/statutory_instruments.py`
- `utility/pages_code/legislation.py`
- `sql_views/legislation_si_current_state.sql`
- SI legal-state tests and docs

Use wording such as “not checked,” “source-linked status,” “affected,” “amended,” “revoked,” “partially revoked,” “coverage unavailable,” and “requires manual legal verification.”

Avoid wording such as “in force” unless source-backed, “valid,” “invalid,” “definitive legal status,” “superseded” unless legally and source-backed, and “repealed” where the source only supports affected/amended/revoked.

Suggested page text:

> This page is a source-linked discovery aid, not legal advice. “Not checked” does not mean “in force.” Status labels reflect source records detected by the project and may be incomplete.

---

### Preserve procurement/lobbying non-causation wording

Procurement and lobbying overlap is useful but reputationally sensitive. The app must not imply lobbying caused procurement outcomes.

Affected areas:

- `sql_views/procurement_lobbying_overlap.sql`
- `utility/data_access/lobbying_data.py`
- `utility/data_access/procurement_data.py`
- `utility/pages_code/lobbying_3.py`
- future procurement page

Use wording such as “overlap,” “co-occurrence,” “same organization appears in both datasets,” “not evidence of influence,” “not evidence of wrongdoing,” and “requires source review.”

Avoid wording such as “lobbying led to,” “influenced,” “caused,” “won after lobbying,” and “conflict” unless independently established.

Suggested page text:

> This overlap indicates that an organization appears in both lobbying and procurement records. It is not evidence that lobbying influenced any award.

---

### Quarantine or suppress personal / sole-trader procurement records

Procurement data can include individuals or sole traders. Publishing searchable personal rows casually creates privacy and reputational risk.

Affected areas:

- `pipeline_sandbox/procurement_etenders_extract.py`
- `sql_views/procurement_awards.sql`
- `utility/data_access/procurement_data.py`
- future procurement page
- procurement metadata and coverage docs

Suggested flags:

- `supplier_is_individual_like`
- `supplier_is_sole_trader_like`
- `supplier_name_truncated`
- `safe_for_public_display`
- `safe_for_aggregation`
- `privacy_quarantine_reason`

Default behavior should suppress individual-like rows from public tables, include only safe aggregates, exclude individual-like rows from lobbying/CRO overlap unless confidently matched to an organization, and document the policy clearly.

---

## High Priority

### Split Streamlit wrappers from a pure DuckDB query core

The current data-access layer imports Streamlit and uses `st.cache_data` / `st.cache_resource`. That makes it harder to reuse with FastAPI, React, CLI exports, scheduled validation, or non-Streamlit tests.

Suggested target architecture:

```text
dail_tracker_data/
  __init__.py
  db.py
  registry.py
  results.py
  member_overview.py
  lobbying.py
  corporate.py
  legislation.py
  attendance.py
  votes.py
  payments.py
  interests.py
  appointments.py
  committees.py
  procurement.py

utility/data_access/
  thin Streamlit cache wrappers only
```

The pure query core should not import Streamlit. Streamlit caching should wrap pure functions rather than being embedded in query logic.

Move one page at a time, starting with SI legal state, Corporate Notices, Lobbying, and Member Overview.

---

### Add an explicit SQL manifest

The SQL registry currently loads SQL files by glob/sorted order. That is fragile as SQL dependencies grow.

Affected areas:

- `utility/data_access/_sql_registry.py`
- `sql_views/`

Suggested manifest shape:

```yaml
views:
  - file: member_base.sql
    view: member_base
    domain: members
    depends_on: []
    grain: one row per member-term/member identity
    public_facing: false

  - file: legislation_si_current_state.sql
    view: legislation_si_current_state
    domain: statutory_instruments
    depends_on:
      - legislation_statutory_instruments
    grain: one row per SI where current-state source has been checked
    public_facing: true
    caveat: Missing row means not checked, not in force.
```

Benefits include deterministic load order, easier dependency reasoning, better docs generation, easier SQL contract testing, and safer future API/export work.

---

### Promote documented sandbox outputs or clearly label them one-shot

Some production-useful outputs appear to be generated from scripts still living in `pipeline_sandbox/`, including procurement, SI legal-state, CBI, CRO, and SIPO OCR work.

This is acceptable for discovery, but public publication needs clearer boundaries.

Suggested classification:

```text
pipeline/
  canonical reproducible chains

pipeline_one_shots/
  documented one-shot extraction scripts that produced committed gold outputs

pipeline_sandbox/
  probes, local experiments, discovery scripts only

archive/
  stale experiments not relevant to current app
```

Do not prematurely move probe work into `pipeline.py`. New procurement/public-body/semi-state work should remain sandbox-first until source coverage, legal semantics, privacy handling, and tests are proven.

---

### Add public methodology pages for high-risk datasets

The project should have concise, public-facing methodology pages for SI legal state, procurement awards, procurement/lobbying overlap, corporate notices, CBI enrichment, CRO/charity matching, lobbying data, payments, attendance, Seanad coverage, and freshness/coverage metadata.

These pages should separate what the source says, what the project infers, what the project does not know, known coverage gaps, match confidence, update cadence, examples of safe interpretation, and examples of unsafe interpretation.

---

### Make Seanad caveats and provenance first-class

Seanad parity is a major strength of the branch, but it needs explicit public-source caveats anywhere coverage differs from Dáil coverage.

Affected areas:

- `utility/pages_code/attendance.py`
- `utility/pages_code/votes.py`
- `utility/pages_code/payments.py`
- `utility/pages_code/member_overview.py`
- Seanad SQL views
- Seanad tests

Recommended wording:

> Seanad coverage is included where source data is available. Some source links, historical coverage, or metadata fields may differ from Dáil coverage.

---

### Stabilize data-access return types

Some functions return DataFrames, some return Series, some return empty DataFrames on failure, and some return fallback-shaped frames.

Suggested fix:

- use DataFrames consistently for table-like data;
- use typed dataclasses for scalar summaries;
- use `QueryResult` or equivalent for anything that can fail;
- document expected columns for each public-facing function;
- add tests for schema stability.

This will reduce page fragility and make future API/export work easier.

---

### Add a public data inventory page

The project already has many committed parquet artifacts and metadata files. Users need a clear inventory.

Suggested fields:

- dataset name;
- file path;
- domain;
- source;
- source URL or source family;
- last updated;
- row count;
- date coverage;
- known gaps;
- whether public-facing;
- whether experimental;
- whether personally sensitive;
- matching/confidence notes;
- SQL views that use it.

This can be generated from `data/_meta/`, the future SQL manifest, and simple parquet metadata checks.

---

## Medium Priority

### Modularize `member_overview.py`

The Member Overview page is one of the strongest product ideas, but it is large and complex.

Suggested split:

```text
utility/pages_code/member_overview/
  page.py
  header.py
  activity_panel.py
  votes_panel.py
  attendance_panel.py
  payments_panel.py
  interests_panel.py
  lobbying_panel.py
  legislation_panel.py
  si_panel.py
  committees_panel.py
  caveats.py
```

Benefits include easier review, easier tests, safer page imports, simpler future reuse in an API/frontend, and clearer ownership of caveats.

---

### Add contract tests for high-risk semantics

Add tests for:

- SI null/unmatched status renders as “not checked,” not “in force”;
- partially revoked SIs are not simplified to fully revoked;
- multiple affecting instruments are preserved;
- procurement framework/DPS values are not summed unless safe;
- `value_safe_to_sum` gates all totals;
- lobbying/procurement overlap is labelled co-occurrence only;
- personal/sole-trader supplier records are quarantined;
- CBI matches remain experimental unless exact/source-backed;
- Seanad and Dáil votes do not leak into each other;
- attendance missing-member records are not treated as absence proof;
- payments page handles no-data vs unavailable data.

---

### Make entity-resolution confidence visible

The project’s strongest future moat is entity resolution, but false positives are the biggest risk.

Affected domains:

- lobbying organization → CRO/charity/procurement;
- procurement supplier → lobbying/CRO;
- corporate notice → CBI/CRO;
- charity/CRO → lobbying/corporate/procurement;
- SIPO experimental outputs → members/parties/donors, if present.

Expose match method, exact/normalized/fuzzy/manual match status, confidence score, source fields used, whether a match is public-facing, whether manual review occurred, and false-positive warnings where appropriate.

Public users and professional users need to know whether a link is exact, heuristic, experimental, or unresolved.

---

### Add feature badges

Use consistent badges across pages:

- Stable
- Beta
- Experimental
- Source coverage incomplete
- One-shot extraction
- Requires manual verification
- Not legal advice
- Not causation
- Not expenditure
- Entity match uncertain

These badges will reduce overclaiming and make the app more honest.

---

### Archive stale docs and experiments

The repository contains many planning, audit, and experimental docs/scripts. That is normal for an active project, but public users and contributors may be confused.

Suggested cleanup:

```text
doc/public/
  stable public methodology and caveats

doc/internal/
  plans, audits, branch notes

doc/archive/
  stale plans and superseded architecture notes

pipeline_sandbox/
  active probes only

pipeline_sandbox/archive/
  stale probes
```

Do not delete historical work if it may be useful, but mark it clearly.

---

### Add public export caveats

For CSV downloads or future API exports, include caveats in metadata.

Every export should include dataset name, generated date, source date, caveat text, row filters, experimental status, match method, and citation/source URL fields where possible.

This is especially important for legal, procurement, corporate, and lobbying exports.

---

### Improve page-level empty states

Each page should distinguish:

- no records for selected filter;
- data not available for selected chamber;
- source not loaded;
- source not checked;
- feature experimental;
- source coverage incomplete;
- internal query error.

Examples:

- “No lobbying returns found for this organization.”
- “This SI has not been checked against the legal-state directory.”
- “Corporate notice enrichment is unavailable because the CBI xref parquet could not be loaded.”
- “Seanad source links are not available for this attendance period.”

---

## Low Priority / Polish

### Improve onboarding and public UX

The app has a lot of data. Add a public-friendly onboarding flow:

- “Start with a member”
- “Search an organization”
- “Explore SIs”
- “Review corporate notices”
- “Understand lobbying/procurement overlap”
- “Check freshness and limitations”

Keep advanced filters, but offer guided entry points.

---

### Add stable citation snippets

For each public page, provide a suggested citation:

> Source: dail_tracker, dataset name, source family, refresh date, URL/access date.

This is useful for journalists, academics, lawyers, NGOs, and policy researchers.

---

### Prepare for exports/API, but do not build full API yet

Do not build a full API until the query core is Streamlit-free and data contracts are stable.

Near-term export work should focus on:

- CSV downloads with caveats;
- source URLs in exports;
- metadata sidecar JSON;
- stable column names;
- clear experimental flags.

A future API is justified only after public-alpha usage proves demand.

---

### Keep heavy OCR dependencies out of normal CI

SIPO OCR work should remain one-shot or optional unless it becomes a canonical pipeline. Heavy OCR dependencies should stay out of normal app/runtime CI.

For one-shot OCR outputs, document:

- exact script used;
- input files;
- OCR engine/version;
- confidence thresholds;
- manual review policy;
- output parquet/CSV location;
- known failure modes.

---

## Procurement-Specific Fixes

### Do not launch procurement as a generic spend dashboard

Procurement award data is not the same as expenditure. The page should launch only as an award and public-record overlap explorer.

Suggested page title:

> Procurement Awards and Public-Record Overlap Explorer

Suggested caveat:

> Award values are not expenditure. Framework and DPS values may represent ceilings or repeated multi-supplier values. Only rows marked safe to sum should be included in totals.

---

### Enforce `value_safe_to_sum` everywhere

Every procurement total should be gated by `value_safe_to_sum`.

Do not allow UI totals, CSV totals, charts, authority totals, supplier totals, or CPV totals to sum unsafe rows.

Tests should assert that unsafe framework/DPS values are excluded from public totals.

---

### Treat local-authority and semi-state expansion as probe-first

Files such as `procurement_la_seed.py`, `procurement_la_registry.py`, and semi-state/public-body discovery work should remain in `pipeline_sandbox/` until proven.

Do not wire new chains into `pipeline.py` prematurely.

Promotion requirements:

- source coverage documented;
- row counts and date coverage recorded;
- privacy handling checked;
- value semantics checked;
- SQL contract tests added;
- public caveats written;
- reproducible script entrypoint exists.

---

## SI / Legal-State Fixes

### Keep “not checked” distinct from “in force”

This is the most important SI legal-state rule.

If no legal-state row exists, show:

> Not checked

Do not show:

> In force

unless there is a source-backed reason to do so.

---

### Preserve eISB/source links

Every SI legal-state result should expose the source link where available.

Exports should include:

- SI number;
- year;
- title;
- detected state;
- source URL;
- source label;
- confidence;
- coverage flag;
- last checked date.

---

### Add dangerous parsing tests

Tests should cover:

- amended by multiple instruments;
- partially revoked;
- revoked and re-made;
- affected without clear amendment;
- broken source link;
- missing source link;
- ambiguous title match;
- same SI number across years;
- null legal-state row;
- low-confidence state.

---

## Corporate-Specific Fixes

### Label CBI enrichment as regulatory provenance / experimental where appropriate

CBI enrichment should not imply that a corporate notice is itself a regulatory action.

Suggested wording:

> CBI enrichment indicates that the entity appears in CBI regulatory-source data. It does not mean the corporate notice itself is a CBI action or that distress is regulatory in origin.

---

### Distinguish solvent from distress-related notices

Members’ Voluntary Liquidation and other solvent/procedural events should not be grouped as distress unless the source explicitly supports that classification.

Add tests or data contracts for:

- MVL not distress;
- receivership distress;
- liquidation categories separated;
- examiner/administrator categories separated;
- ambiguous notice type marked uncertain.

---

### Show match method and confidence

For corporate → CRO/CBI matches, show whether the match is:

- exact registration number;
- exact normalized name;
- fuzzy name;
- manually reviewed;
- unresolved;
- low confidence.

Do not let low-confidence matches appear identical to exact matches.

---

## CI / Tooling Fixes

### Keep current CI checks and require them for public branches

The main CI should continue to enforce:

- Ruff lint;
- Ruff formatting check;
- dependency sync;
- logic firewall;
- basedpyright;
- normal pytest;
- SQL contract tests.

Before public alpha, verify that all required jobs pass on the branch and enable branch protection if applicable.

---

### Do not over-automate the pipeline yet

Full cloud refresh/publish automation should wait until scripts have clean entrypoints and import-time side effects are removed.

Project A should come first:

> Make current committed data transparent, tested, caveated, and publishable.

Project B comes later:

> Full cloud refresh and publish automation.

Do not bundle Project A and Project B together.

---

### Add main guards before script promotion

Any script promoted out of sandbox should have:

```python
def main() -> None:
    ...

if __name__ == "__main__":
    main()
```

It should also have clear inputs, outputs, logging, failure behavior, and no heavy side effects on import.

---

## Documentation Fixes

### Separate public docs from internal planning docs

Public docs should be stable and caveated. Internal plans and old audits should not read like current product claims.

Suggested structure:

```text
doc/public/
  methodology.md
  data_limitations.md
  si_legal_state.md
  procurement.md
  corporate_notices.md
  lobbying.md
  freshness.md

doc/internal/
  branch plans
  audit notes
  architecture experiments

doc/archive/
  old plans
  superseded architecture notes
```

---

### Avoid overclaiming uniqueness

Do not claim the project is unique because it shows votes, questions, debates, lobbying, or basic parliamentary data.

The defensible uniqueness claim is:

> A cross-source Irish public-record graph linking parliamentary, lobbying, legal, corporate, procurement, payments, appointments, and enrichment data with source caveats and freshness metadata.

---

### Add a publication checklist

Before public alpha, add a checklist covering:

- CI green;
- app imports green;
- high-risk caveats present;
- freshness page present;
- source coverage visible;
- no hidden failures on core pages;
- no unsafe procurement totals;
- no “in force” default for unchecked SIs;
- no causation language for lobbying/procurement overlap;
- privacy quarantine policy documented;
- docs updated;
- experimental pages clearly labelled.

---

## Product Strategy Recommendations

### Position the project correctly

Best positioning:

> An Irish public-record transparency explorer connecting parliamentary, lobbying, legal, corporate, procurement, and public-office data with source-linked caveats.

Avoid positioning it as:

- an official legal tool;
- a definitive compliance database;
- a lobbying influence detector;
- a procurement spending dashboard;
- a complete company-risk database;
- a polished commercial SaaS product.

---

### Keep the strongest public-launch pages prominent

Strongest launch candidates:

1. Member Overview
2. Lobbying
3. Statutory Instruments
4. Corporate Notices
5. Votes
6. Payments
7. Attendance
8. Committees
9. Appointments
10. Glossary / Methodology / Freshness

Procurement should launch later unless privacy, value semantics, and overlap caveats are fully implemented.

---

### Treat monetization as secondary

The most realistic support paths are:

- grants;
- civic-tech funding;
- GitHub Sponsors / OpenCollective;
- custom research;
- paid professional exports;
- consulting;
- portfolio/reputation value.

The least realistic path is a broad commercial SaaS subscription for the general public.

Core public-interest access should remain free.

---

## Suggested Implementation Order

### Phase 1: Public-alpha safety pass

1. Add visible caveats to high-risk pages.
2. Add freshness/source status component.
3. Stop hiding core data failures as empty DataFrames.
4. Preserve SI “not checked” semantics.
5. Preserve lobbying/procurement non-causation semantics.
6. Document privacy policy for procurement and individuals.
7. Confirm CI is green.

### Phase 2: Architecture cleanup

1. Split pure DuckDB query core from Streamlit wrappers.
2. Add explicit SQL manifest.
3. Stabilize return types.
4. Modularize Member Overview.
5. Add public data inventory.
6. Add semantic contract tests.

### Phase 3: Methodology and trust

1. Publish methodology pages.
2. Surface entity-resolution confidence.
3. Clean stale docs and sandbox work.
4. Add export caveats.
5. Add source-linked citations.

### Phase 4: New product surfaces

1. Build Procurement page only after safety contracts.
2. Add CSV exports with sidecar metadata.
3. Add alerts only after freshness automation is reliable.
4. Consider API only after the query core is stable.

---

## Do Not Build Yet

Do not build these yet:

- full cloud refresh/publish automation;
- broad semi-state/public-body procurement ingestion in the canonical pipeline;
- commercial SaaS packaging;
- alerts;
- causal lobbying/procurement narratives;
- a polished API before query contracts stabilize;
- legal-status claims beyond source-linked discovery;
- public searchable individual/sole-trader procurement rows.

---

## Final Priority Summary

The project should focus first on trust, caveats, source coverage, and failure transparency. The underlying idea is strong, but the risk is users misinterpreting missing data, stale data, heuristic matches, procurement values, SI legal states, or lobbying/procurement overlap.

The best next work is small, disciplined PRs:

1. Add public caveat components.
2. Add source/freshness display.
3. Introduce explicit unavailable/error states.
4. Preserve legal/procurement wording safeguards.
5. Add SQL manifest.
6. Split Streamlit wrappers from query core.
7. Add semantic contract tests.
8. Cleanly document promoted one-shot/sandbox outputs.

The project is worth continuing, but its credibility will depend more on caution, provenance, and maintenance than on adding more sources quickly.
