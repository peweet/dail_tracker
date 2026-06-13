# Dáil Tracker — Shipping Strategy, Pipeline Readiness, UI Coupling, and Page Roadmap

**Prepared:** 2026-06-05  
**Repository:** `https://github.com/peweet/dail_tracker`  
**Purpose:** Package the working assessment into a concise strategic Markdown plan for deciding whether to ship the current Streamlit app, rebuild the UI, or stage the new Judiciary, Procurement, Local Authority, and Housing pages.

---

## 1. Executive Recommendation

Ship the current Streamlit app first as a controlled public beta.

Do not do a full UI rebuild yet.

The strongest path is:

```text
1. Harden the existing Streamlit app.
2. Hide or label unfinished domains.
3. Ship a public beta.
4. Learn which workflows users actually value.
5. Rebuild only the proven high-value areas later.
```

The reason is simple: the project’s hardest asset is not the UI layer. It is the data pipeline, messy public-source integration, SQL views, entity linkage, caveats, and trust model. Rebuilding the UI before validating that value would risk delaying useful feedback for months.

A polished UI can come later. A credible source-linked beta can ship sooner.

---

## 2. Current Build Readiness

| Area | Readiness | Notes |
|---|---:|---|
| Core Streamlit app | Medium | Navigation and many pages exist; still needs public-beta hardening. |
| Pipeline foundation | Medium-high | Strong batch-pipeline direction, but not every domain is equally mature. |
| Procurement backend | High-ish | One of the closest hidden/unsurfaced areas to page readiness. |
| Procurement UI | Low-medium | No stable top-nav page yet; needs caveats and source-state handling. |
| Judiciary UI | Medium | Page appears routed in current main; needs privacy/freshness hardening. |
| Local Authority / Housing | Low-medium | Best as a planned second tile; housing data sources are not yet fully built. |
| Full UI rebuild readiness | Low | Premature until user workflows are validated. |

---

## 3. Ship Now vs Rebuild

### Ship Streamlit Now

Best when:

```text
- the data is the main asset;
- the project has taken a long time to get working;
- feedback is more valuable than visual polish;
- the app can be clearly labelled as beta;
- fragile domains can be hidden or caveated.
```

Advantages:

```text
- validates real user demand;
- avoids another long engineering tunnel;
- preserves existing working Streamlit investment;
- gets feedback from journalists, researchers, civic-tech users, and policy users;
- focuses attention on data trust rather than frontend polish.
```

Risks:

```text
- UI may feel less polished;
- Streamlit pages can become monolithic;
- custom CSS/HTML may be brittle;
- public users may misinterpret incomplete or caveated data;
- blank tables may hide source or pipeline failures unless improved.
```

### Rebuild UI Now

Best when:

```text
- you already know the killer workflows;
- you need SEO/shareable public profiles;
- the Streamlit app is blocking user experience;
- you have frontend capacity;
- you need a long-term polished public product immediately.
```

Risks:

```text
- delays launch;
- duplicates existing effort;
- shifts attention away from data quality;
- creates API/design-system work before product-market validation;
- may rebuild pages users do not actually care about.
```

### Recommendation

Do not rebuild now.

Ship a constrained Streamlit public beta after a focused hardening pass.

---

## 4. Public Beta Scope

### Ship Now If Stable

```text
- Members
- Attendance
- Votes
- Payments
- Interests
- Lobbying
- Legislation
- Statutory Instruments
- Appointments / Corporate if stable
```

### Ship Carefully / Beta Label

```text
- Courts & Judiciary
- Procurement
```

### Do Not Fully Ship Yet

```text
- Local Authority & Housing
- Housing as a full policy lens
- Local-authority procurement/payment rollups
- CPO / planning / legal-infrastructure signals
```

These should be visible only as “coming soon,” “methodology in progress,” or hidden until source contracts are reliable.

---

## 5. Minimum Hardening Before Launch

### 5.1 Add Public Beta Language

Every page should clearly say:

```text
This is a public beta. Data is aggregated from official sources and may change as sources update. Always check linked source records before relying on a result.
```

### 5.2 Show Data Freshness

Every page should expose:

```text
- last updated timestamp;
- source coverage;
- known missing sources;
- methodology link;
- data dictionary link where possible;
- report-an-issue link.
```

### 5.3 Fix Empty-State Ambiguity

A blank table should never be ambiguous.

Distinguish:

```text
- no matching records;
- data not loaded;
- source unavailable;
- extractor failed;
- SQL view missing;
- privacy filter removed all records.
```

### 5.4 Hide Fragile Domains

Do not expose a page simply because files exist.

Only surface pages where:

```text
- source coverage is known;
- SQL views are stable;
- empty/source-error states are clear;
- caveats are visible;
- sensitive data is protected;
- the page does not imply more certainty than the data supports.
```

### 5.5 Add Provenance Footer

Each major page should include:

```text
Primary source
Last refresh
Methodology
Known caveats
Data dictionary
Audit trail / change log
Report issue
```

### 5.6 Add Feedback Path

At minimum:

```text
- Report data issue
- Suggest source
- Request feature
- Contact / GitHub issue
```

---

## 6. Pipeline Readiness by Domain

### 6.1 Procurement

Procurement is one of the closest domains to UI readiness.

Observed backend shape:

```text
eTenders / OGP awards
  -> procurement extractor
  -> supplier classification
  -> CRO matching
  -> value-kind handling
  -> procurement SQL views
  -> procurement query/data-access layer
```

Recommended first page:

```text
Public Money & Procurement
  ├─ caveat banner: award ≠ spend
  ├─ supplier summary
  ├─ contracting authority summary
  ├─ CPV/category summary
  ├─ lobbying overlap
  └─ supplier detail drilldown
```

Do not present awarded values as actual spend.

Required language:

```text
Awarded value is not actual spend.
Framework and DPS values may represent ceilings or repeated values.
Lobbying overlap means an organisation appears in both datasets, not causation.
```

Before shipping Procurement:

```text
[ ] Add result-aware data access, not only empty DataFrames
[ ] Make source unavailable states explicit
[ ] Add value-kind legend
[ ] Add required vs optional SQL view handling
[ ] Add tests for value_safe_to_sum
[ ] Add privacy checks for sole traders / individuals
```

### 6.2 Judiciary

Judiciary appears more surfaced than originally assumed: the current app imports and routes a Courts & Judiciary page.

However, this domain must remain privacy-first.

Recommended positioning:

```text
Courts & Judiciary
A source-linked, privacy-first summary of public legal-diary activity.
```

Not:

```text
judge ranking
litigant search
misconduct implication
personal case register
```

Required hardening:

```text
[ ] Confirm legal diary extraction is part of a canonical scheduled run or documented separate job
[ ] Replace privacy asserts with explicit runtime exceptions
[ ] Add tests that protected categories are dropped
[ ] Add tests that raw case text never enters public output
[ ] Add tests that natural-person names are anonymised
[ ] Add clear freshness and source-state panels
[ ] Avoid “busiest judge” or performance-ranking language
```

Use neutral labels:

```text
listed sessions
listed items
court/list activity
publishable listed matters
```

Avoid:

```text
top judges
judge performance
case backlog by judge
```

### 6.3 Local Authority & Housing

This should be the second planned tile, but it is not ready as a full page yet.

Best product concept:

```text
Local Authority & Housing
  = local authority context
  + housing need
  + housing delivery/supports
  + local finance
  + public money/procurement signals
  + constituency/member context
```

This is better than a page called only “Housing” because most of the relevant data is local-authority or constituency contextual.

Recommended staged approach:

```text
v0: Local Authority & Housing skeleton
v1: SSHA housing need
v2: housing grants/supports
v3: local authority finance / AFS
v4: local authority procurement/payments
v5: TD activity and housing policy lens
v6: audit / CPO / infrastructure signals
```

Do not launch the full mocked housing page until at least SSHA and one housing-money source are implemented as stable views.

---

## 7. Streamlit UI Coupling Review

### Good Signs

```text
- Existing Streamlit navigation works.
- Data-access modules are increasingly thin wrappers.
- SQL views are becoming the semantic contract.
- There is a stated architecture direction that UI should render, not define business logic.
- The project can ship useful dashboards without a full frontend rewrite.
```

### Coupling / Fragility Points

```text
- Some pages are large and combine UI, layout, CSS, pandas shaping, and source-state logic.
- Custom HTML/CSS in Streamlit can become brittle.
- Page-level grouping/filtering risks turning into business logic.
- Data wrappers sometimes return empty DataFrames instead of preserving failure semantics.
- SQL view registration can swallow errors, which is risky for required views.
- A polished UI built directly in Streamlit may require many layout hacks.
```

### Recommended Refactor Before Adding More Pages

Create shared UI components:

```text
utility/ui/page_shell.py
utility/ui/filter_rail.py
utility/ui/kpi_card.py
utility/ui/source_banner.py
utility/ui/provenance_footer.py
utility/ui/value_kind_legend.py
utility/ui/empty_state.py
utility/ui/data_quality_badge.py
```

Create stricter data-access result patterns:

```text
fetch_x_result() -> QueryResult
fetch_x() -> DataFrame  # legacy convenience wrapper
```

Then pages can distinguish:

```text
success
zero records
source unavailable
view failure
privacy-filtered result
```

---

## 8. UI Framework Options

### Option A — Stay with Streamlit

Best short-term choice.

Use Streamlit for:

```text
- public beta;
- internal QA;
- analyst workflows;
- quick civic-data dashboards;
- source review pages;
- data validation views.
```

Pros:

```text
- fastest path;
- preserves current investment;
- Python-only;
- works well with DuckDB/parquet;
- good enough for beta validation.
```

Cons:

```text
- limited polish compared with React;
- custom layout requires CSS workarounds;
- rerun model can be awkward;
- very large pages become hard to maintain.
```

Recommendation:

```text
Use Streamlit for v1 public beta.
```

### Option B — Dash / Plotly

Good if staying Python-first but wanting more structured dashboard callbacks.

Pros:

```text
- stronger dashboard interaction model;
- good charting;
- Python-centric;
- more explicit callbacks than Streamlit.
```

Cons:

```text
- migration cost;
- still requires CSS/design work;
- can become verbose.
```

Recommendation:

```text
Consider only if Streamlit interactions become a blocker but a full React app is too much.
```

### Option C — FastAPI + Next.js

Best long-term public-product direction.

Architecture:

```text
DuckDB / Parquet / SQL views
  -> FastAPI read API
  -> Next.js frontend
  -> ECharts / TanStack Table / shadcn/ui / Tailwind
```

Pros:

```text
- best polish;
- real design system;
- better navigation and shareable URLs;
- better tables and drilldowns;
- easier public profile pages;
- more scalable product architecture.
```

Cons:

```text
- largest build cost;
- requires frontend work;
- API contracts must be designed;
- deployment is more complex.
```

Recommendation:

```text
Do this later, after the beta proves which workflows matter.
```

### Option D — Hybrid

Recommended medium-term architecture:

```text
Streamlit = internal data workbench / beta tool
FastAPI = read API over canonical SQL views
Next.js = polished public product
DuckDB SQL views = shared semantic layer
```

This preserves the current Streamlit investment while enabling a proper public product later.

---

## 9. Suggested Launch Plan

### Phase 0 — Decide What Is Public

```text
[ ] Mark stable pages
[ ] Hide or label unstable pages
[ ] Add beta label
[ ] Add known limitations page
```

### Phase 1 — Hardening Pass

```text
[ ] Add freshness banners
[ ] Add provenance footers
[ ] Add report-issue links
[ ] Improve empty states
[ ] Add source unavailable states
[ ] Ensure no debug/error details are exposed in production
```

### Phase 2 — Soft Launch

Share with a small trusted group:

```text
- journalists
- civic-tech people
- political researchers
- academics
- policy analysts
- transparency advocates
```

Ask:

```text
What did you try to find?
Where did you trust it?
Where did you not trust it?
Which page was most useful?
Which data caveat was unclear?
What would make this worth returning to?
```

### Phase 3 — Iterate

Prioritise based on observed use:

```text
If Member Overview wins:
  improve public member profiles.

If Procurement wins:
  build supplier/public-body dossiers.

If Housing wins:
  build Local Authority & Housing profiles.

If Judiciary wins:
  harden privacy, source coverage, and explainability.
```

### Phase 4 — Rebuild Selectively

Only after the killer workflow is clear:

```text
Streamlit remains internal.
Next.js/FastAPI becomes public v2.
```

---

## 10. Roadmap for the Two Planned Tiles

### Tile 1 — Public Money & Procurement

Build first.

Minimum page:

```text
- supplier summary
- authority summary
- CPV/category summary
- lobbying overlap
- value-kind legend
- source coverage panel
- supplier detail drilldown
```

Must-have caveats:

```text
award ≠ spend
framework ceilings may overstate
lobbying overlap is not causation
supplier matching confidence varies
```

### Tile 2 — Local Authority & Housing

Build second.

Start as:

```text
- local authority selector
- SSHA housing need once implemented
- AFS/local finance panel
- housing grants once implemented
- procurement/payment placeholder
- TD housing activity once queryable
```

Do not overstate constituency mappings unless there is a defensible geography bridge.

---

## 11. Practical Decision

The right decision is:

```text
Ship the Streamlit app as a controlled public beta.
Do not do a full UI rebuild now.
Perform a small hardening pass first.
Use real users to identify the valuable workflows.
Rebuild later only around the proven workflows.
```

The repo’s value is in its data and accountability model. The UI should be good enough to reveal that value, not perfect enough to delay it.

---

## 12. One-Page Checklist

```text
[ ] Choose public beta pages
[ ] Hide or label unfinished pages
[ ] Add beta language
[ ] Add freshness metadata
[ ] Add provenance/methodology footer
[ ] Add report-issue link
[ ] Improve empty/source-error states
[ ] Add production-safe Streamlit settings
[ ] Keep Procurement as next build candidate
[ ] Keep Local Authority & Housing as second planned tile
[ ] Harden Judiciary privacy/freshness before expanding it
[ ] Delay UI rebuild until user workflows are validated
```

---

## 13. Final Verdict

Ship.

But ship carefully.

Use the existing Streamlit app as a public beta, with strong caveats, source links, known limitations, and feedback loops. Do not burn more time on a full UI rebuild until the product has proven which data workflows users actually care about.

The UI mockups should guide v2, not block v1.
