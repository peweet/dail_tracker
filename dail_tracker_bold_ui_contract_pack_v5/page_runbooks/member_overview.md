# Page Runbook — Member Overview

## What this page is

A single-politician public accountability record — every public dataset for one TD in one place.
Think **theyworkforyou.com**: attendance, votes, interests, payments, lobbying, legislation.
The user forms their own view. The page does not editorialize.

**Greenfield — no existing page to preserve or match.**
Design from the contract and supplements below. CSS and look-and-feel must match the rest of the app
(same `dt-*` / `int-rank-*` / `pay-*` classes, same editorial tone, same card+button pattern).

Contract: `utility/page_contracts/member_overview.yaml`
Output file: `utility/pages_code/member_overview.py`

---

## Full sequence

```
Step 0  explore agent            ← read-only: confirm views, CSS, helpers  (no code)
Step 1  /shape                   ← design brief                             (no code)
        contract agent           ← only if shape reveals contract gaps      (no code)
Step 2  /build-page              ← implement
Step 3  /civic-ui-review         ← catch issues
Step 4  /streamlit-frontend      ← if any domain tab feels sparse           (optional)
Step 5  /pipeline-view           ← when pipeline work is ready to unblock TODOs
```

Add the **supplements block** (copy below) after each skill invocation (Steps 1–4).

---

## Supplements block — copy after every skill invocation

```
Member Overview page-specific supplements:

FRESH DESIGN: This is a greenfield page. There is no existing page to preserve.
CSS and editorial style must match the rest of the app (shared dt-*, int-rank-*, pay-* classes,
shared components.py helpers). Do not invent a new visual language.

TWO-STAGE FLOW:
  Stage 1 = TD browse/index table with filters.
  Stage 2 = single TD full accountability profile with six domain tabs.
  Entry to Stage 2: row click in Stage 1 table, sidebar selectbox,
  or ?member=join_key URL param (st.query_params.get("member")).

NO PROVENANCE FOOTER — this page merges 5-6 disparate sources.
Any data quality caveat goes as a brief inline note in the relevant domain section only.
No about expander. No provenance section anywhere.

STAGE 1 MART NOT BUILT YET:
  v_member_overview_browse does not exist.
  Stage 1 must show identity columns only: member_name, party, constituency, government_status.
  Add a visible st.callout above the table listing cross-dataset columns pending pipeline:
    attendance_rate, payment_total_eur, declared_interests_count,
    lobbying_interactions_count, revolving_door_flag
  Mark each with: TODO_PIPELINE_VIEW_REQUIRED: v_member_overview_browse

STAGE 2 — these views exist and can be queried now (filter by join_key = ?):
  v_attendance_member_year_summary
  v_payments_member_detail
  v_vote_td_summary
  v_member_interests
  v_lobbying_summary
  v_lobbying_revolving_door
  v_legislation_index  (TODO_PIPELINE_VIEW_REQUIRED: sponsor_join_key column missing)

DOMAIN TABS — six tabs, in this order:
  :material/calendar_today: Attendance
  :material/how_to_vote: Votes
  :material/interests: Interests
  :material/payments: Payments
  :material/groups: Lobbying
  :material/gavel: Legislation

CARD + RIGHT BUTTON HEIGHT RULE:
  CSS grid unit via st.html: display:grid; grid-template-columns:1fr auto; align-items:stretch
  Right action: <a href="?member={join_key}" class="dt-action">→</a>
  Never: ghost HTML, MutationObserver, setTimeout, position:absolute overlays.

COLOUR ACCESSIBILITY (mandatory):
  Government badge = blue. Opposition badge = amber. Never red/green for any distinction.
  Category pills: reuse established pill palette from interests.py.

NO GLOBAL YEAR PILLS — each domain tab manages its own year scope.
  Attendance and Payments tabs have per-tab st.pills (newest first).
  Votes, Interests, Lobbying, Legislation do not need year pills.

BACK NAVIGATION:
  st.button("← Back to all members") at the TOP of main content in Stage 2.
  Not sidebar only. Clear st.query_params on back.
```

---

## Step 0 — Explore (read-only, no code)

Use the `explore` agent. Read-only scan — do not edit any files.

Prompt:

```
We are working on member_overview only.

Read only — do not edit any files:
1. utility/page_contracts/member_overview.yaml
2. utility/shared_css.py
3. utility/ui/components.py
4. sql_views/attendance_member_year_summary.sql
5. sql_views/payments_member_detail.sql
6. sql_views/vote_td_summary.sql
7. sql_views/member_interests_views.sql
8. sql_views/lobbying_summary.sql
9. sql_views/lobbying_revolving_door.sql
10. sql_views/legislation_index.sql

Do not scan data/ folders. Do not read other page files.

Return only:
1. Which per-domain views have a usable join_key filter column
2. Which views are missing a per-member filter (flag as TODO_PIPELINE_VIEW_REQUIRED)
3. Whether v_legislation_index has a sponsor join_key column
4. Whether v_vote_td_summary exposes per-vote detail or aggregate only
5. CSS class families in shared_css.py reusable for this page
6. components.py helpers reusable for this page (empty_state, member_identity_strip, etc.)
```

Use the output to validate the contract before shaping.

---

## Step 1 — Shape (design brief, no code)

```
/shape member_overview

[paste supplements block above]
```

The skill reads the contract and produces: layout, section order, hero, tab structure,
interaction model, chart strategy, empty states, TODO items, and implementation plan.

**If the shape output reveals contract gaps** (missing views, wrong column names, etc.):

```
Use the contract agent to update utility/page_contracts/member_overview.yaml.
Keep: data_access.mode = duckdb_in_process_registered_analytical_views
Keep: no provenance footer (source_links.note already set)
Keep: TODO_PIPELINE_VIEW_REQUIRED for v_member_overview_browse and any missing columns
Do not add Streamlit-side modelling or joins.
```

Approve the brief (and updated contract if changed) before moving to Step 2.

---

## Step 2 — Build

```
/build-page member_overview

[paste supplements block above]
```

The command reads `CLAUDE.md`, the runbook, and the contract. The supplements fill in
what the shared skill does not know: no provenance, two-stage flow, per-domain view list,
mart pending, colour rules.

**Files the build may touch:**
- `utility/pages_code/member_overview.py` ← main output
- `utility/shared_css.py` ← new CSS classes only; read first before adding
- `utility/ui/components.py` ← new helpers only; read first
- `utility/ui/table_config.py`
- `utility/ui/export_controls.py`
- `utility/page_contracts/member_overview.yaml` ← if contract gaps found

**Files the build must not touch:**
- `pipeline.py`, `enrich.py`, `normalise_join_key.py` — main pipeline, fragile
- `data/` — generated data, read-only
- Any other page file

---

## Step 3 — Review

```
/civic-ui-review member_overview

Additional checks specific to this page:
- No provenance footer rendered anywhere
- Stage 1 has visible TODO callout for pending mart columns
- Stage 2 back button is at TOP of main content, not sidebar only
- ?member=join_key loads Stage 2 directly without going through Stage 1
- All six domain tabs present (or explicit TODO if a view blocks it)
- No red/green colour pairs used for any distinction
- st.html used (not unsafe_allow_html)
- width="stretch" on all buttons (not use_container_width)
- html.escape on all dynamic HTML text
```

---

## Step 4 — If domain tabs feel too sparse

Each tab must feel like an evidence panel, not a dataframe dump.
Run this if any tab is missing a headline stat, empty state, or visual affordance:

```
/streamlit-frontend member_overview

The Stage 2 domain tabs are too sparse. For each tab add:

1. A bold headline stat — one number + one plain-language sentence
2. A visual affordance where it adds insight:
   Attendance: ProgressColumn on attendance_rate
   Votes: aye/níl/absent breakdown with st.metric or mini bar chart
   Interests: category pill strip reusing interests.py pill palette
   Payments: NumberColumn formatted "€{:,.2f}" + TAA band always visible
   Lobbying: revolving door badge if flagged (st.badge, color="orange")
   Legislation: oireachtas_url as labelled link via st.column_config.LinkColumn
3. A human empty state per tab: empty_state(heading, body) from components.py

[paste supplements block above]
```

---

## Step 5 — Unblock pipeline TODOs (when pipeline work is ready)

Two outstanding items block full Stage 1 and the Legislation tab.

**Architecture rule before starting:**
- SQL analytical views → create directly in `sql_views/` (cheap, serves frontend, no pipeline risk)
- Any new Python/Polars enrichment (joins, normalisation, flag computation) → `pipeline_sandbox/` only
- Never touch `pipeline.py`, `enrich.py`, or `normalise_join_key.py`

**1. Stage 1 browse mart (blocks cross-dataset columns in Stage 1):**

This requires joining 5 per-domain views on `join_key` — Python/Polars work needed first.

```
/pipeline-view v_member_overview_browse

Enrichment approach:
  New Python/Polars script in pipeline_sandbox/ reads each per-domain Parquet,
  joins on join_key, aggregates to one-row-per-TD, writes output Parquet.
  SQL view in sql_views/member_overview_browse.sql then reads that output.

Columns needed: member_name, join_key, party, constituency, government_status,
  attendance_rate, payment_total_eur, declared_interests_count,
  lobbying_interactions_count, revolving_door_flag, bills_sponsored_count
```

**2. Legislation sponsor column (blocks Legislation tab in Stage 2):**

If the source data already has the sponsor join_key, this is SQL only.

```
/pipeline-view v_legislation_index — add sponsor_join_key column

If sponsor data exists in the source Parquet:
  Add the column directly to sql_views/legislation_index.sql — no sandbox needed.
If it requires joining or normalisation:
  New Python/Polars script in pipeline_sandbox/ to resolve sponsor name → join_key,
  then SQL view reads the enriched output.
```

Once either view is built, remove the corresponding TODO_PIPELINE_VIEW_REQUIRED
callout from `member_overview.py` and enable the column/tab.

---

## Reference — docs by phase

Only load the doc if you need it. Do not scan all docs.

### Before shaping

| Doc | Why |
|---|---|
| `docs/BOLD_UI_REDESIGN_PROTOCOL.md` | Greenfield design principles; primary view noise budget |
| `docs/UI_DESIGN_SYSTEM.md` | Mandatory API rules table; editorial tone; identity strip + stats + domain sections pattern |
| `docs/PIPELINE_VIEW_BOUNDARY.md` | What belongs in pipeline vs Streamlit; SQL view inspection rule |
| `docs/INTERACTION_PATTERNS.md` | Two-stage flow mechanics; sidebar order; year pills; session state keys; back button rule |
| `utility/styles/CSS_REUSE_GUIDE.md` | Existing CSS families before adding any new class; card+button `:has()` pattern |

### While building

| Doc | Why |
|---|---|
| `docs/streamlit_skill_using_layouts.md` | `st.columns` alignment, containers, `horizontal=True`, borders for section grouping |
| `docs/streamlit_skill_displaying_data.md` | `st.dataframe` column_config — ProgressColumn, NumberColumn, LinkColumn, CheckboxColumn |
| `docs/streamlit_skill_choosing_selection_widgets.md` | `st.segmented_control` vs `st.pills` vs `st.selectbox` for which control |
| `docs/streamlit_skill_using_session_state.md` | Stage 1→2 navigation; `st.session_state.pop` for back button; key scoping |
| `docs/streamlit_skill_building_multipage_apps.md` | `st.query_params` for `?member=join_key` direct-link entry |
| `docs/streamlit_skill_improving_design.md` | Material icon names; `st.badge` syntax; `st.caption` vs `st.info`; spacing polish |
| `docs/CHART_AND_TABLE_STYLE_GUIDE.md` | Altair timeline strip for Attendance tab; ranking cards vs tables decision |
| `docs/GOVERNMENT_SOURCE_LINKS.md` | Legislation tab `oireachtas_url` — labelled links, LinkColumn, what to label them |
| `docs/streamlit_architecture_reference.md` | `@st.cache_data` for query results, `@st.cache_resource` for DuckDB connection; widget state across page navigations |

### If tabs feel sparse (Step 4)

| Doc | Why |
|---|---|
| `docs/streamlit_skill_displaying_data.md` | Sparklines, progress bars, metric deltas for domain headline stats |
| `docs/UI_DESIGN_SYSTEM.md` | Hero statistic typography; leaderboard card anatomy |

### Review

| Doc | Why |
|---|---|
| `docs/BOLD_UI_REDESIGN_PROTOCOL.md` | Six-dimension required-difference checklist |
| `docs/UI_DESIGN_SYSTEM.md` | API rules table to verify compliance |
