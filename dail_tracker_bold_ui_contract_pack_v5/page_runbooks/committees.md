# Page Runbook — Committees

## Target

Page ID: `committees`  
Page file: `utility/pages_code/committees.py`  
User question: **Which members sit on which committees, and how can those assignments be inspected?**

## Architectural assumption

This page queries **in-process DuckDB registered analytical views**.

It does not connect to a persistent `.duckdb` file.
It does not call `read_parquet`.
It does not register views.
It does not perform business logic in Streamlit.

Approved registered views in the contract include:

```text
v_committee_assignments, v_committee_member_detail, v_committee_sources
```

Shared patterns:

```text
member_drilldown, csv_export, government_source_links
```



## Files Claude may modify

- `utility/pages_code/committees.py`
- `utility/styles/base.css`
- `utility/ui/components.py`
- `utility/ui/member_drilldown.py`
- `utility/ui/table_config.py`
- `utility/ui/export_controls.py`
- `utility/ui/source_links.py`
- `utility/page_contracts/committees.yaml`

## Files Claude must not modify

- `pipeline.py`
- `data/`
- `sql_queries/`

## Step 1 — Explore only

Paste this into Claude Code:

```text
We are working on `committees` only.

Read only:
1. `CLAUDE.md`
2. `page_runbooks/committees.md`
3. `utility/page_contracts/committees.yaml`
4. `utility/page_contracts/_shared_ui_policy.yaml`
5. `utility/page_contracts/_interaction_patterns.yaml`
6. `utility/pages_code/committees.py`
7. `utility/styles/base.css`
8. relevant helpers in `utility/ui/`

Do not edit files.
Do not scan generated data folders.
Do not inspect unrelated pages.

Return:
1. relevant files
2. current page structure
3. current data access pattern
4. UI weaknesses
5. where a bold redesign could differ from the current page
```

## Step 2 — Shape bold redesign before code

```text
Create a bold UI redesign plan for `committees`.

The existing page is a functional reference, not a design reference.

Do not edit code yet.

Preserve:
- backend behaviour
- data semantics
- approved registered views
- approved columns and filters
- CSV export requirements
- provenance/source requirements

Rethink boldly:
- page structure
- information hierarchy
- controls
- filter placement
- chart/table presentation
- detail or drilldown flows
- source-link display
- empty states
- mobile order
- shared CSS polish

The redesigned page must be materially different from the current page in at least six ways.

Return:
1. current UI problems
2. proposed redesigned layout
3. interaction flow
4. chart/table choices
5. components/helpers to create or reuse
6. CSS classes to add to shared `base.css`
7. files to modify
8. TODO_PIPELINE_VIEW_REQUIRED items
```

## Step 3 — Update contract if needed

```text
Update `utility/page_contracts/committees.yaml` only if the redesign plan requires contract changes.

Keep:
- `data_access.mode: duckdb_in_process_registered_analytical_views`
- `persistent_duckdb_file: null`
- `TODO_PIPELINE_VIEW_REQUIRED` for missing data

Do not point at a persistent DuckDB database.
Do not add Streamlit-side modelling.
```

## Step 4 — Implement bold UI redesign

```text
Implement the `committees` UI redesign.

This is a UI redesign, not a safe refactor.

You may modify only the allowed files listed in `page_runbooks/committees.md`.

The final UI must be materially different from the current page.

Required:
1. stronger editorial hero
2. clearer primary controls
3. better evidence hierarchy
4. improved table configuration
5. useful chart/timeline only if it answers the user question
6. detail/drilldown or focus section where relevant
7. source/provenance display
8. CSV export of current displayed view
9. better empty states
10. shared CSS polish

Forbidden:
- backend changes
- raw Parquet scans
- persistent DuckDB file assumption
- view registration in Streamlit
- joins/groupby/metric definitions in Streamlit
- hardcoded absolute paths
- page-local CSS system

If data is missing, add:
TODO_PIPELINE_VIEW_REQUIRED: <specific missing view/column/filter/metric/source_url>

Before final response, inspect for:
- `.merge(`
- `.join(`
- `.groupby(`
- `.pivot`
- `read_parquet`
- `parquet_scan`
- `CREATE VIEW`
- hardcoded `C:\`
- backend file modifications

Return only:
1. files changed
2. main UI changes made
3. registered views queried
4. retrieval SQL used, if any
5. TODO_PIPELINE_VIEW_REQUIRED items
6. test commands
```

## Step 5 — If it still looks too similar

```text
The redesign is still too close to the old `committees` page.

Do another UI pass.

Treat the existing page as an anti-reference for layout. Preserve functionality, not structure.

Required:
1. replace the top section with a stronger editorial hero
2. move primary controls into a prominent command/filter bar
3. reorganise the page into clearer evidence sections
4. improve the table configuration and labels
5. add or improve detail/focus section
6. connect CSV export visually to the table it exports
7. improve provenance/source links
8. add better empty states
9. add reusable shared CSS classes, not page-local CSS

Still forbidden:
- backend changes
- new metrics
- joins/groupby
- raw Parquet scans
- registered view creation
- hardcoded paths
- page-local CSS systems

Return:
1. what was too similar
2. what changed to make it materially different
3. files changed
4. test commands
```

## Step 6 — Review

```text
Review `committees`.

Check separately:

A. Data/architecture:
- no backend files modified
- no raw Parquet scans
- no persistent DuckDB file assumption
- no view registration in Streamlit
- no joins/groupby/metric definitions in Streamlit
- no hardcoded paths
- TODO_PIPELINE_VIEW_REQUIRED used correctly

B. UI quality:
- materially different from old page
- civic/editorial feel
- strong hero
- clear primary controls
- usable table
- useful chart/timeline if present
- source/provenance visible
- CSV export clear
- empty states useful
- mobile flow reasonable

Return:
1. pass/fail
2. high-severity issues
3. medium-severity issues
4. minimal fixes
5. whether another boldness pass is needed
```
