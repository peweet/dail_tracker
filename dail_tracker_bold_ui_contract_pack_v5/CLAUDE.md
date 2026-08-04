# Dáil Tracker — Claude Operating Context

## Mission

Dáil Tracker is a civic transparency tool for Irish parliamentary data. It helps citizens and journalists see the evidence behind parliamentary accountability.

## Proven reference products

The execution of Dáil Tracker is deliberately informed by two proven, trusted civic accountability tools:

- **theyworkforyou.com** (UK) — parliamentary transparency for the UK. Simple, factual, highly trusted by citizens and press. [https://www.theyworkforyou.com](https://www.theyworkforyou.com)
- **theyvoteforyou.eu** — European Parliament voting and accountability tracker. [https://theyvoteforyou.eu](https://theyvoteforyou.eu)

This is not plagiarism or IP infringement. Their interfaces and code are not copied. The intention is to keep execution close to their **proven UX decisions**: simple primary views, clean member profiles, minimal noise, year-based navigation, and a focus on facts over decoration.

When in doubt about a UI decision, ask: does this look and feel like something theyworkforyou would ship?

## Core product identity

Tone: **Direct. Civic. Accountable.**

Aesthetic: **editorial accountability journalism** — investigative newspaper crossed with a data reference tool.

Data tables and numbers are the hero. The interface should feel serious, legible, and trustworthy. It should not feel like a generic Streamlit dashboard.

Avoid:
- generic Streamlit defaults
- purple-gradient AI dashboards
- glassmorphism
- fintech-style cards
- decorative charts with no user question
- bare custom JavaScript (injecting `<script>` tags via `st.markdown(..., unsafe_allow_html=True)` — this pattern is banned)
- page-local CSS systems

Custom JavaScript is permitted only via the CCv2 API (`st.components.v2.component()`) following the `building-streamlit-custom-components-v2` skill. The purpose must be specific and functional — not decorative. Feature-bloat JavaScript remains forbidden regardless of delivery mechanism.

## Correct architecture

Streamlit is a thin **data-semantics** layer, not a thin **user-experience** layer.

Pipeline and in-process DuckDB registered analytical views own:
- joins
- fuzzy matching
- normalisation
- metric definitions
- cohorts
- rankings
- rollups
- flags
- raw Parquet access
- PDF/API/scraping logic
- registered analytical view creation

Streamlit owns:
- page layout
- filter controls
- parameter binding
- retrieval SQL only
- formatting
- tables
- charts over already-shaped data
- CSV export
- provenance display
- empty states
- official source links
- member drilldowns
- interaction design
- visual hierarchy

## Pipeline safety rule

The main pipeline (`pipeline.py`, `enrich.py`, `normalise_join_key.py` and their dependencies)
runs in strict order and is fragile. Do not touch it.

### SQL analytical views — safe to create directly

SQL views at the end of the pipeline are cheap to write and cheap to discard.
Create new analytical views directly in `sql_views/` — they exist only to serve the frontend
and have no side effects on the pipeline order.

```text
sql_views/<view_name>.sql   ← create here directly; always a SELECT, never INSERT/UPDATE/DELETE
```

### New Python/Polars enrichment — sandbox only

If a new analytical view requires Python/Polars enrichment before it can be expressed as SQL
(joins across datasets, complex string normalisation, flag computation, fuzzy matching),
place **all new Python/Polars code** in the sandbox directory:

```text
pipeline_sandbox/   ← all new enrichment/join/transformation code lives here
```

Rules for `pipeline_sandbox/`:
- Self-contained — does not import from or call into the main pipeline
- Produces a Parquet or CSV output file that a SQL view can then read
- Can be examined, tested, and discarded without risk to the main pipeline
- No file in `pipeline_sandbox/` is called by `pipeline.py` or `enrich.py` directly

The sandbox exists so new enrichment logic can be reviewed and proven before (if ever)
being integrated into the main pipeline. Most sandbox code never needs integration —
the SQL view reading its output is sufficient.

### Never

- Edit `pipeline.py`, `enrich.py`, or `normalise_join_key.py`
- Add imports of main pipeline modules into sandbox files
- Create views that call `pipeline.py` functions
- Treat `pipeline_sandbox/` output as production until reviewed

## Data access rule

The app exposes analytical SQL views through an in-process DuckDB connection.

Streamlit page files query approved registered views only.

Do not assume a persistent `.duckdb` database file unless the page contract explicitly says so.

Forbidden in Streamlit page files:
- connecting directly to a persistent `.duckdb` file as a modelling shortcut
- registering views
- `read_parquet`
- `parquet_scan`
- `CREATE VIEW`
- `CREATE TABLE`
- raw data access
- SQL joins used for modelling
- multi-dimensional GROUP BY used for business metrics (belongs in pipeline)
- HAVING, WINDOW functions
- fuzzy matching
- API calls
- PDF parsing

## Retrieval SQL rule

Claude may generate only retrieval SQL in Streamlit.

Standard retrieval shape:

```sql
SELECT approved_columns
FROM approved_registered_view
WHERE approved_filter_column approved_operator parameter
ORDER BY approved_sort_column ASC|DESC
LIMIT approved_limit
```

**Allowed aggregate functions** — scalar aggregates for hero stats and date bounds are presentation-layer concerns:

```sql
SELECT COUNT(*), COUNT(DISTINCT member_id), MAX(vote_date), MIN(vote_date)
FROM approved_registered_view
WHERE ...
```

`COUNT(*)`, `COUNT(DISTINCT col)`, `MAX(col)`, `MIN(col)` are permitted.
`GROUP BY` with multiple columns, `HAVING`, and `WINDOW` functions remain forbidden — those define business metrics and belong in the pipeline.

If the UI needs missing data, do not improvise. Write:

```text
TODO_PIPELINE_VIEW_REQUIRED: <specific missing view/column/filter/metric/source_url>
```

## UI rule: boldness is required

Do not treat “thin Streamlit layer” as “boring UI.”

The existing page is a **functional reference**, not a design reference.

Preserve:
- backend/data semantics
- approved registered views
- approved columns
- approved filters
- existing working data access
- export requirements
- provenance requirements

Rethink boldly:
- layout
- section order
- filter placement
- visual hierarchy
- chart choice
- table ergonomics
- drilldown flows
- member focus panels
- source-link presentation
- mobile flow
- empty/loading/error states
- editorial copy
- reusable CSS/components

A UI redesign must be materially different from the old page. It should not be a safe refactor or a light restyle.

## CSS rule

**Primary CSS file — add all new classes here:**

```text
utility/shared_css.py   ← inject_css() — loaded by every page via inject_css()
```

**Legacy file — do not add new styles here:**

```text
utility/styles/base.css ← loaded only by lobbying_2.py for backwards compatibility
```

Before writing any new CSS, read `utility/shared_css.py` to see what already exists.

Known class families:
- `dt-*` — core layout chrome (`dt-hero`, `dt-kicker`, `dt-dek`, `dt-badge`, `dt-callout`, `dt-provenance-box`)
- `att-hall-*` — attendance hall of fame/shame cards (`att-hall-card-good`, `att-hall-card-bad`, `att-hall-rank`, `att-hall-medal`, `att-hall-body`, `att-hall-name`, `att-hall-meta`, `att-hall-badge-good`, `att-hall-badge-bad`)
- `att-list-*` — attendance ranked list rows (`att-list-row`, `att-list-rank`, `att-list-pill`, `att-list-pill-name`, `att-list-pill-meta`)
- `int-rank-*` — interests leaderboard cards
- `pay-*` — payments ranked list (`pay-name-row`, `pay-name-rank`, `pay-name-body`, `pay-name-body-name`, `pay-name-body-pos`, `pay-taa-pill`, `pay-count-pill`, `pay-amount-badge`, `pay-amount-badge-num`, `pay-amount-badge-label`, `pay-identity-card`, `pay-identity-card-name`, `pay-identity-card-meta`)
- `dt-nav-anchor` — vertical alignment shim for `→` buttons adjacent to cards

**Card background rule:** Always use `background: #ffffff` for card and pill elements. Never `background: var(--surface)` — that resolves to warm beige and makes cards invisible against the page background.

Add reusable global classes when needed. Do not create a new page-local CSS system. No inline `style=""` attributes.

## Page contract rule

Every page is driven by:

```text
utility/page_contracts/<page>.yaml
```

The contract is the source of truth for:
- page purpose
- approved registered views
- approved columns
- approved filters
- allowed SQL operators
- temporal behaviour
- tables
- charts
- metrics
- exports
- source links
- member drilldowns
- empty states
- acceptance tests
- UI creativity budget

## Token discipline

For a page task, read only:
1. the target page runbook
2. the target page contract
3. the target page file
4. shared CSS
5. relevant `utility/ui` helpers
6. data-access helper only if required
7. navigation only if routing changes

Do not scan generated data folders.
Do not read every page contract.
Do not read every docs file.
Do not inspect unrelated pages unless the runbook asks you to.

## Final response format

Return only:
1. Files changed
2. Main UI changes made
3. Registered views queried
4. Retrieval SQL used, if any
5. TODO_PIPELINE_VIEW_REQUIRED items
6. Test commands
