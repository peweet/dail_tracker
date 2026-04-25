# dail_tracker — improvements and optimizations


## Further reading

- [Method Chaining in Pandas — Tom Augspurger](https://tomaugspurger.net/posts/method-chaining/)
  (the Polars `pipe()` equivalent is identical in concept)
- [Refactoring Guru — Template Method pattern](https://refactoring.guru/design-patterns/template-method)
  (the `build_*` skeleton is a textbook Template Method)

## Immediate fixes (highest priority)

1. **Make the pipeline actually runnable end-to-end**
   - Fix script-name drift in `pipeline.py` (`interests.py`, `votes.py`, `tests.py`, `lobbying_processing.py` vs actual files).
   - Add a real `main()` to every runnable module.
   - Stop all import-time execution.

2. **Remove the architecture cycle**
   - `oireachtas_api_service.py` should not depend on `data/gold/enriched_td_attendance.csv` to construct legislation/question URLs.
   - Use the authoritative members API output as the source for downstream per-member fetches.

3. **Stabilize the joins**
   - Replace the current many-to-many enrichment joins with explicit grain-specific tables:
     - `dim_member`
     - `fact_attendance`
     - `fact_vote`
     - `fact_bill_sponsorship`
     - `fact_lobby_contact`
   - Only create wide denormalized marts after validating join cardinality.

4. **Add minimal automated tests before more feature work**
   - Unit tests for name normalization.
   - Regression tests for legislation flattening.
   - Golden-file tests for one attendance PDF and one payments PDF.
   - Contract tests for API response shape.

## Architecture improvements

- Introduce a small orchestration layer with explicit stages:
  - `extract_members`
  - `extract_legislation`
  - `extract_questions`
  - `extract_votes`
  - `extract_pdfs`
  - `transform_*`
  - `build_gold`
- Write stage manifests with:
  - input dependencies
  - output files
  - row counts
  - run timestamps
  - source version / fetch date
- Prefer **idempotent stages**: rerunning a stage should overwrite or version its own outputs cleanly.
- Move from ad hoc CSV passing to a documented data contract per dataset.

## Data modeling and quality

- Keep raw API and PDF-derived outputs immutable in bronze.
- Add schema validation for silver outputs.
- Add data quality checks:
  - unique key assertions
  - null-rate checks on critical columns
  - date-range sanity checks
  - duplicate detection
  - row-count drift warnings
- Replace the sorted-character fuzzy join key for primary joins where possible.
- Use canonical upstream identifiers whenever available.
- For fuzzy matches, store:
  - match method
  - confidence / rule used
  - review flag for ambiguous cases

## Performance optimizations

- Standardize on **Polars or DuckDB** for large-table transformations.
- Avoid building giant wide tables when the intended use is analytical slicing.
- Push expensive joins and aggregations into DuckDB models.
- Cache API responses locally with refresh windows.
- Use incremental fetch logic for sources with clear date boundaries.
- Parallelize network-bound fetches consistently, but cap concurrency and add backoff.

## Reliability and operations

- Add retries with exponential backoff for network calls.
- Fail loudly on partial fetches unless an explicit degraded mode is requested.
- Emit structured logs with stage name, dataset, row counts, and elapsed time.
- Add a run summary artifact after pipeline completion.
- Add checksum or source metadata tracking for downloaded PDFs / CSVs.
- Containerize the app and pipeline for reproducible execution.

## Security and compliance

- Move all environment-specific settings to environment variables or a `.env.example` pattern.
- Add dependency scanning and pinned lockfiles.
- Add a data handling note covering:
  - public-source provenance
  - refresh cadence
  - interpretation caveats
  - known join limitations
- Record upstream licensing / attribution per source.

## Developer experience

- Keep one canonical service for member ingestion.
- Split large constants / column mapping files into smaller domain modules.
- Add pre-commit hooks for Ruff and tests.
- Add typed function signatures and return contracts.
- Introduce small reusable utility modules for:
  - file IO
  - HTTP client behavior
  - logging
  - schema validation
  - path resolution

## CI/CD

- Add GitHub Actions for:
  - Ruff
  - pytest
  - packaging sanity check
  - smoke-run on a tiny fixture dataset
- Publish a versioned release when the pipeline is runnable.
- Add nightly or scheduled refresh workflows once the pipeline becomes deterministic.

## Dashboard / product readiness

- Ensure Streamlit dependencies are declared explicitly.
- Separate business logic from UI code.
- Precompute dashboard-ready tables instead of heavy page-time joins.
- Add accessibility checks and clear source citations in the UI.
- Surface dataset freshness, caveats, and missing-data explanations on every page.

## What “enterprise ready” would require

- Deterministic batch runs
- CI with mandatory checks
- Data contracts and schema tests
- Dimensional model or query layer in DuckDB/Postgres
- Structured logs, metrics, and alerting
- Reproducible environments (Docker)
- Versioned releases and rollback path
- Ownership of refresh cadence and incident handling
- Security/dependency hygiene
- User-facing documentation for trust, provenance, and limitations

## Recommended implementation sequence

### Week 1
- Fix pipeline names and import-time execution.
- Add `main()` wrappers.
- Add 5–10 core tests.

### Week 2
- Remove the circular dependency in API ingestion.
- Introduce stage outputs and row-count validation.
- Add CI.

### Week 3–4
- Redesign gold layer around facts/dimensions.
- Move heavy joins into DuckDB.
- Add data quality assertions and run summaries.

### Week 5+
- Harden dashboard packaging.
- Add scheduled refreshes, observability, and release process.

---

## 14) Streamlit strategy when you do **not** want to build a full frontend

This is a perfectly reasonable choice.

For your use case, **Streamlit is not the problem**. The problem only starts when Streamlit becomes:
- the ETL layer
- the business-rules layer
- the semantic layer
- the join-debugging layer

A good strategy for you is:

> **AI helps build the UI shell and page code. Your pipeline defines the data. DuckDB defines the metrics. Streamlit only reads, filters, and displays.**

That means:
- **pipeline owns extraction and cleaning**
- **DuckDB SQL owns business logic and metrics**
- **Streamlit owns presentation**
- **AI owns rapid UI generation and refactoring**

This lines up well with Streamlit’s own execution model: Streamlit reruns scripts top-to-bottom on interactions, so heavy work should be pushed outside the app when possible, and cached when it must stay inside the app.  
See:
- Streamlit caching overview: https://docs.streamlit.io/develop/concepts/architecture/caching
- `st.cache_data`: https://docs.streamlit.io/develop/api-reference/caching-and-state/st.cache_data
- Streamlit multipage apps: https://docs.streamlit.io/develop/concepts/multipage-apps/overview
- Streamlit Community Cloud: https://docs.streamlit.io/deploy/streamlit-community-cloud
- Streamlit blog on moving heavy work out of apps: https://blog.streamlit.io/how-to-improve-streamlit-app-loading-speed-f091dc3e2861

### 14.1 The operating model I recommend

#### What stays in the pipeline
- source ingestion
- normalization
- entity resolution
- join logic
- derived metrics
- pre-aggregations
- provenance metadata
- exports to Parquet / DuckDB

#### What stays in DuckDB SQL
- fact tables
- dimensions
- bridge tables
- marts for each dashboard page
- reusable metric definitions
- filter-ready analytical views

#### What stays in Streamlit
- page layout
- filters
- tables
- charts
- text explanations
- download buttons
- source/caveat boxes

#### What AI should do
- generate page layouts
- improve labels/help text
- build chart sections
- write page wiring code against fixed marts
- refactor repetitive UI code into components/helpers

#### What AI should **not** own
- source joins
- fuzzy matching logic
- core metric definitions
- provenance logic
- data-grain decisions

That is how you keep the app maintainable even if most of the UI is AI-assisted.

---

### 14.2 The “thin Streamlit” rule

For every page, try to follow this rule:

> **A Streamlit page should mostly contain `SELECT`, `filter`, `display`, `download`.**

If a page contains:
- complicated joins
- many group-bys
- repeated metric definitions
- name normalization
- date bucketing logic
- duplicate deduplication logic

…that logic probably belongs in DuckDB SQL or in the pipeline, not in Streamlit.

#### Bad page pattern

```python
# page loads 4 csv files
# merges them
# deduplicates them
# computes lobbying metrics
# computes attendance bands
# computes rankings
# then draws the chart
```

That is fragile and expensive.

#### Better page pattern

```python
import duckdb
import streamlit as st
import pandas as pd

@st.cache_resource
def get_conn():
    return duckdb.connect("data/gold/dail.duckdb", read_only=True)

@st.cache_data(ttl=3600)
def load_member_overview():
    con = get_conn()
    return con.sql("""
        SELECT *
        FROM mart_member_overview
        ORDER BY lobbying_return_count DESC
    """).df()

df = load_member_overview()

party = st.selectbox("Party", ["All"] + sorted(df["party"].dropna().unique().tolist()))
if party != "All":
    df = df[df["party"] == party]

st.dataframe(df, use_container_width=True)
st.download_button(
    "Download CSV",
    data=df.to_csv(index=False).encode("utf-8"),
    file_name="member_overview.csv",
    mime="text/csv",
)
```

That is the kind of Streamlit page AI can safely generate and maintain.

---

### 14.3 Build one mart per page

This is the single most practical trick for you.

Instead of asking Streamlit to understand the whole warehouse, give each page a **page mart**.

Examples:

#### `mart_member_overview`
One row per member:
- member_id
- full_name
- party
- constituency
- attendance_rate
- questions_asked
- bills_sponsored
- lobbying_return_count
- distinct_lobbyists
- total_payment_amount

#### `mart_lobbying_org_overview`
One row per lobbying org:
- organization_name
- return_count
- distinct_members_contacted
- top_sector
- latest_return_date

#### `mart_revolving_door_cases`
One row per case:
- person_name
- former_role
- left_office_date
- organization_name
- first_seen_lobbying_date
- months_to_lobbying
- sector

#### `mart_member_timeseries_monthly`
One row per member per month:
- month
- member_id
- attendance_count
- question_count
- vote_count
- lobbying_returns_targeting_member
- payments_amount

This makes Streamlit easy because each page already has the shape it needs.

---

### 14.4 A clean directory structure for the app

```text
app/
  Home.py
  pages/
    01_Member_Overview.py
    02_Lobbying.py
    03_Legislation.py
    04_Revolving_Doors.py
  components/
    filters.py
    source_box.py
    downloads.py
  data_access/
    queries.py
    contracts.py
  assets/
    logo.png
```

And keep **all SQL outside the Streamlit page files**:

```text
data_models/
  gold/
  marts/
    mart_member_overview.sql
    mart_lobbying_org_overview.sql
    mart_revolving_door_cases.sql
```

This follows Streamlit’s multipage guidance well, but keeps your own architecture much cleaner than mixing SQL and UI into page files.

---

### 14.5 A reusable page pattern for AI-generated Streamlit

If you want AI to be productive without wasting tokens, give it a stable template.

#### Example prompt pattern for AI page generation

```text
Build a Streamlit page that reads from mart_member_overview only.

Rules:
- Do not perform joins.
- Do not compute business metrics in Python.
- Only filter existing columns.
- Use st.cache_data for query results with ttl=3600.
- Use st.dataframe for the table and add a CSV download button.
- Include a source/caveat expander using the provided provenance fields.
- Keep the page under 120 lines.
```

This is far better than pasting a whole repo and asking for “make a dashboard”.

---

## 15) How to reduce token waste when using AI to develop the dashboard

This is a very smart question.

The best way to save tokens is to avoid making the AI infer your business logic from giant code files or full datasets.

### 15.1 Give AI **contracts**, not your whole warehouse

For each page, create a tiny machine-readable contract.

Example: `dashboard_contracts/member_overview.yaml`

```yaml
page: member_overview
source_view: mart_member_overview
grain: one row per member
primary_key:
  - member_id
filters:
  - party
  - constituency
  - is_current_member
metrics:
  - attendance_rate
  - questions_asked
  - bills_sponsored
  - lobbying_return_count
  - distinct_lobbyists
  - total_payment_amount
display_columns:
  - full_name
  - party
  - constituency
  - attendance_rate
  - lobbying_return_count
  - total_payment_amount
downloads:
  - csv
notes:
  - Do not recompute any metric in Streamlit.
  - Read from DuckDB or pre-exported parquet only.
```

This tiny file is gold for AI tools.

It tells the model:
- what the page is
- what table to use
- what the grain is
- what filters exist
- which metrics are already computed
- what it is allowed to do

That can reduce token usage dramatically.

---

### 15.2 Create a column dictionary for marts

Example: `schemas/mart_member_overview.json`

```json
{
  "table": "mart_member_overview",
  "grain": "one row per member",
  "columns": {
    "member_id": "stable member key",
    "full_name": "display name of member",
    "party": "current or latest mapped party",
    "constituency": "member constituency",
    "attendance_rate": "precomputed attendance ratio from fact_attendance",
    "questions_asked": "count of parliamentary questions",
    "bills_sponsored": "count of bills sponsored",
    "lobbying_return_count": "count of linked lobbying returns",
    "distinct_lobbyists": "count of distinct lobbying organizations",
    "total_payment_amount": "sum of payment amounts in source currency"
  }
}
```

AI writes much better UI code when it gets:
- the table schema
- the grain
- the allowed filters
- the caveats

instead of dozens of Python files.

---

### 15.3 Use **gold marts** as AI-facing APIs

Think of each mart as an API contract for the UI.

That means:
- stable names
- stable column types
- stable metric definitions
- stable grain

If you do that, AI can regenerate the UI many times without breaking the analytics.

That is one of the biggest wins you can create for yourself.

---

### 15.4 Give AI **sample rows**, not full data dumps

For AI development, create a tiny CSV per mart with 5–20 rows.

Example:
- `samples/mart_member_overview_sample.csv`
- `samples/mart_lobbying_org_overview_sample.csv`

This is enough for:
- layout generation
- chart generation
- column formatting
- filter UI generation

without wasting tokens on large real data.

---

### 15.5 Create a “UI prompt pack”

A great pattern is to store one small doc that any AI tool can reuse.

Example: `doc/streamlit_ui_contract.md`

Include:
- app design rules
- color/theme rules
- table formatting rules
- the list of marts
- one-paragraph summary of each page
- the rule “no business logic in pages”
- examples of acceptable page code

Then you can paste that instead of the whole repo every time.

---

## 16) Keep logic in the pipeline: a practical design

### 16.1 Target architecture

```text
raw source files / APIs
        ↓
bronze raw copies
        ↓
silver normalized parquet tables
        ↓
gold fact / dimension / bridge tables
        ↓
marts for pages
        ↓
streamlit UI
```

This is the right architecture for your project.

### 16.2 What “keep logic in the pipeline” means in practice

Move these into the pipeline or SQL layer:
- date bucketing
- deduplication
- name matching
- sector normalization
- attendance rollups
- payment rollups
- lobbying-member linking
- rankings
- percentiles
- “top N” source views
- any metric that appears on more than one page

Keep these out of the pipeline:
- widget state
- selected filter values
- sort order chosen by the user
- CSV export action
- local chart formatting

---

### 16.3 A useful split between physical marts and dynamic queries

Not every view must be precomputed to final chart grain.

Use two layers:

#### Layer A — durable marts
Examples:
- `mart_member_overview`
- `mart_revolving_door_cases`
- `mart_member_monthly_activity`

These should be persisted and documented.

#### Layer B — thin UI queries
Examples:
- `SELECT * FROM mart_member_overview WHERE party = ?`
- `SELECT month, SUM(question_count) FROM mart_member_monthly_activity GROUP BY month`

This balance keeps the pipeline responsible for business logic, while Streamlit still gets enough flexibility for interactivity.

---

## 17) Source provenance metadata: a more explicit implementation example

Your instinct to show screenshots and explain joins is good. Keep that.

But add a machine-readable layer too.

### 17.1 Recommended provenance tables

#### `meta_pipeline_runs`

```sql
CREATE TABLE IF NOT EXISTS meta_pipeline_runs (
    run_id VARCHAR PRIMARY KEY,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    code_version VARCHAR,
    git_branch VARCHAR,
    git_commit VARCHAR,
    pipeline_version VARCHAR,
    triggered_by VARCHAR,
    notes VARCHAR
);
```

#### `meta_dataset_registry`

```sql
CREATE TABLE IF NOT EXISTS meta_dataset_registry (
    dataset_name VARCHAR,
    dataset_path VARCHAR,
    layer VARCHAR,
    grain_description VARCHAR,
    primary_key VARCHAR,
    owner_step VARCHAR,
    refresh_strategy VARCHAR,
    description VARCHAR
);
```

#### `meta_dataset_lineage`

```sql
CREATE TABLE IF NOT EXISTS meta_dataset_lineage (
    run_id VARCHAR,
    child_dataset VARCHAR,
    parent_dataset VARCHAR,
    relationship_type VARCHAR,
    join_keys VARCHAR,
    transform_step VARCHAR,
    notes VARCHAR
);
```

#### `meta_source_registry`

```sql
CREATE TABLE IF NOT EXISTS meta_source_registry (
    source_name VARCHAR,
    source_type VARCHAR,
    source_url VARCHAR,
    source_license VARCHAR,
    fetch_method VARCHAR,
    caveats VARCHAR
);
```

These tables make your process inspectable.

---

### 17.2 Example row-level provenance fields in a silver table

For a silver lobbying table:

```text
return_id
source_return_id
source_name
source_url
source_file
source_row_number
fetch_timestamp_utc
parser_name
parser_version
run_id
record_loaded_at_utc
```

For a PDF-derived attendance row:

```text
member_id
report_period
attendance_count
source_file
source_page
source_name
parser_name
parser_version
run_id
```

That gives you concrete traceability.

---

### 17.3 Example sidecar manifest

`data/gold/mart_member_overview.manifest.json`

```json
{
  "dataset_name": "mart_member_overview",
  "layer": "mart",
  "grain": "one row per member",
  "built_from": [
    "dim_member",
    "fact_attendance",
    "fact_payment",
    "fact_parliamentary_question",
    "bridge_lobby_return_member"
  ],
  "run_id": "run_2026_04_22_001",
  "code_version": "abc1234",
  "row_count": 174,
  "columns": [
    "member_id",
    "full_name",
    "party",
    "constituency",
    "attendance_rate",
    "questions_asked",
    "bills_sponsored",
    "lobbying_return_count",
    "total_payment_amount"
  ],
  "caveats": [
    "Lobbying links depend on available names/identifiers in source data",
    "Attendance source granularity may vary by reporting period"
  ]
}
```

This is simple and powerful.

---

### 17.4 How to surface provenance in Streamlit

Use an expander at the bottom of each page.

```python
with st.expander("Source and methodology"):
    st.markdown("""
    **Source mart:** mart_member_overview  
    **Grain:** one row per member  
    **Built from:** dim_member, fact_attendance, fact_payment, bridge_lobby_return_member  
    **Last refresh:** 2026-04-22 10:32 UTC  
    **Run ID:** run_2026_04_22_001  

    Caveats:
    - Lobbying relationships may be many-to-many.
    - Counts may differ from headline totals if source updates occur.
    """)
```

That is great for clarity, but the important point is that the app should read those values from metadata tables or a manifest, not hard-code them.

---

### 17.5 Screenshots: where they fit

Screenshots are excellent in:
- methodology docs
- parser docs
- caveat pages
- notebooks / writeups
- a “How the data is built” section in the app

A good pattern is to add:
- one screenshot of the raw PDF/CSV/API
- one screenshot of the cleaned silver table
- one diagram of the join path into the gold mart

That is very clear and useful for trust.

---

## 18) Cheap hosting strategy: simplest path first

You said you want something cheap and succinct. So optimize for **low ops burden**, not perfect architecture on day one.

### 18.1 Recommended order

#### Option 1 — Streamlit Community Cloud
Use this first if:
- the app can be public
- the data is not huge
- you want the easiest setup
- you want GitHub-connected deployment with minimal ops

Why:
- Streamlit says Community Cloud is free, connects directly to GitHub repos, and handles containerization for you.
- It is explicitly aimed at non-commercial, personal, and educational apps.

Official docs:
- https://docs.streamlit.io/deploy/streamlit-community-cloud

#### Option 2 — Hugging Face Spaces (Docker)
Use this if:
- you want more control over the runtime
- you want a Docker-based deployment
- you may want public/protected/private visibility options
- you want simple git-driven rebuilds

Why:
- Hugging Face says Spaces rebuild and restart on each push.
- Spaces supports public, protected, and private visibility.
- Free CPU Basic hardware currently offers 2 vCPU, 16 GB RAM, and 50 GB of non-persistent disk.

Official docs:
- https://huggingface.co/docs/hub/spaces-overview
- https://huggingface.co/docs/hub/spaces-sdks-docker

### 18.2 My practical recommendation for you

For your project today:

1. **Host the Streamlit app on Streamlit Community Cloud**
2. **Run the data refresh in GitHub Actions**
3. **Commit or publish small Parquet / DuckDB outputs if size allows**
4. **Keep the app read-only**
5. **Move all expensive transforms to the pipeline**

That is probably the cheapest, lowest-friction setup.

If Community Cloud resource limits become annoying, move the same app to **Hugging Face Docker Spaces** without changing your overall architecture much.

---

### 18.3 Cheap deployment shape

```text
GitHub repo
  ├─ pipeline code
  ├─ data_models SQL
  ├─ Streamlit app
  ├─ tests
  └─ GitHub Actions

GitHub Actions
  ├─ run pipeline on schedule
  ├─ validate outputs
  └─ publish gold artifacts

Streamlit app
  └─ reads prebuilt gold tables / marts only
```

That is the “cheap and succinct” version.

---

### 18.4 What not to do yet

Do **not**:
- build a custom React frontend
- add a backend API unless you really need it
- make Streamlit compute giant joins live
- let AI continuously regenerate your core metric logic
- deploy a complicated orchestration stack too early

Those things all increase maintenance cost faster than they increase value for your project right now.

---

## 19) Example page contracts you can actually use

### 19.1 Member overview page

```yaml
page: member_overview
source: mart_member_overview
grain: one row per member
filters:
  - party
  - constituency
  - is_current_member
sort_defaults:
  - lobbying_return_count desc
charts:
  - type: bar
    x: full_name
    y: lobbying_return_count
  - type: scatter
    x: attendance_rate
    y: questions_asked
table_columns:
  - full_name
  - party
  - constituency
  - attendance_rate
  - lobbying_return_count
  - total_payment_amount
download: csv
provenance: true
business_logic_in_page: forbidden
```

### 19.2 Revolving doors page

```yaml
page: revolving_doors
source: mart_revolving_door_cases
grain: one row per revolving-door case
filters:
  - former_role
  - organization_name
  - sector
charts:
  - type: histogram
    x: months_to_lobbying
  - type: bar
    x: sector
    y: case_count
table_columns:
  - person_name
  - former_role
  - left_office_date
  - organization_name
  - first_seen_lobbying_date
  - months_to_lobbying
download: csv
provenance: true
business_logic_in_page: forbidden
```

These are small enough for AI tools and strong enough to keep the app stable.

---

## 20) Extra libraries and tools that fit your “thin app, strong pipeline” approach

You already asked for libraries in general; these are the ones I would particularly associate with this strategy.

### 20.1 Very good fits

- **streamlit-option-menu** or careful native navigation use — lightweight navigation help if you do not want a custom frontend
- **plotly** — strong interactive charts inside Streamlit
- **Altair** — elegant charting with less code for many analytical charts
- **pyarrow** — useful for Parquet typing and interoperability
- **Ibis** — if you want Python to generate/query DuckDB expressions while keeping logic closer to SQL semantics
- **pydantic** — great for config schemas and page contracts
- **PyYAML** — easy YAML-based contracts for pages and metrics
- **pandera** — schema validation for DataFrames before writes
- **tenacity** — retries for brittle fetch steps
- **pre-commit** — enforce quality before commit
- **duckdb-engine** — helpful only if you later want SQLAlchemy-style integration

### 20.2 Libraries worth learning with caution

- **dbt-duckdb** — very useful if your gold layer becomes deeply SQL-model-driven, but it is an extra abstraction; only add it if you want model/test/docs ergonomics
- **Dagster** — strong if orchestration, assets, and lineage become major concerns, but probably heavier than you need right now
- **Great Expectations** — useful, but more overhead than pytest + pandera at your current stage
- **Evidence** — worth knowing if one day you want a more report-oriented UI than Streamlit

---

## 21) Reading list: official docs, articles, and learning links

### 21.1 Streamlit
- Streamlit Community Cloud: https://docs.streamlit.io/deploy/streamlit-community-cloud
- Multipage apps overview: https://docs.streamlit.io/develop/concepts/multipage-apps/overview
- Caching overview: https://docs.streamlit.io/develop/concepts/architecture/caching
- `st.cache_data`: https://docs.streamlit.io/develop/api-reference/caching-and-state/st.cache_data
- `st.dataframe`: https://docs.streamlit.io/develop/api-reference/data/st.dataframe
- Blog: *How to improve Streamlit app loading speed*  
  https://blog.streamlit.io/how-to-improve-streamlit-app-loading-speed-f091dc3e2861
- Blog: *10 Principles for Keeping the Vibe while Coding Streamlit Apps*  
  https://blog.streamlit.io/10-principles-for-keeping-the-vibe-while-coding-streamlit-apps-b5e62cc8497d
- Resource limits FAQ / performance links:  
  https://discuss.streamlit.io/t/faq-this-app-has-gone-over-its-resource-limits/62973

### 21.2 pytest / CI
- pytest getting started: https://docs.pytest.org/en/stable/getting-started.html
- pytest good practices: https://docs.pytest.org/en/7.1.x/explanation/goodpractices.html
- GitHub Actions Python build/test docs: https://docs.github.com/en/actions/tutorials/build-and-test-code/python

### 21.3 DuckDB / Parquet / semantics
- DuckDB Parquet overview: https://duckdb.org/docs/current/data/parquet/overview.html
- DuckDB Python API: https://duckdb.org/docs/current/clients/python/overview.html
- DuckDB metadata functions: https://duckdb.org/docs/current/sql/meta/duckdb_table_functions.html
- DuckDB `COMMENT ON`: https://duckdb.org/docs/stable/sql/statements/comment_on
- MotherDuck article: *Why Semantic Layers Matter — and How to Build One with DuckDB*  
  https://motherduck.com/blog/semantic-layer-duckdb-tutorial/
- MotherDuck article: *Modern Data Warehouse Use Cases: Dashboards & Live Apps*  
  https://motherduck.com/learn/modern-data-warehouse-use-cases/

### 21.4 Hosting
- Streamlit Community Cloud: https://docs.streamlit.io/deploy/streamlit-community-cloud
- Hugging Face Spaces overview: https://huggingface.co/docs/hub/spaces-overview
- Hugging Face Docker Spaces: https://huggingface.co/docs/hub/spaces-sdks-docker

### 21.5 Your own repo note to keep building on
- `doc/lobbying_sql_learning.md` is already the right instinct:
  move analytical business logic out of Streamlit/Python and into SQL on DuckDB.
  Keep leaning into that.

---

## 22) YAML-directed frontend development: what it is, how to use it, and how it relates to AI tools

This pattern has a few common names:
- **config-driven UI**
- **schema-driven UI**
- **metadata-driven UI**
- **contract-driven frontend**

For your project, the best description is probably:

> **thin Streamlit frontend over DuckDB marts with YAML page contracts**

That means:
- DuckDB / SQL defines the data meaning
- YAML defines the page contract
- Streamlit renders the page
- AI helps generate or restyle the page code

### 22.1 What the YAML is actually doing

The YAML is **not** your data model and it is **not** where business logic should live.

It is a small contract that answers questions like:
- which mart or view should this page read?
- what is the grain?
- which filters are allowed?
- which metrics already exist?
- which columns should appear in the table?
- which charts should be shown?
- should the page offer CSV export?
- should provenance/caveats be displayed?

A useful way to think about it:

```text
SQL mart = truth
YAML contract = page instructions
Streamlit = renderer
AI = page builder / restyler
```

### 22.2 Why this helps with AI-generated UI

AI is usually much better at:
- layout
- labels and help text
- page structure
- chart sections
- table formatting
- download buttons

AI is much worse at:
- preserving data grain
- getting joins right
- keeping KPI definitions stable
- understanding subtle provenance and caveats

So the safe pattern is:
- keep your metric definitions in SQL
- keep your page configuration in YAML
- let AI write the Streamlit presentation layer against those fixed contracts

### 22.3 What should go in YAML vs SQL vs Streamlit

#### Put this in SQL / DuckDB
- joins
- aggregations
- metric definitions
- deduplication
- ranking logic
- time bucketing
- fact/dimension modeling
- provenance rollups

#### Put this in YAML
- page title
- data source name
- grain description
- list of filters
- display labels
- visible columns
- chart declarations
- export options
- which provenance fields to show

#### Put this in Streamlit
- layout
- widget placement
- tabs / expanders
- page text
- data display
- CSV download button
- small interactive filtering on existing columns

### 22.4 A good YAML example for your style of dashboard

```yaml
contract_version: 1
page:
  id: member_overview
  title: Member Overview
  description: Overview of member activity, lobbying links, attendance, and payments.

data_source:
  type: duckdb
  relation: mart_member_overview
  grain: one row per member
  primary_key:
    - member_id

filters:
  - column: party
    label: Party
    type: multiselect
  - column: constituency
    label: Constituency
    type: multiselect
  - column: is_current_member
    label: Current member only
    type: boolean

metrics:
  - column: member_id
    label: Members
    aggregation: count_distinct
  - column: questions_asked
    label: Questions Asked
    aggregation: sum
  - column: lobbying_return_count
    label: Lobbying Returns
    aggregation: sum
  - column: total_payment_amount
    label: Total Payments
    aggregation: sum

charts:
  - type: bar
    title: Lobbying returns by member
    x: full_name
    y: lobbying_return_count
  - type: scatter
    title: Attendance vs questions asked
    x: attendance_rate
    y: questions_asked

table:
  default_sort:
    column: lobbying_return_count
    direction: desc
  columns:
    - full_name
    - party
    - constituency
    - attendance_rate
    - questions_asked
    - lobbying_return_count
    - total_payment_amount

export:
  csv: true

provenance:
  show: true
  fields:
    - source_mart
    - grain
    - run_id
    - last_refresh_utc
    - caveats

rules:
  no_business_logic_in_streamlit: true
```

### 22.5 A bad YAML example

```yaml
page: member_overview
join_logic:
  - join members to attendance on fuzzy_name
  - join lobbying to members using sorted_char_key
attendance_rate_formula: attended_sittings / total_possible_sittings
```

This is bad because the YAML is starting to become an ETL or semantic layer. That logic should live in SQL models or upstream transforms.

### 22.6 A good prompt to give an AI tool

```text
Build a Streamlit page from this YAML contract.

Rules:
- Query only the relation named in data_source.relation.
- Do not perform joins in Python.
- Do not redefine metrics already listed in the contract.
- Use Streamlit widgets only for filtering existing columns.
- Use st.cache_data(ttl=3600) for the query result.
- Render metric cards, charts, a dataframe, provenance expander, and CSV export.
- Keep the file concise and presentation-focused.
```

That prompt is small, cheap, and reliable compared with pasting your whole repo.

### 22.7 How this compares to Claude Skills

This is **similar in spirit**, but not the same thing.

A Claude Skill or subagent config is mainly about **agent behavior and reusable instructions/resources**. Your YAML page contract is about **frontend rendering and data-page structure**.

The overlap is that both approaches:
- reduce repeated prompt context
- create reusable small contracts
- make AI outputs more consistent
- reduce the need to paste large amounts of project context each time

So the analogy is useful, but your pattern is better described as **config-driven frontend development**, not as a Claude Skill.

### 22.8 What to search for and read

These search terms will help a lot:
- `config driven ui`
- `schema driven ui`
- `metadata driven frontend`
- `server driven ui`
- `contract driven frontend`
- `json schema ui schema`

#### Strong references
- JSON Forms docs: https://jsonforms.io/docs/
- JSON Forms UI schema: https://jsonforms.io/docs/uischema/
- react-jsonschema-form docs: https://rjsf-team.github.io/react-jsonschema-form/docs/
- react-jsonschema-form usage docs: https://rjsf-team.github.io/react-jsonschema-form/docs/usage/
- Backstage software templates: https://backstage.io/docs/features/software-templates/
- Backstage writing templates: https://backstage.io/docs/features/software-templates/writing-templates/
- Streamlit architecture docs: https://docs.streamlit.io/develop/concepts/architecture
- Streamlit caching docs: https://docs.streamlit.io/develop/concepts/architecture/caching

#### Helpful explanatory articles / posts
- Dev.to beginner article on config-driven UI: https://dev.to/lovishduggal/mastering-config-driven-ui-a-beginners-guide-to-flexible-and-scalable-interfaces-3l91
- Digia post on server-driven UI: https://www.digia.tech/post/server-driven-ui-migration-zero-release-mobile-architecture
- Anthropic subagents / YAML frontmatter docs for the analogy: https://code.claude.com/docs/en/sub-agents

### 22.9 Tips specific to your project

#### Tip 1 — one page, one mart
Try to make each page depend on one main mart. That keeps the YAML small and the Streamlit code thin.

#### Tip 2 — keep the YAML vocabulary small
Define a tiny set of allowed widget types, for example:
- `metric`
- `table`
- `bar_chart`
- `line_chart`
- `scatter`
- `download_csv`
- `provenance_box`

This prevents chaos.

#### Tip 3 — version your contracts
Add fields like:

```yaml
contract_version: 1
mart_version: 2026_04
```

That will make refactors much safer later.

#### Tip 4 — add sample rows for AI work
For every mart, keep a 5–20 row sample CSV. Then AI can build pages using the contract + sample data without needing your full dataset.

#### Tip 5 — store page contracts beside the app
Recommended structure:

```text
app/
  Home.py
  pages/
  dashboard_contracts/
    member_overview.yaml
    lobbying_overview.yaml
    revolving_doors.yaml
```

#### Tip 6 — document “what not to do”
Put this in your UI contract doc:
- do not perform joins in Streamlit
- do not define metrics in page code
- do not fuzzy-match in page code
- do not read multiple raw CSV files in a page
- do not hide caveats if provenance exists

That helps both humans and AI.

### 22.10 The simplest way to apply this right now

A very practical first move:

1. Pick one page only, such as `member_overview`
2. Build one stable DuckDB mart for it
3. Write one YAML contract for it
4. Keep the Streamlit page under ~100–150 lines
5. Ask AI only to improve layout and chart presentation

Once that pattern works, repeat it for `lobbying_overview` and `revolving_doors`

That is much safer than trying to convert the whole dashboard architecture in one go.
