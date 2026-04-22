# dail_tracker — improvements, optimizations, and learning roadmap

This document is written to help move `dail_tracker` from a promising civic-data project into a more reliable, analytical, and production-friendly system **without making it feel over-engineered**.

It is deliberately practical.

---

## 1) Immediate fixes (highest priority)

1. **Make the pipeline actually runnable end-to-end**
   - Fix script-name drift in `pipeline.py`.
   - Add a real `main()` to every runnable module.
   - Stop all import-time execution.
   - Make every script accept input/output paths explicitly.

2. **Remove the architecture cycle**
   - `oireachtas_api_service.py` should not depend on a gold-layer output to decide what to fetch.
   - Use authoritative member ingestion output as the source for downstream fetches.

3. **Stabilize the joins before adding more features**
   - Replace large many-to-many enrichment joins with explicit fact and dimension tables.
   - Build denormalized marts only after validating grain and cardinality.

4. **Add small automated tests before more data sources**
   - Unit tests for normalization.
   - Regression tests for one PDF parser.
   - Contract tests for one API response shape.
   - Smoke test for the gold build on tiny fixture data.

---

## 2) How to think about the pipeline

A good shape for this project is:

- **Bronze** = raw, immutable source copies and raw extracted rows
- **Silver** = cleaned and standardized tables with stable column names
- **Gold** = analytical fact tables, dimensions, and user-facing views/marts
- **Streamlit** = presentation layer only

The key rule:

> Streamlit should read from gold tables/views, not contain the business logic that defines them.

That is the right long-term direction for this repo.

---

## 3) Testing: a practical how-to for a data pipeline

You said the hardest part is **setting up the data**. That is normal. In data projects, test setup is often harder than writing the assertion.

The trick is to **not test against the full real dataset first**.

Start with **tiny, hand-made fixtures**.

### 3.1 What to test first

Start with these 4 layers:

#### A. Pure function tests
These are the easiest.

Examples:
- normalize name function
- constituency normalization
- date parsing helper
- join key generation
- bill status mapping

Why first:
- no files
- no network
- no DuckDB required
- fastest feedback

#### B. Parser tests
Examples:
- given one known attendance PDF page, does the parser extract the expected member and count?
- given one known payment PDF snippet, does it produce expected columns?

Why second:
- they protect the fragile parts
- they catch layout regressions early

#### C. Data contract tests
Examples:
- members silver table must contain `member_id`, `full_name`, `party`, `constituency`
- `member_id` must be unique
- `date` must parse as date
- null rate for critical fields must stay below a threshold

Why third:
- they help you trust the pipeline output
- they are much easier to maintain than giant end-to-end tests

#### D. Gold-model smoke tests
Examples:
- build a tiny DuckDB database from fixture files
- run gold SQL views
- assert row counts, keys, and a few known metric outputs

Why fourth:
- proves your analytical layer actually works
- lets Streamlit depend on something stable

---

### 3.2 Recommended test folder structure

```text
project/
  tests/
    fixtures/
      members_sample.json
      attendance_sample.csv
      payments_sample.csv
      lobbying_sample.csv
      attendance_sample.pdf
    test_normalise_join_key.py
    test_attendance_parser.py
    test_members_contract.py
    test_gold_models.py
  src_or_repo_files...
```

If a PDF is too large, keep a **tiny cropped sample** or a small extracted CSV version for most tests.

---

### 3.3 Your first pytest tests

Install:

```bash
pip install -e ".[dev]"
```

If your extras do not yet line up with the package config, fix that first.

Create a simple test like this:

```python
from normalise_join_key import normalise_join_key


def test_normalise_join_key_handles_accents_and_apostrophes():
    assert normalise_join_key("Ó Súilleabháin") == normalise_join_key("O'Sullivan")
```

That is the perfect first test:
- tiny
- meaningful
- stable
- central to your whole project

---

### 3.4 How to test with fixture data instead of real data

This is the most important learning point.

Do **not** make tests depend on your real output folders.

Instead:

1. Put a tiny input file into `tests/fixtures/`
2. Use `tmp_path` from pytest to create a temporary output directory
3. Run your function against the fixture
4. Assert on the resulting file or DataFrame

Example pattern:

```python
from pathlib import Path
import polars as pl
from attendance import transform_attendance_file


def test_attendance_transform_builds_expected_columns(tmp_path: Path):
    input_pdf = Path("tests/fixtures/attendance_sample.pdf")
    output_csv = tmp_path / "attendance_out.csv"

    transform_attendance_file(input_pdf, output_csv)

    df = pl.read_csv(output_csv)
    assert set(df.columns) >= {"full_name", "attendance_count", "source_file"}
    assert len(df) > 0
```

Why `tmp_path` is great:
- each test gets a clean directory
- no manual cleanup
- tests do not overwrite your real data

---

### 3.5 What to assert in data tests

Good assertions:
- expected columns exist
- row count equals expected small number
- key column is unique
- no nulls in required columns
- one known row matches expected value
- join output row count does **not explode**

Weak assertions:
- “file exists” only
- printing DataFrames with no checks
- comparing huge full outputs line-by-line unless truly needed

---

### 3.6 A useful testing progression for you

#### Stage 1 — easiest
- test normalization helpers
- test column-renaming helpers
- test date parsing helpers

#### Stage 2
- test one attendance parser example
- test one payments parser example
- test one interests parser example

#### Stage 3
- test a tiny members + attendance join
- assert no duplicate member grain after enrichment

#### Stage 4
- test DuckDB gold SQL on tiny fixture tables
- assert metric outputs

This gives you a path that grows with confidence.

---

### 3.7 Data quality tests you should add early

These are very valuable in civic-data projects.

Examples:

```python
import polars as pl


def test_member_id_unique():
    df = pl.read_csv("data/silver/members.csv")
    assert df["member_id"].n_unique() == len(df)
```

```python

def test_gold_attendance_has_one_row_per_member_date():
    df = pl.read_parquet("data/gold/fact_attendance.parquet")
    assert df.select(["member_id", "sitting_date"]).n_unique() == len(df)
```

Add checks for:
- uniqueness
- null rates
- allowed categories
- date ranges
- no unexpected duplicate joins
- row count drift warnings

---

### 3.8 Libraries worth learning for testing and data quality

Use these in order, not all at once.

#### Core
- **pytest** — main test runner
- **pytest-cov** — coverage reports
- **pytest-mock** — easier mocking

#### Great next step
- **pandera** — dataframe schema validation for pandas/polars
- **hypothesis** — property-based testing, excellent for normalization/parsing edge cases

#### Helpful for pipeline reliability
- **responses** or **requests-mock** — mock API calls
- **freezegun** — freeze time for deterministic tests

#### Optional, later
- **great_expectations** — more heavyweight data-quality framework
- **dbt + dbt-duckdb** — helpful if your gold layer becomes SQL-first and you want lineage/tests/docs, though it may be more than you need right now
- **sqlmesh** — another option if you want SQL-model orchestration with testing

My recommendation for this repo:
- start with `pytest`, `pytest-cov`, `requests-mock`, `pandera`
- add `hypothesis` later
- only consider `dbt-duckdb` if you truly want the gold layer to become model-driven SQL

---

## 4) CI/CD: a practical how-to

CI/CD sounds intimidating, but for your project it should start very small.

### 4.1 What CI should do first

Every push / PR should run:

1. Ruff / lint
2. pytest unit tests
3. one smoke test for a tiny pipeline fixture
4. packaging/install sanity check

That is enough to start.

### 4.2 Minimal GitHub Actions workflow

Create:

```text
.github/workflows/ci.yml
```

Example:

```yaml
name: CI

on:
  push:
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install
        run: |
          python -m pip install --upgrade pip
          pip install -e ".[dev]"

      - name: Lint
        run: ruff check .

      - name: Run tests
        run: pytest -q
```

Then later add a smoke test job that runs only on tiny fixtures.

---

### 4.3 What a smoke test means for your project

A smoke test is **not** “run the entire real pipeline.”

It means:
- use tiny fixture files
- run a small subset of scripts/functions
- prove the pipeline shape still works

For example:
- ingest fixture members
- transform fixture attendance
- build one gold table
- assert row count and schema

That is much more realistic for CI.

---

### 4.4 CD for you right now

You do **not** need full deployment automation yet.

Your “CD” for now can mean:
- merge only when CI passes
- create tagged releases when a pipeline version is stable
- optionally schedule a nightly refresh later

Later you can add:
- scheduled GitHub Action to refresh public data
- artifact upload of logs/manifests
- release notes for data refreshes

---

## 5) Parquet conversion plan

Your idea is a good one:

> use Parquet at the gold layer, query with DuckDB, and move analytical/business logic out of Streamlit into SQL views and fact tables.

That is exactly the right direction.

### 5.1 Why Parquet helps here

Parquet is useful because it gives you:
- smaller files than CSV
- typed columns
- faster scans
- better performance in DuckDB
- easier reuse across Python and SQL

For a repo like yours, Parquet is most useful from **silver onward**.

### 5.2 Recommended storage strategy

#### Bronze
Keep original sources as-is:
- raw API JSON
- raw lobbying CSV exports
- raw PDFs

Also keep extraction manifests.

#### Silver
Convert cleaned intermediate tables to Parquet:
- `silver_members.parquet`
- `silver_attendance.parquet`
- `silver_payments.parquet`
- `silver_interests.parquet`
- `silver_lobbying_returns.parquet`
- `silver_votes.parquet`
- `silver_legislation.parquet`

#### Gold
Store analytical tables in Parquet and expose views from DuckDB:
- dimensions as Parquet
- facts as Parquet
- marts/views in DuckDB SQL

---

### 5.3 Recommended migration path so you do not break everything

Do this gradually.

#### Phase 1
- keep existing CSV outputs
- add equivalent Parquet outputs beside them
- compare row counts and schema

#### Phase 2
- point DuckDB models at Parquet silver tables
- create gold views in SQL
- keep Streamlit still reading old outputs while validating new ones

#### Phase 3
- switch Streamlit to read from DuckDB views or gold Parquet tables
- retire duplicated Python business logic

#### Phase 4
- remove old CSV dependencies once the dashboard is stable

This avoids a dangerous “big bang” migration.

---

### 5.4 Practical Parquet patterns

Example with Polars:

```python
import polars as pl


df = pl.read_csv("data/silver/members.csv")
df.write_parquet("data/silver/members.parquet")
```

Example with DuckDB:

```sql
COPY (
  SELECT * FROM read_csv_auto('data/silver/members.csv')
) TO 'data/silver/members.parquet' (FORMAT PARQUET);
```

Example of reading many Parquet files in DuckDB:

```sql
SELECT *
FROM read_parquet('data/silver/*.parquet');
```

---

## 6) Gold-layer modeling: how to make Streamlit easier, not harder

The most important idea is this:

> model the data around the questions you want to answer, not around the source files.

That means facts + dimensions + some dashboard marts.

### 6.1 Suggested dimensions

#### `dim_member`
One row per person.

Columns might include:
- `member_id`
- `full_name`
- `normalized_name`
- `party`
- `constituency`
- `chamber`
- `start_date`
- `end_date`
- `is_current_member`

#### `dim_party`
One row per party.

#### `dim_constituency`
One row per constituency.

#### `dim_date`
One row per date.
Useful for time-based rollups.

#### Optional: `dim_lobbyist`
One row per organization/lobbying entity.

#### Optional: `dim_bill`
One row per bill / legislative item.

#### Optional: `dim_topic`
One row per normalized topic/sector if you create one.

---

### 6.2 Suggested fact tables

These are the most useful starting facts.

#### `fact_attendance`
**Grain:** one row per `member_id + sitting_date` or per `member_id + report_period`, depending on source granularity.

Measures:
- attendance_count
- possible_attendance_count
- attendance_rate

#### `fact_payment`
**Grain:** one row per `member_id + payment_period + payment_type`

Measures:
- payment_amount

#### `fact_lobby_return`
**Grain:** one row per lobbying return / filing

Measures:
- return_count = 1

Dimensions:
- lobbyist
- date
- topic
- filing period

#### `bridge_lobby_return_member`
Because one return may target multiple members.

**Grain:** one row per `return_id + member_id`

This is important because it avoids flattening many-to-many relationships into a broken wide table.

#### `fact_vote`
**Grain:** one row per `debate_title + vote_id + date + member_id`

Measures:
- voted_yes_flag
- voted_no_flag
- abstained_flag
- absent_flag

#### `fact_bill_sponsorship`
**Grain:** one row per `bill_id + member_id + sponsorship_role`

#### `fact_parliamentary_question`
**Grain:** one row per question asked

Measures:
- question_count = 1

---

### 6.3 Why bridge tables help

A lot of your data is many-to-many.

Examples:
- one lobbying return -> many politicians
- one bill -> many sponsors
- one member -> many interests
- one topic -> many filings and many questions

If you try to flatten all of that into one giant table, you create:
- duplicate rows
- cartesian explosions
- incorrect metrics
- very confusing Streamlit logic

Bridge tables are the correct way to represent those relationships.

---

### 6.4 What Streamlit should read

Do **not** make Streamlit calculate hard joins live.

Instead let Streamlit read one of these:

1. **Gold fact tables** for simple charts
2. **Gold SQL views** for reusable business logic
3. **Dashboard marts** for specific pages

### Good example marts

#### `mart_member_overview`
One row per member with:
- attendance rate
- total payments
- lobbying return count
- bills sponsored count
- questions asked count

#### `mart_lobbying_member_summary`
One row per member with:
- total lobbying returns
- distinct lobbying organizations
- top sector
- top active period

#### `mart_bill_activity_summary`
One row per bill with:
- sponsor count
- vote count
- date introduced
- stage reached

These marts are ideal for Streamlit because they are already shaped for display.

---

## 7) SQL views plan in DuckDB

Your existing SQL-learning direction is strong. The best next step is to create a dedicated folder such as:

```text
data_models/
  silver/
  gold/
  marts/
```

Example:

```text
data_models/
  gold/
    dim_member.sql
    fact_attendance.sql
    fact_payment.sql
    fact_vote.sql
    fact_lobby_return.sql
    bridge_lobby_return_member.sql
  marts/
    mart_member_overview.sql
    mart_lobbying_member_summary.sql
```

Then define a small builder script that executes them in order.

That makes your project:
- easier to reason about
- easier to test
- easier to review
- easier to debug when counts drift

---

## 8) Source-provenance metadata: how to do it clearly

This is a very good instinct.

Screenshots and explanations are useful, but they are **not enough on their own**.

A strong provenance approach combines:

1. **human-readable documentation**
2. **machine-readable metadata**
3. **lineage records for joins/transforms**
4. **clear UI/source attribution in Streamlit**

### 8.1 What provenance means here

For each dataset or row group, you want to answer:

- Where did this come from?
- When was it fetched?
- Which source file or endpoint produced it?
- Which script transformed it?
- Which join keys were used?
- Which source records contributed to this final row?

That is provenance.

### 8.2 The simplest useful implementation

At minimum, add these metadata columns to silver/gold tables wherever possible:

- `source_name`
- `source_url`
- `source_file`
- `source_format`
- `fetch_timestamp_utc`
- `source_period_start`
- `source_period_end`
- `parser_name`
- `parser_version`
- `run_id`
- `code_version`
- `record_loaded_at_utc`

Example:

```text
member_id,full_name,attendance_count,source_name,source_file,fetch_timestamp_utc,parser_name,run_id
123,Jane Doe,18,attendance_pdf,attendance_2026_02.pdf,2026-04-22T10:32:00Z,attendance.py,run_2026_04_22_001
```

That alone already helps a lot.

---

### 8.3 Better: add dataset manifests

For every output table, create a small JSON manifest beside it.

Example file:

```text
data/silver/attendance.parquet
 data/silver/attendance.manifest.json
```

Example manifest fields:

```json
{
  "dataset_name": "silver_attendance",
  "dataset_path": "data/silver/attendance.parquet",
  "source_name": "Oireachtas attendance PDFs",
  "source_urls": ["https://..."] ,
  "input_files": ["attendance_jan_2026.pdf", "attendance_feb_2026.pdf"],
  "fetch_timestamp_utc": "2026-04-22T10:32:00Z",
  "row_count": 423,
  "primary_key": ["member_id", "sitting_date"],
  "parser_name": "attendance.py",
  "parser_version": "v1",
  "code_version": "git_sha_here",
  "run_id": "run_2026_04_22_001"
}
```

This is one of the easiest high-value improvements you can make.

---

### 8.4 Even better: create a lineage table

Add a table like:

```text
meta_data_lineage
```

Columns:
- `child_dataset`
- `parent_dataset`
- `join_type`
- `join_keys`
- `transform_step`
- `code_version`
- `run_id`
- `notes`

Example:

| child_dataset | parent_dataset | join_type | join_keys | transform_step | notes |
|---|---|---|---|---|---|
| `mart_member_overview` | `fact_attendance` | left | `member_id` | `mart_member_overview.sql` | attendance summary |
| `mart_member_overview` | `fact_payment` | left | `member_id` | `mart_member_overview.sql` | payment summary |
| `fact_lobby_return_member` | `silver_lobbying_returns` | explode | `return_id` | `lobbying_processing.py` | one return to many members |

This gives you machine-readable lineage.

---

### 8.5 Row-level provenance: how far should you go?

You do **not** need full row-level lineage everywhere at first.

That can get complex.

Instead do this:

#### Bronze and silver
Keep row-level provenance where practical:
- source file name
- source page number for PDFs if useful
- source row number for CSV imports if useful
- source record ID / API URL if available

#### Gold
Usually dataset-level provenance + source IDs is enough.

Example in a gold fact table:
- `member_id`
- `return_id`
- `source_return_id`
- `run_id`

That lets you trace back without overcomplicating the model.

---

### 8.6 Are screenshots still useful?

Yes — but as **documentation**, not as provenance metadata.

Use screenshots for:
- showing how a PDF table looked before extraction
- showing the source website or API endpoint
- showing how a join was reasoned about
- explaining caveats to users/readers

But screenshots do not replace:
- metadata columns
- manifests
- lineage tables
- source IDs

The strongest approach is both:
- screenshots in docs for clarity
- metadata in the data for traceability

---

### 8.7 A very practical provenance pattern for you

For each pipeline stage:

1. Generate a `run_id`
2. Save output table
3. Save a sidecar manifest JSON
4. Insert one row into a `meta_pipeline_runs` table
5. Insert lineage rows into `meta_data_lineage`

That already gives you a solid provenance system without being enterprise-overkill.

---

## 9) Recommended metrics you could extract

You already have strong directions like revolving doors and payments. Below are more ideas grouped by theme.

### 9.1 Activity metrics
- questions asked per TD
- bills sponsored per TD
- amendments proposed per TD
- votes participated in per TD
- attendance rate over time
- committee-related activity if available

### 9.2 Influence / access metrics
- lobbying returns targeting each TD
- distinct lobbying organizations per TD
- sectors lobbying each TD
- repeat lobbyist-to-member relationships
- concentration of lobbying by a few organizations

### 9.3 Finance / ethics metrics
- payments by TD over time
- payment spikes by period
- interests declared by sector
- overlap between declared interests and lobbying sectors
- overlap between payment category and activity pattern

### 9.4 Legislative behavior metrics
- sponsorship network centrality
- voting alignment within party
- rebellion rate versus party majority position
- co-sponsorship frequency between members
- legislative activity by topic/sector

### 9.5 Public-interest / accountability metrics
- attendance versus payment relationship
- attendance versus questions asked
- lobbying intensity versus legislative activity on same topic
- lobbying intensity versus committee or office role
- members with high activity but low attendance, or the reverse

### 9.6 Time-series metrics
- monthly lobbying pressure by sector
- attendance trend by member
- payment trend by member
- legislative productivity over time
- pre/post appointment or role-change comparisons

### 9.7 Network metrics
- lobbyist -> member network degree
- bill co-sponsorship network
- issue/topic clusters
- former politician to lobbying organization relationships if you model this historically

### 9.8 Revolving doors extensions
- former politicians appearing in lobbying organizations
- time from leaving office to lobbying appearance
- sector continuity before and after office
- organizations employing multiple former officeholders
- concentration of ex-politicians across lobbying firms/sectors

---

## 10) Libraries and tools worth knowing

You asked for tools that are both helpful and regarded as good practice.

### 10.1 Strong fits for this repo

#### Data handling
- **Polars** — fast dataframe work, great for medium-to-large local data
- **DuckDB** — excellent analytical SQL engine for local Parquet workflows
- **PyArrow** — useful for Parquet/Arrow interoperability

#### Testing and quality
- **pytest**
- **pytest-cov**
- **requests-mock**
- **pandera**
- **hypothesis**

#### Reliability / operations
- **tenacity** — retries with backoff
- **structlog** or standard `logging` with JSON output — better logs
- **python-dotenv** — local env file support

#### API / schema safety
- **pydantic** — config and data model validation
- **httpx** — modern HTTP client, nice alternative to requests if you want async later

#### Developer workflow
- **ruff** — lint + formatting ecosystem
- **pre-commit** — auto-run checks before commit

### 10.2 Potentially useful later
- **dbt-duckdb** — if gold models become heavily SQL-driven
- **Ibis** — if you want Python expressions that compile to DuckDB SQL
- **Dagster** — if orchestration becomes more serious and you want assets/lineage
- **Evidence** — if you later want a more analytics-style front end than Streamlit for some pages

### 10.3 My suggested stack, realistically

For now:
- Polars
- DuckDB
- PyArrow
- pytest
- pandera
- tenacity
- pre-commit
- Ruff

That is already a very respectable modern data-engineering toolkit.

---

## 11) Recommended implementation sequence

### Phase 1 — get control of the repo
- fix script naming and imports
- add `main()` wrappers
- add 5–10 core tests
- add CI with Ruff + pytest

### Phase 2 — stabilize the data shape
- define silver schemas
- add Parquet outputs
- add manifests and run metadata
- add uniqueness/null checks

### Phase 3 — create analytical gold layer
- create dimensions
- create facts
- create bridge tables
- create 2–3 marts for Streamlit

### Phase 4 — move business logic out of Streamlit
- replace page-level joins with DuckDB views
- keep Streamlit focused on filters, charts, tables, and explanations

### Phase 5 — trust and transparency
- add provenance metadata to all major outputs
- add source/caveat panels in Streamlit
- publish methodology notes and screenshots/examples in docs

---

## 12) What “enterprise ready” would require

- deterministic pipeline runs
- CI with required checks
- data contracts and schema tests
- stable fact/dimension model
- reproducible environments
- structured logs and run summaries
- provenance metadata and lineage tables
- versioned releases
- refresh cadence and incident handling
- user-facing methodology and limitations pages

---

## 13) My direct advice for your next concrete steps

If I were sequencing this for learning and impact, I would do the next 6 things in this order:

1. Write **3 tiny pytest tests** for normalization and one parser helper.
2. Add **one GitHub Action** that runs Ruff and pytest.
3. Convert **one silver table** from CSV to Parquet.
4. Create **`dim_member`** and **`fact_attendance`** in DuckDB SQL.
5. Build **`mart_member_overview`** for Streamlit.
6. Add a **manifest JSON** beside one silver and one gold dataset.

That gets you learning in the right order:
- trust
- automation
- better storage
- better modeling
- simpler UI
- stronger provenance

