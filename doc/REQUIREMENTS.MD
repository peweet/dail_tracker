# REQUIREMENTS.md — Roadmap, Architecture, and Action Items

**Last updated: 16 Apr 2026**

This file supersedes and consolidates the two prior REARCHITECTURE documents. It is the single source of truth for what has been built, what remains, and where the project is going.

## Table of Contents

1. [What the Codebase Does](#1-what-the-codebase-does)
2. [Current Pipeline](#2-current-pipeline)
3. [Scope Principles](#3-scope-principles)
4. [Active Bugs](#4-active-bugs)
5. [The Lobbying Layer — DuckDB → Streamlit Plan](#5-the-lobbying-layer--duckdb--streamlit-plan)
6. [Committee as a "Hardworking Politician" Metric](#6-committee-as-a-hardworking-politician-metric)
7. [Bills Roadmap](#7-bills-roadmap)
8. [Medallion Architecture Plan](#8-medallion-architecture-plan)
9. [DuckDB as the Analytical Layer](#9-duckdb-as-the-analytical-layer)
10. [Data Quality Testing](#10-data-quality-testing)
11. [Data Enrichment — APIs Not Yet Used](#11-data-enrichment--apis-not-yet-used)
12. [Temporal Analysis](#12-temporal-analysis)
13. [Dashboard Path](#13-dashboard-path)
14. [Interview Prep Angles](#14-interview-prep-angles)
15. [Portfolio Polish — What Makes This Pop](#15-portfolio-polish--what-makes-this-pop)
16. [Action Items Checklist](#16-action-items-checklist)

---

## 1. What the Codebase Does

The pipeline ingests open data published by the Oireachtas, the Standards in Public Office Commission, and the Register of Lobbying, and collates it into a unified dataset about sitting TDs.

Sources currently ingested:

- **TD members** — `/v1/members`, filtered to the 34th Dáil
- **Legislation** — `/v1/legislation`, per TD, concurrent fetch
- **Parliamentary questions** — `/v1/questions`, per TD, concurrent fetch
- **Attendance** — PDF scrape via PyMuPDF, Jan 2023 → Feb 2026
- **Parliamentary Standard Allowance** — PDF scrape, Jan 2024 → Feb 2026
- **Register of Members' Interests** — PDF scrape, nine declaration categories
- **Lobbying register** — CSV export from lobbying.ie

The outputs are flat CSVs in `members/`, `bills/`, and `lobbyist/`. A DuckDB gold layer and Streamlit dashboard are on the roadmap.

---

## 2. Current Pipeline

```
INGESTION (Bronze)
  oireachtas_api_service.py  → members.json, legislation_results.json, questions_results.json
  pdf_enpoint_check.py       → health-checks all PDF + API URLs

TRANSFORMATION (Silver)
  flatten_members_json_to_csv.py  → flattened_members.csv
  attendance.py                   → aggregated_td_tables.csv
  payments.py                     → aggregated_payment_tables.csv, top_tds_by_payment_2024.csv
  member_interest.py              → member_interests_grouped.csv
  lobbying_processing.py          → combined_lobbying_data.csv
                                  → cleaned_output.csv
                                  → lobby_break_down_by_politician.csv
                                  → most_lobbied_politicians.csv
                                  → lobby_count_details.csv

ENRICHMENT (Silver → Gold)
  enrich.py                       → enriched_td_attendance.csv
  flatten_service.py              → drop_cols_flattened_bills.csv
```

Key technical decisions made to date:

- **PyMuPDF** replaced camelot for PDF extraction (roughly 2x faster on 244-page PDFs, handles page-spanning records natively).
- **JSON** replaced pickle for inter-script data transfer (security + portability).
- **polars** for joins and filtering (faster tabular operations); **pandas** for JSON flattening (better at deep nested structure).
- **Sorted-character fuzzy key** (`normalise_join_key.py`) handles Irish-name variants across datasets.
- **Python logging** in the API service writes to `pipeline.log`.
- **Unified API service** — members, legislation, questions now share one client with scenario parameter.
- **Column configuration centralised** in `utility/select_drop_rename_cols_mappings.py`.
- **`pyproject.toml`** with optional dependency groups (`duckdb`, `ocr`, `dev`).
- **`__main__` guards** added so scripts are importable without side effects.

---

## 3. Scope Principles

Two scope decisions shape everything else. Full rationale is in `DATA_LIMITATIONS.md §0`.

1. **Sitting politicians only.** 34th Dáil members. Former TDs and private citizens who have left politics are excluded. This keeps the dataset focused on accountability for people currently exercising public authority, reduces the fuzzy-join collision surface, and avoids surfacing private citizens' data beyond what the upstream sources already publish.
2. **Dáil only, not Seanad.** Parameterisable later — out of scope for now.

These are not accidental gaps. They are product decisions and should be defended as such if anyone asks.

---

## 4. Active Bugs

Estimates are rough — implementation time assuming familiarity with the codebase. Fix order is top-to-bottom.

| # | Bug | Where | Effort |
|---|---|---|---|
| 1 | `lobbying_processing.py` uses `df` (last file) instead of `lobbying_df` (combined) for DPO explode | `lobbying_processing.py:86` | 5 min + 30 min verify |
| 2 | Double-join on `most_lobbied_politician` produces `total_count` and `total_count_right` | `lobbying_processing.py:119` | 10 min |
| 3 | `endpoint_checker` returns inside `for` loop — only tests first URL | `pdf_enpoint_check.py:60` | 10 min |
| 4 | 4 TDs with fada characters fail API call (Unicode encoding) | `oireachtas_api_service.py` | 30 min |
| 5 | Hardcoded Windows paths across multiple scripts | several files | 2 hours |
| 6 | `member_api_request()` called twice in `oireachtas_api_service.py` | `oireachtas_api_service.py:48,104` | 10 min |
| 7 | Missing TD — 127 of 128 in attendance PDF | `attendance.py` | 1 hour to diagnose |
| 8 | TDs with `0` attendance dropped by `dropna()` | `attendance.py` | 30 min |
| 9 | `flatten_members_json_to_csv.py` short-circuit bug: `if os.path.exists('...' or os.path.exists(...))` | `flatten_members_json_to_csv.py` | 10 min |
| 10 | `flatten_members_json_to_csv.py` produces hundreds of "Null" columns | same file | 2 hours |
| 11 | No banned-names filter or position filter on lobbying DPO targets | `lobbying_processing.py` | 2 hours |
| 12 | No canonical-name mapping for known TD variants in lobbying data | `lobbying_processing.py` | 1 hour |

Items 1–6 are small and should be the next commit. Items 7–12 are meatier.

---

## 5. The Lobbying Layer — DuckDB → Streamlit Plan

The lobbying pipeline is the project's differentiator. No other Irish civic-tech project systematically joins lobbying.ie data with member interests and parliamentary activity. The plan here is to get it from "flat CSVs" to "interactive Streamlit dashboard backed by DuckDB" without boiling the ocean.

### Why DuckDB sits in the middle

Streamlit is constrained: the full cleaned lobbying dataset (all DPO explodes × all activity explodes × all periods) is too big to load into memory on every user interaction, and re-running the polars cleaning on every click is wasteful. DuckDB between them fixes both problems:

1. The pipeline loads cleaned CSVs into DuckDB once.
2. Streamlit queries DuckDB on demand — pushdown filters, indexed joins, precomputed aggregates.
3. The dashboard feels snappy because the expensive work happened at build time, not in the click path.

This is the same architecture `robmcelhinney/lobbyieng` uses (with SQLite instead of DuckDB). It works.

### Tables to land in DuckDB

Minimum viable set for a lobbying dashboard:

```sql
-- Reference
dim_politician        -- one row per TD, sitting only
dim_lobby_org         -- one row per registered lobby organisation
dim_period            -- one row per lobbying return period
dim_committee         -- one row per Oireachtas committee

-- Facts
fact_lobbying_contact -- one row per (lobby_org × politician × period × activity)
fact_lobbying_return  -- one row per return submitted by a lobby org

-- Precomputed rollups (materialised tables, refreshed per pipeline run)
mart_most_lobbied_td           -- TD rankings by lobbying contact count
mart_most_prolific_lobbyist    -- lobby org rankings by contact count
mart_biggest_movers            -- period-over-period deltas per TD
mart_shared_lobbyists          -- which TDs share the most lobbyists
```

### Streamlit pages, in order of build

1. **TD profile page** — pick a TD, see their lobbying contacts, most frequent lobbyists, topics lobbied on, period timeline. Pull from `fact_lobbying_contact` filtered by `politician_id`.
2. **Lobbying organisation page** — pick an org, see which TDs they lobby, what topics, what method (email/meeting/phone).
3. **Rankings page** — top-N most lobbied, top-N most prolific lobbyists, biggest movers. Straight reads from the mart tables.
4. **Network page (later)** — shared-lobbyists graph. Degree centrality, self-join on `fact_lobbying_contact.lobby_org_id`.
5. **Conflict-of-interest page (much later)** — keyword match between `lobby_org.main_activities_of_organisation` and `member_interest.description`. This is the headline feature and the hardest to get right.

### Why this ordering

Pages 1–3 answer questions people already want answered and are shippable in roughly a day each once DuckDB is populated. Page 4 is nice-to-have and computationally cheap on top of the mart tables. Page 5 is the one that generates press coverage if it works — and generates corrections if it doesn't. Build it last, show it to a few domain people before going public.

### Effort

| Stage | Effort |
|---|---|
| Load cleaned CSVs into DuckDB, create dim/fact tables | 3 hours |
| Build mart tables (SQL `CREATE TABLE AS SELECT` from facts) | 2 hours |
| Streamlit TD profile page | 4 hours |
| Streamlit lobbying-org page | 3 hours |
| Streamlit rankings page | 2 hours |
| Network page | 8 hours |
| Conflict-of-interest page | 12+ hours (keyword matching, human review) |

---

## 6. Committee as a "Hardworking Politician" Metric

Attendance alone is a poor proxy for TD effort. A TD can attend every plenary sitting and sit on zero committees. Another can attend half the plenary sittings and carry three committee memberships — the second TD is doing more parliamentary work than the first. The current enriched dataset has both data points but no combined metric.

### What data we have

`flattened_members.csv` contains up to 5 committee slots per TD in wide format (`committee_1_name_english`, `committee_2_name_english`, ... `committee_5_name_english`). These need to be unpivoted before they're useful for analysis.

### Proposed metric — "Work Surface"

A composite score combining attendance and committee load. A crude starter version:

```
work_surface = (sitting_days_attendance / max_possible_days)
             + (committee_count * committee_weight)
             + (is_minister * ministerial_weight)
             + (bill_sponsorship_count * bill_weight)
```

Each weight is tunable. The ministerial weight exists because ministers typically attend fewer plenary sessions — they're in their department running the brief. Without the adjustment, ministers look lazy; with it, they look correctly "busy elsewhere". The weights themselves are a judgment call and should be surfaced as Streamlit sliders so users can see how sensitive the ranking is to the weighting scheme. If the top-10 list reshuffles dramatically when committee weight goes from 0.1 to 0.3, that tells you the ranking is fragile and the user should treat it as one view, not the view.

### The unpivot pattern to land in DuckDB

```sql
-- Wide → long: one row per TD per committee
CREATE TABLE fact_committee_membership AS
SELECT politician_id, 1 AS slot, committee_1_name_english AS committee_name
FROM stg_members WHERE committee_1_name_english IS NOT NULL
UNION ALL SELECT politician_id, 2, committee_2_name_english FROM stg_members WHERE committee_2_name_english IS NOT NULL
UNION ALL SELECT politician_id, 3, committee_3_name_english FROM stg_members WHERE committee_3_name_english IS NOT NULL
UNION ALL SELECT politician_id, 4, committee_4_name_english FROM stg_members WHERE committee_4_name_english IS NOT NULL
UNION ALL SELECT politician_id, 5, committee_5_name_english FROM stg_members WHERE committee_5_name_english IS NOT NULL;
```

DuckDB also supports `UNPIVOT` syntax natively, which is cleaner:

```sql
UNPIVOT stg_members
ON committee_1_name_english, committee_2_name_english, committee_3_name_english,
   committee_4_name_english, committee_5_name_english
INTO NAME committee_slot VALUE committee_name
WHERE committee_name IS NOT NULL;
```

This becomes `dim_committee_membership` and lets you answer "who sits on the Finance committee" with a single `WHERE committee_name = 'Finance'` instead of five OR conditions across wide columns.

### Caveats to surface in the Streamlit UI

The metric is a model, not a measurement. Several things it cannot see:

- Committee attendance vs committee membership — a TD listed on a committee may rarely attend its meetings
- Speaking quality — a TD who speaks thoughtfully once is not half as useful as one who speaks carelessly twice
- Constituency work — done outside the chamber, entirely invisible to this dataset
- Bill authorship vs bill signing — co-sponsors may add little beyond a name

The dashboard should say this explicitly on the rankings page. Presenting "Work Surface" as a definitive ranking would be misleading; presenting it as one lens among several is defensible.

**Effort:** 1 hour for the unpivot, 1 hour for the initial weighted score, 2–3 hours to build the Streamlit page with weight sliders.

---

## 7. Bills Roadmap

Bills are ingested (`bills/legislation_results.json`) and flattened (`bills/drop_cols_flattened_bills.csv`), but nothing downstream uses them yet. The join back to TDs exists via `unique_member_code` but no bill-specific analytics have been built.

### Low-hanging bill questions

These are achievable with the current flattened CSV once it's loaded into DuckDB:

- **Bills-per-TD** — count per TD, by party, by constituency
- **Enactment rate by party** — what proportion of a party's bills actually pass?
- **Time-to-enactment** — from first-stage date to enacted date, per bill
- **Bill source split** — Government vs Private Member, by party
- **Most-legislating TDs** — rank by `COUNT(*)` on bills
- **Bill stage progression** — which bills stalled at which stage?

### The bills × lobbying intersection

This is where bills become interesting in combination. Once bills are loaded:

- **Did lobbied TDs sponsor bills on the lobbied topic?** — keyword match on `bill.short_title` vs `lobbying_contact.subject_matter` for the same TD.
- **Bills introduced by ministers vs backbenchers** — does portfolio alignment predict what a minister sponsors?
- **Co-sponsorship network** — which TDs co-sponsor with which? Add after fixing the index-0-only co-sponsor truncation (see `DATA_LIMITATIONS.md §5`).

### Bill stages as events — a clean temporal fact table

The `bill_stages_*_event_dates_0_date` columns are currently flattened into wide format. Unpivoting them into an event table unlocks time-series analysis:

```
fact_bill_event
  bill_id
  stage_name        -- e.g. 'Second Stage', 'Committee Stage', 'Report Stage', 'Enacted'
  event_date
  chamber
```

With this, `LAG()` and `LEAD()` window functions give stage-to-stage durations per bill.

### Effort

| Item | Effort |
|---|---|
| Fix bill sponsor index-0 truncation | 1 hour |
| Load bills into DuckDB, create `fact_bill` and `fact_bill_event` | 3 hours |
| Per-TD bill rollup in the Streamlit TD profile page | 1 hour |
| Bills × lobbying keyword match | 4 hours |

---

## 8. Medallion Architecture Plan

The medallion pattern (bronze → silver → gold) separates raw fetched data from cleaned data from analytical data. Three layers, three responsibilities, three directories.

```
bronze/     Raw as received — PDFs, API JSON, lobbying CSVs
            Gitignored, cheap to regenerate from sources

silver/     Cleaned, validated, schema-enforced
            Columns renamed, types enforced, joins keyed
            Gitignored (too large for git)

gold/       Analytical — DuckDB database + precomputed marts
            Optional Parquet exports for portability
            The dashboard's source of truth
```

The migration is mechanical but not trivial — scripts need their input/output paths repointed, and `config.py` needs to become the authoritative location for every path. Roughly 8 hours to restructure and verify.

The current state is effectively "bronze + silver in the same tree" with no gold. The first DuckDB work creates the gold layer; the directory move is a subsequent cleanup.

---

## 9. DuckDB as the Analytical Layer

| Feature | SQLite | DuckDB |
|---|---|---|
| Setup | Stdlib in Python | `pip install duckdb` |
| Best for | Transactional / CRUD | Analytical queries |
| Direct CSV/Parquet read | No | `read_csv_auto()`, `read_parquet()` |
| Polars integration | Manual | Native |
| SQL dialect | Standard SQL | PostgreSQL-compatible |

DuckDB reads CSVs and Parquet directly without a load step. It speaks PostgreSQL SQL, so anything you learn here transfers to server-side Postgres. It supports `UNPIVOT`, `QUALIFY`, `GROUPING SETS`, and other analytical SQL that SQLite does not. For an analytical-read workload with a single writer (the pipeline) it's the right choice.

**Effort:** 3 hours to set up, load all silver CSVs, and create initial views.

Useful reading:

- DuckDB docs — https://duckdb.org/docs/
- DuckDB SQL dialect reference — https://duckdb.org/docs/sql/introduction
- "DuckDB: The Little Database that Can" — https://motherduck.com/blog/duckdb-the-little-database-that-can/

---

## 10. Data Quality Testing

Structure:

```
test/
├── conftest.py              Shared fixtures (paths, sample data)
├── test_attendance.py       TD count, max totals, Irish name coverage
├── test_payments.py         Payment amounts valid, no missing TDs
├── test_member_interest.py  All 9 interest categories present
├── test_lobbying.py         DPO counts, banned names, position filtering
├── test_enrich.py           Join completeness, no duplicates
└── test_normalise.py        Fuzzy key edge cases (fadas, apostrophes, hyphens)
```

Initial assertions worth writing first:

- `aggregated_td_tables.csv` has ≥ 127 distinct identifiers
- `sitting_total_days` never exceeds the Dáil limit (103 for 2024)
- Every TD in the enriched output has a non-null party
- No TD appears twice in `enriched_td_attendance.csv`
- `normalise_join_key("Ó Súilleabháin") == normalise_join_key("O'Sullivan")`

**Effort:** 4 hours for the initial suite. Expand per-bugfix — every bug fix should land a test that would have caught it.

Useful reading:

- pytest docs — https://docs.pytest.org/en/stable/
- "Testing Data Pipelines" (Microsoft Engineering Playbook) — https://microsoft.github.io/code-with-engineering-playbook/machine-learning/ml-testing/
- Pandera (schema validation for dataframes) — https://pandera.readthedocs.io/

---

## 11. Data Enrichment — APIs Not Yet Used

### Quick-win URL changes (1 hour total)

| Change | Impact | Effort |
|---|---|---|
| Add `date_start=2024-03-22` to questions URL | 60–80% payload reduction for veteran TDs | 1 line |
| Add `bill_status=Current,Enacted` to legislation URL | Eliminates Withdrawn/Lapsed server-side | 1 line |
| Add `skip`-based pagination loop | Prevents silent truncation at `limit=1000` | ~10 lines |

### Endpoints worth adding as new scenarios

| Endpoint | Value | Effort |
|---|---|---|
| `/v1/divisions` — votes | **Highest-value enrichment.** Individual TD votes (Tá/Níl/Staon). Unlocks party loyalty scoring, rebellion detection, attendance-vs-voting gap analysis. | 4 hours |
| `/v1/debates` — speeches | Speech count per TD, word volume, topic dominance. | 4 hours |
| `/v1/houses` | Dáil term dates and seat counts — removes hardcoded `2024-03-22`. | 30 min |
| `/v1/constituencies` | Canonical constituency reference table. | 30 min |
| `/v1/parties` | Canonical party reference table. | 30 min |

### External enrichment

| Source | Value | Effort |
|---|---|---|
| CRO (services.cro.ie) | Lookup lobby org company number → directors/shareholders/filings | 6 hours |
| CSO PxStat | Constituency demographics (population, unemployment, age) | 3 hours |
| Wikidata SPARQL | TD photos and social media links | 2 hours |
| Historical attendance PDFs (2022, 2023) | Year-over-year trends | 1 hour |

---

## 12. Temporal Analysis

The attendance PDF dates are individual `dd/mm/yyyy` values — genuine time-series data points, not just a count. Once parsed, they enable:

- **Attendance gap analysis** — days between consecutive attendances per TD; histogram reveals common absence patterns
- **Weekly/monthly patterns** — do some TDs attend Tuesdays but skip Thursdays?
- **Session coverage** — build a reference calendar of sitting days, compute true attendance rate (`sitting_days / total_sitting_days`)
- **Change-over-time** — with multi-year PDFs, trend lines per TD
- **Temporal joins** — attendance dates vs division dates, debate dates, lobbying return periods, bill stage dates

The lobbying data has its own temporal dimension (`lobbying_period`). Biggest-movers-between-periods is the most useful aggregate — which TDs saw the largest jump or drop in lobbying contacts between consecutive returns.

**Effort:** 4 hours for the core analysis (gap analysis, monthly patterns, biggest movers).

Useful reading:

- pandas time series user guide — https://pandas.pydata.org/docs/user_guide/timeseries.html
- Kaggle Time Series micro-course (free, pragmatic) — https://www.kaggle.com/learn/time-series

---

## 13. Dashboard Path

```
Start here                For more learning              For production
───────────              ──────────────────              ──────────────
Streamlit          →     FastAPI + HTMX + Chart.js  →   FastAPI + React/Vue
(~4 hours MVP)           (~12 hours)                    (~40+ hours)
```

Streamlit is the right answer for now. One `app.py` that connects to DuckDB, renders a searchable TD table, shows attendance-by-party, and exposes the lobbying views described in §5. Ship it, iterate on it, outgrow it.

FastAPI + HTMX is worth graduating to if/when you want to separate data access from presentation (better for learning web-architecture patterns) or expose a public API.

Useful reading:

- Streamlit docs — https://docs.streamlit.io
- "Streamlit + DuckDB" — https://motherduck.com/blog/streamlit-duckdb/
- FastAPI docs — https://fastapi.tiangolo.com
- HTMX docs — https://htmx.org/docs/

---

## 14. Interview Prep Angles

The project is well-suited to practising three categories of interview material: SQL, data modelling, and system design. What follows is intended as a study guide — you have real data to make every concept concrete rather than synthetic-textbook.

### 14.1 SQL you can practise on your own data

The analytical SQL patterns that come up in interviews are almost all practicable here once data is in DuckDB.

**Window functions.** The bread and butter of analytical interviews. `RANK() OVER (PARTITION BY party ORDER BY sitting_days DESC)` gives you each party's attendance ranking. `LAG()` gives gap analysis. `SUM() OVER (PARTITION BY party)` gives percentage-of-party-total. DuckDB supports `QUALIFY` for filtering on window results without a subquery — worth knowing.

**CTEs and conditional aggregation.** Multi-stage queries that build intermediate results and then aggregate them. "For each party, count ministers and count TDs on 3+ committees" is one CTE.

**Self-joins.** Shared-committee-membership or shared-lobbyist queries are textbook self-joins. Use `a.id < b.id` to avoid mirror pairs.

**UNPIVOT.** Wide tables like `flattened_members.csv` with five committee slots need unpivoting to be analytically useful. DuckDB's native `UNPIVOT` syntax is cleaner than the UNION ALL alternative, but both are worth being fluent in.

**GROUPING SETS and ROLLUP.** Sub-total and grand-total in one pass. Party-by-constituency breakdown with party sub-totals and an overall grand total — one query instead of three UNIONs.

**Correlated subqueries.** "TDs who sponsor more bills than the average TD" — the inner query depends on the outer row. Common interview pattern.

**PIVOT.** Reshape long data to wide. Attendance by party × year is a classic pivot.

Useful reading:

- Mode's SQL Analytics Training — https://mode.com/sql-tutorial/ (the best free SQL curriculum on the internet)
- DataLemur for interview practice — https://datalemur.com
- StrataScratch — https://www.stratascratch.com (real company interview questions)
- "SQL queries don't start with SELECT" — https://jvns.ca/blog/2019/10/03/sql-queries-don-t-start-with-select/ (Julia Evans — how SQL actually executes vs how it reads)
- Use The Index, Luke — https://use-the-index-luke.com/ (SQL performance / indexing, the canonical resource)

### 14.2 Dimensional modelling (Kimball / star schema)

Your data is currently one-big-wide-table. Restructuring into a star schema is a concrete piece of portfolio work and a commonly-asked interview topic.

The target shape:

```
Dimensions (attributes that describe):
  dim_politician   id, name, party, constituency, year_elected, is_minister, dail_term
  dim_committee    committee_id, name, committee_type, chamber
  dim_lobby_org    org_id, name, cro_number, main_activities, website
  dim_date         date_key, year, month, quarter, day_of_week
  dim_bill         bill_id, title, source (Govt/PM), status, introduced_date

Facts (measurable events):
  fact_attendance           politician_id, date_key, attendance_type
  fact_committee_membership politician_id, committee_id, slot
  fact_lobbying_contact     politician_id, org_id, date_key, method, subject_matter
  fact_bill_sponsorship     bill_id, politician_id, is_primary
  fact_bill_event           bill_id, date_key, stage_name
  fact_payment              politician_id, date_key, amount, payment_type
  fact_vote (future)        politician_id, date_key, division_id, direction
```

The star schema name comes from the shape: facts in the centre, dimensions radiating out. Every fact row links to dimensions via foreign keys. Queries join fact-to-dimension rather than dimension-to-dimension.

This is what interview questions like "design a schema for a parliamentary activity tracker" are asking for. Being able to say "here's the one I built for the Irish Dáil, here are the tradeoffs I made" is materially stronger than the textbook answer.

**Key concepts to be comfortable with:**

- **Conformed dimensions.** `dim_date` and `dim_politician` are shared across every fact table — joins work the same way everywhere. This is why they live in `dim_` tables rather than as columns in each fact.
- **Grain.** Every fact table has one declared grain — "one row per X". If you ever need a row at a different grain, you're building a new fact table, not modifying this one. `fact_attendance` is one row per TD per date. `fact_bill_event` is one row per bill per stage-event. Mixing grains is the most common modelling mistake.
- **Additivity.** Measures are additive (can sum across any dimension), semi-additive (can sum across some dimensions — e.g., bank balance adds across accounts but not across time), or non-additive (ratios, percentages). Knowing which your measures are tells you what aggregations are safe.

**SCD Type 2 — slowly changing dimensions.** TDs change party. The current `flattened_members.csv` only shows current party. Real dimensional modelling preserves history:

```
dim_politician_history
  politician_id
  party
  effective_from   -- date joined this party
  effective_to     -- date left (NULL = current)
  is_current       -- boolean flag
```

Now a query can ask "what was Brian Stanley's party when he voted on Bill X in 2023?" by joining on `date BETWEEN effective_from AND COALESCE(effective_to, '9999-12-31')`. The Oireachtas API already has party date ranges in the member JSON (`parties_0_party_dateRange_start`) — you're currently dropping these, but preserving them is the prerequisite for SCD Type 2.

**Effort:** 4 hours to restructure into the star schema above. 2 hours for SCD Type 2 on party.

Useful reading:

- Kimball Group's "Dimensional Modeling Techniques" — https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/
- "The Data Warehouse Toolkit" by Ralph Kimball (the Kimball book — chapter 1 and 2 are the key chapters; the rest is reference)
- dbt's dimensional modelling guide — https://www.getdbt.com/blog/kimball-dimensional-model (modern interpretation)
- Holistics' SCD explainer — https://www.holistics.io/blog/what-is-slowly-changing-dimension/

### 14.3 System design angles

This project is small (174 TDs, a few thousand bills, tens of thousands of lobbying records) but the architectural decisions you're making are the same as larger systems. Interviews reward being able to articulate why a choice was made and how it would scale.

**The three axes to be able to discuss for this project:**

1. **Batch vs streaming.** The pipeline is batch — weekly or daily runs, not real-time. Defensible because the upstream sources publish on lag (weekly PDFs, quarterly lobbying returns) — streaming would be pretending to have data you don't. If asked "how would you make this real-time", the honest answer is "I wouldn't, the source isn't", and that's a better answer than waving at Kafka.

2. **Read vs write pattern.** One writer (the pipeline), many readers (the dashboard). This is why DuckDB is right — optimised for analytical reads, relaxed about write contention. If this were a user-generated-content app with many writers, Postgres would be right. Being able to explain why you chose *this* database for *this* workload is half the system design interview.

3. **Scale headroom.** Current data fits in memory on a laptop. If you added every national parliament in Europe (~30x growth), DuckDB still handles it on a laptop. If you added every local council (~1000x growth), you need partitioned Parquet on object storage and a query engine that reads it (DuckDB can, or Spark/Trino). The scaling story isn't "I'd rewrite it" — it's "this architecture scales to N, and here's what would change at 10N".

**System design topics that come up and that you can anchor to this project:**

- **Caching.** Where are the natural caches? (Precomputed mart tables in DuckDB, the `@st.cache_data` decorator in Streamlit.) What's the invalidation strategy? (Refresh marts on each pipeline run; evict Streamlit cache when the DuckDB file's mtime changes.)
- **Idempotency.** If the pipeline runs twice, the output should be the same. Current pipeline fails this — each run re-fetches everything. Fixing it means tracking what's been fetched (manifest table) and only re-fetching the delta.
- **Failure modes.** API returns 503. Fada-URL fails. PDF format changes. Each has a different right response — retry with backoff, skip with log, alert loudly. Know which you're doing and why.
- **Data contracts.** Pandera schemas between stages are a data contract — "the output of attendance.py MUST have columns X, Y, Z with these types". If the contract breaks, the downstream stage fails loudly instead of producing garbage.
- **Observability.** You have `pipeline.log`. What would you put in it to debug "the number of TDs dropped by 5 overnight"? (Per-stage counts before/after, per-source row counts, a checksum per output file.)

**Useful reading (system design):**

- "Designing Data-Intensive Applications" by Martin Kleppmann (the book — chapters 1, 2, 3, 10 for batch systems)
- ByteByteGo System Design Newsletter — https://blog.bytebytego.com/ (short, practical, interview-shaped)
- "The System Design Primer" — https://github.com/donnemartin/system-design-primer (GitHub, free)
- High Scalability blog — http://highscalability.com/ (case studies of real-world architectures)
- Jordan Has No Life (YouTube) — https://www.youtube.com/@jordanhasnolife5163 (system design walkthroughs at interview pace)

---

## 15. Portfolio Polish — What Makes This Pop

Honest feedback: the project is good work. It is not yet *striking* portfolio work. The difference between "this is a solid data pipeline" and "I want to talk to this person about a role" is not in code quantity — it's in a small number of things that make an employer or interviewer stop scrolling.

What would make this one stop the scroll, in rough order of impact:

**1. A live dashboard.** A recruiter who clicks your GitHub sees a repo. A recruiter who clicks your GitHub, then clicks a link, and sees a working dashboard with real Irish MP data they can interact with — that's a different category of portfolio. Streamlit Community Cloud hosts free. Even an ugly MVP with three charts beats a perfect README with no deployment.

**2. One headline insight.** Every good data project has a single sentence the author can deliver at a dinner party. "I found that TDs who receive the most lobbying contacts have 12% lower plenary attendance than the median" — even if the finding turns out to be a correlation with no causal story, the *question* is what's memorable. Produce three or four such findings in a `FINDINGS.md` or a blog post. Even better: tweet one with a chart. That's what gets seen.

**3. A conflict-of-interest match.** The intersection of lobbying × member interests is genuinely novel for Ireland. If you can produce even one well-documented case where a TD holds financial interests in a sector that is actively lobbying them — even if the conclusion is "this is disclosed and within the rules" — that's the kind of work that gets referenced. Do the careful version: human-review the matches before publishing, be specific about what is and isn't alleged, err toward boring language.

**4. A written post explaining what you built.** Not README content — a narrative. "I spent 8 weeks scraping the Dáil. Here's what I learned about Irish parliamentary data." 1500 words, three charts, linked to the repo and the dashboard. Personal blog, Medium, LinkedIn — doesn't much matter where. This is the thing that makes the repo gettable-to. Recruiters and hiring managers click blog posts; they don't always click code.

**5. A single striking visualisation.** A choropleth of Ireland coloured by attendance-per-constituency. A chord diagram of shared lobbyists between TDs. A calendar heatmap of one TD's attendance over three years. Pick one, make it really good — high contrast, clear labelling, a meaningful title that states the finding rather than the axis. Put it in the README.

**6. Published dataset.** Package the gold layer (DuckDB file + Parquet exports) and publish it as a versioned release. Kaggle accepts datasets. HuggingFace Datasets Hub accepts them. Academics and journalists use them. Every download is a signal that the work has independent value.

**7. A testimonial or reference.** Ask an Irish journalist or civic-tech person to look at the project. Irish Times data journalism, Noteworthy, thejournal.ie's FactCheck team — a single "this is useful, we used it for X" from a credible outside party is worth more than any number of stars.

The things that are *less* impactful for portfolio even though they feel productive:

- More rows of test coverage (matters for code quality, not for portfolio first impression)
- More endpoints ingested (diminishing returns — three well-cleaned sources beat eight dirty ones)
- More refactoring (readers don't grade code style; they grade whether the thing works)
- A longer README (shorter with better figures beats longer with more prose)

Pick 2–3 of the high-impact items. Don't do all seven. Depth beats breadth for portfolio presentation every time.

---

## 16. Action Items Checklist

Estimates are rough. Items are grouped by category, not by priority — prioritise across categories based on what you want to be working on.

### Critical fixes (do these first)

| Item | Effort |
|---|---|
| `lobbying_processing.py` `df` → `lobbying_df` at line 86 | 5 min |
| Double-join fix on `most_lobbied_politician` line 119 | 10 min |
| `endpoint_checker` return inside loop | 10 min |
| Unicode encoding for fada-URL TDs | 30 min |
| Replace hardcoded Windows paths with `config.py` | 2 hours |
| `member_api_request()` double-call | 10 min |

### Lobbying pipeline (the differentiator)

| Item | Effort |
|---|---|
| Banned names list + position filtering for DPO data | 2 hours |
| Canonical-name mapping for known TD variants | 1 hour |
| Join lobbying DPO data onto `enriched_td_attendance.csv` | 1 hour |
| Period-over-period "biggest movers" analysis | 1 hour |
| Shared lobbyists between TD pairs (self-join) | 30 min |
| Degree centrality per TD | 30 min |
| Lobbying method breakdown (email/meeting/phone) | 30 min |
| Keyword extraction from subject_matter / intended_results | 2 hours |
| Conflict-of-interest detection (lobbying × interests) | 6 hours |

### Data enrichment

| Item | Effort |
|---|---|
| Add `date_start` to questions URL + pagination loop | 1 hour |
| Add `bill_status` filter to legislation URL | 10 min |
| Add divisions (votes) endpoint | 4 hours |
| Add debates (speeches) endpoint | 4 hours |
| `/v1/houses` for dynamic term dates | 30 min |
| `/v1/constituencies`, `/v1/parties` as reference tables | 1 hour |
| Historical attendance PDFs (2022, 2023) | 1 hour |
| CRO company lookup for lobby orgs | 6 hours |
| CSO constituency demographics | 3 hours |
| Seanad expansion | 3 hours |

### Architecture & infrastructure

| Item | Effort |
|---|---|
| Wire `config.py` into all scripts | 2 hours |
| Create `pipeline.py` orchestrator with subcommands + `--demo` | 3 hours |
| Medallion directory restructure | 4 hours |
| DuckDB gold layer setup | 3 hours |
| Parquet outputs alongside CSV | 30 min |
| `Makefile` with install / run / test targets | 1 hour |
| `gold/metadata.json` after each pipeline run | 30 min |

### Testing

| Item | Effort |
|---|---|
| `conftest.py` with shared fixtures | 30 min |
| `test_attendance.py` (TD count, max totals, Irish names) | 1 hour |
| `test_lobbying.py` (DPO counts, banned names) | 1 hour |
| `test_normalise.py` (fuzzy key edge cases) | 1 hour |
| `test_enrich.py` (join completeness, no duplicates) | 1 hour |

### Dashboard

| Item | Effort |
|---|---|
| Streamlit MVP (TD search + attendance charts) | 4 hours |
| Lobbying explorer page | 4 hours |
| Rankings page (most lobbied, biggest movers) | 2 hours |
| Network page (shared lobbyists graph) | 8 hours |
| Conflict-of-interest page | 12+ hours |

### Modelling & interview prep

| Item | Effort |
|---|---|
| Restructure silver into star schema in DuckDB | 4 hours |
| Committee UNPIVOT + Work Surface metric | 2 hours |
| SCD Type 2 on party membership | 2 hours |
| Bill stage → `fact_bill_event` table | 2 hours |

### Portfolio polish

| Item | Effort |
|---|---|
| Deploy Streamlit to Community Cloud | 1 hour |
| `FINDINGS.md` with 3–4 headline insights + charts | 4 hours |
| Blog post narrative | 4 hours |
| One headline visualisation (choropleth / chord / heatmap) | 3 hours |
| Publish dataset release (DuckDB + Parquet) | 2 hours |
| Polished README with badges and architecture diagram | 2 hours |
| GitHub Actions CI/CD (weekly pipeline + pytest) | 2 hours |
| Wikidata SPARQL for TD photos/socials | 2 hours |