# Analytical SQL view ideas for gold layer (votes and lobbying)

## Accurate Dáil division vote counts
- Always group by (debate_title, vote_id, date) to avoid cross-division contamination (vote_id is not globally unique)
- Example query for a specific division:

```sql
SELECT vote_type, COUNT(*) AS n
FROM votes
WHERE debate_title = 'Triple Lock Mechanism and Irish Neutrality: Motion (Resumed) [Private Members]'
    AND vote_id = 'vote_33'
    AND date = '2025-03-26'
GROUP BY vote_type;
```


## Analytical views to consider
- Closest vote margins (smallest |Yes-No| per division)
- Most frequent rebels (TDs voting against party/outcome)
- Party cohesion by topic/theme
- Yearly trends in division frequency and turnout
- Notable debates: filter by keywords (carbon, housing, confidence, etc.)
- Participation rates by member, party, and year
- Multi-division debates: aggregate all divisions for a debate_title in a year
- Votes on confidence, carbon, property, taxation, abortion, etc. (curated themes)
- SQL window functions for running totals, party breakdowns, and member streaks
- Cross-reference lobbying and voting (e.g., did lobbied TDs vote differently?)

### Fewest committee memberships excluding current ministers
-- Analytical query idea: Exclude TDs who are currently serving as ministers (office end date is null or in the future) from the 'fewest committee memberships' ranking. This can be implemented in SQL by joining the committee activity table with the office holders table and filtering out current ministers.

See also: DuckDB SQL patterns for deduplication, aggregation, and windowing above.
# Lobbying Pipeline — SQL Learning Guide

Port the Polars pipeline to DuckDB SQL yourself. This guide gives you hints, patterns,
and reference material for each operation — not the answers.

---

## Environment setup

### DuckDB in VSCode

Install the [DuckDB extension for VSCode](https://marketplace.visualstudio.com/items?itemName=Evidence.duckdb-power-user)
(publisher: Evidence). It gives you an inline query runner, schema explorer, and result pane.

Alternatively, open a terminal and run the DuckDB CLI:

```bash
pip install duckdb
python -c "import duckdb; duckdb.sql('SELECT 42').show()"
# or the standalone CLI if you have it:
duckdb data/gold/lobbyist.duckdb
```

Inside the DuckDB CLI, useful meta-commands:
```
.tables               -- list all tables
.schema <table>       -- show CREATE TABLE for a table
.mode column          -- human-readable column output
.timer on             -- show query execution time
```

### Loading your silver CSVs into DuckDB

DuckDB can query CSVs directly without importing them, which is great for exploration:

```sql
-- Query a CSV directly (no import needed)
SELECT * FROM read_csv_auto('lobbyist/output/returns_master.csv') LIMIT 10;
```

When you're ready to build permanent tables:

```sql
CREATE OR REPLACE TABLE returns_master AS
SELECT * FROM read_csv_auto('lobbyist/output/returns_master.csv');
```

DuckDB infers types automatically. Check what it guessed with `.schema returns_master`.

---

## Concept index

| Polars operation | SQL concept | Section |
|---|---|---|
| `pl.concat(frames, how='diagonal')` | `UNION ALL` | [Stacking CSVs](#1-stacking-csvs--union-all) |
| `.unique(subset=[...])` | `SELECT DISTINCT` / dedup CTE | [Deduplication](#2-deduplication) |
| `.group_by().agg()` | `GROUP BY` + aggregate functions | [Aggregation](#3-aggregation) |
| `.filter()` | `WHERE` / `HAVING` | [Filtering](#4-filtering) |
| `.sort()` | `ORDER BY` | [Sorting](#5-sorting) |
| `.join()` | `JOIN` | [Joins](#6-joins) |
| `.with_columns()` | `SELECT ..., expr AS alias` | [Derived columns](#7-derived-columns) |
| `pl.format(template, ...)` | `FORMAT()` / `||` / `CONCAT()` | [String building](#8-string-building) |
| `str.split("::").list.len()` | `LEN(str) - LEN(REPLACE(str,'::',''))` / `array_length` | [String splitting & counting](#9-string-splitting) |
| `.explode()` | `UNNEST()` / `STRING_SPLIT()` | [Exploding arrays](#10-exploding-rows) |
| `dt.year()`, `dt.quarter()` | `EXTRACT(YEAR FROM ...)`, `EXTRACT(QUARTER FROM ...)` | [Date arithmetic](#11-date-arithmetic) |
| `dt.total_days()` between dates | `DATEDIFF('day', start, end)` | [Date arithmetic](#11-date-arithmetic) |
| `str.to_datetime(format=...)` | `STRPTIME(col, format)` | [Date parsing](#12-date-parsing) |
| `fill_null("")` | `COALESCE(col, '')` | [Null handling](#13-null-handling) |
| `str.len_chars()` | `LENGTH(col)` | [String metrics](#14-string-metrics) |
| `pl.struct(...).n_unique()` | `COUNT(DISTINCT ...)` on a composite | [Counting composite keys](#15-counting-composite-keys) |
| Chained `.with_columns()` | CTE chain (`WITH a AS ..., b AS ...`) | [CTEs as pipeline stages](#16-ctes-as-pipeline-stages) |
| `.select(cols)` after join | `SELECT col1, col2` after `JOIN` | — |
| Window / rank | `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` | [Window functions](#17-window-functions) |

---

## 1. Stacking CSVs — UNION ALL

**Polars:** `pl.concat(frames, how='diagonal')` stacks frames with potentially different schemas;
missing columns are filled with `null`.

**SQL hint:** SQL's equivalent is `UNION ALL`. If schemas differ between files, you need to
explicitly `SELECT NULL AS missing_col` in branches that lack it, or use DuckDB's
`read_csv_auto` glob pattern which handles schema merging for you.

```sql
-- DuckDB can glob multiple CSVs in one shot — try this:
SELECT * FROM read_csv_auto('lobbyist/raw/Lobbying*.csv', union_by_name=true);
```

`union_by_name=true` is the DuckDB equivalent of `how='diagonal'` — it matches columns
by name rather than position, filling nulls for absent columns.

**Good practice:** Always add a `source_file` column when you union so you can trace
which file a row came from.

---

## 2. Deduplication

**Polars:** `lobbying_df.unique(subset=["primary_key"], keep="first")`

**SQL hint:** There is no direct `DISTINCT ON (primary_key)` in standard SQL for "keep first".
The pattern is a window function: assign `ROW_NUMBER()` over a partition on the key,
then filter to `rn = 1`. The `ORDER BY` inside the window determines what "first" means.

```sql
-- Skeleton — fill in the ORDER BY to decide which row to keep:
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY primary_key ORDER BY ???) AS rn
    FROM returns_raw
)
SELECT * EXCLUDE rn FROM ranked WHERE rn = 1;
```

`EXCLUDE` is DuckDB-specific syntax for "all columns except these" — handy for dropping
the helper `rn` column without listing everything.

**Variant:** If you truly don't care which row survives, `SELECT DISTINCT *` works but only
when all columns are identical. For the dedup-on-subset case, the `ROW_NUMBER` CTE is
always correct.

---

## 3. Aggregation

**Polars:**
```python
df.group_by(["full_name", "chamber"]).agg(
    pl.col("primary_key").n_unique().alias("lobby_returns_targeting"),
    pl.col("lobbyist_name").n_unique().alias("distinct_orgs"),
)
```

**SQL equivalents:**

| Polars | SQL |
|---|---|
| `pl.len()` | `COUNT(*)` |
| `pl.col("x").count()` | `COUNT(x)` (excludes nulls) |
| `pl.col("x").n_unique()` | `COUNT(DISTINCT x)` |
| `pl.col("x").sum()` | `SUM(x)` |
| `pl.col("x").mean()` | `AVG(x)` |
| `pl.col("x").median()` | `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x)` |
| `pl.col("x").min()` | `MIN(x)` |
| `pl.col("x").max()` | `MAX(x)` |
| `pl.col("x").first()` | `FIRST(x)` or `ANY_VALUE(x)` (DuckDB supports both) |

**Good practice:** When you have both a grouped aggregate and a total that you join back
(like `compute_most_lobbied_politicians` does with `segmented` + `total`), the SQL equivalent
is either two CTEs joined together, or a single query with window functions alongside GROUP BY.
Both are idiomatic — try both and see which reads cleaner.

---

## 4. Filtering

**Polars:** `.filter(pl.col("x") == "Yes")` / `.filter(pl.col("x").is_not_null())`

**SQL equivalents:**

| Polars | SQL |
|---|---|
| `.filter(condition)` | `WHERE condition` |
| `.is_null()` | `IS NULL` |
| `.is_not_null()` | `IS NOT NULL` |
| `.is_in(list)` | `IN (...)` |
| `~condition` (not) | `NOT condition` |
| Combined with `&` | `AND` |
| Combined with `\|` | `OR` |

Post-aggregation filters use `HAVING` not `WHERE`. `WHERE` runs before GROUP BY;
`HAVING` runs after. `filter(pl.col("count") > 1)` on an aggregated frame becomes
`HAVING COUNT(...) > 1`.

---

## 5. Sorting

**Polars:** `.sort(["col_a", "col_b"], descending=[True, False])`

**SQL:** `ORDER BY col_a DESC, col_b ASC`

Null handling: in DuckDB, nulls sort last by default in `ASC`, first in `DESC`.
You can override with `NULLS FIRST` / `NULLS LAST`:
```sql
ORDER BY total_returns DESC NULLS LAST
```

---

## 6. Joins

**Polars join types → SQL:**

| Polars `how=` | SQL |
|---|---|
| `"inner"` | `INNER JOIN` (default) |
| `"left"` | `LEFT JOIN` |
| `"outer"` | `FULL OUTER JOIN` |
| `"cross"` | `CROSS JOIN` |
| `"semi"` | `WHERE EXISTS (subquery)` or `JOIN ... SELECT DISTINCT` |
| `"anti"` | `WHERE NOT EXISTS (subquery)` |

**Good practice:** Always be explicit about what can go null after a LEFT JOIN.
Any column from the right side can be null if there's no match — apply `COALESCE`
where you need a fallback value.

---

## 7. Derived columns

**Polars:** `.with_columns(pl.col("x").dt.year().alias("year"))`

**SQL:** `SELECT *, EXTRACT(YEAR FROM x) AS year FROM ...`

In DuckDB, `SELECT * REPLACE (expr AS col)` lets you swap out a column's value
without listing all other columns:

```sql
SELECT * REPLACE (COALESCE(lobby_url, 'https://...') AS lobby_url)
FROM returns_master;
```

---

## 8. String building

**Polars:** `pl.format("https://www.lobbying.ie/return/{}", pl.col("primary_key"))`

**SQL options:**
```sql
-- ANSI standard (works everywhere):
'https://www.lobbying.ie/return/' || primary_key

-- DuckDB FORMAT (like Python f-strings):
FORMAT('https://www.lobbying.ie/return/{}', primary_key)

-- CONCAT (null-propagating — use CONCAT_WS to ignore nulls):
CONCAT('https://www.lobbying.ie/return/', primary_key)
```

`CONCAT_WS(separator, col1, col2, ...)` skips nulls, which is useful when building
` · ` delimited policy area lists.

---

## 9. String splitting

**Polars:** `pl.col("dpo_lobbied").str.split("::").list.len()`

**SQL hint:** SQL doesn't have a built-in "count occurrences of substring" function.
The classic trick is:

```sql
-- Count occurrences of '::' in a string:
(LENGTH(dpo_lobbied) - LENGTH(REPLACE(dpo_lobbied, '::', ''))) / LENGTH('::') + 1
-- That gives the number of elements when split by '::'
```

DuckDB also has `STRING_SPLIT(str, delim)` which returns a `VARCHAR[]` array,
and `LEN(STRING_SPLIT(...))` gives the count directly.

---

## 10. Exploding rows

This is the heart of the pipeline — turning `::` delimited DPO strings into one row
per politician.

**Polars chain:**
1. `str.split("::")` → list column
2. `.explode()` → one row per element
3. `str.split("|")` → sub-split each element
4. `list.get(0/1/2)` → extract named fields

**SQL equivalent in DuckDB:**
DuckDB has `UNNEST()` which is the direct equivalent of `.explode()`:

```sql
-- Skeleton for the politicians explode:
SELECT
    primary_key,
    lobbyist_name,
    -- split on '::' first, then unnest:
    UNNEST(STRING_SPLIT(dpo_lobbied, '::')) AS dpo_raw
FROM returns_raw;
```

To then split each `dpo_raw` on `|` and extract fields, nest another `STRING_SPLIT`:

```sql
WITH exploded AS (
    SELECT primary_key, UNNEST(STRING_SPLIT(dpo_lobbied, '::')) AS dpo_raw
    FROM returns_raw
)
SELECT
    primary_key,
    STRING_SPLIT(dpo_raw, '|')[1] AS full_name,   -- DuckDB arrays are 1-indexed
    STRING_SPLIT(dpo_raw, '|')[2] AS position,
    STRING_SPLIT(dpo_raw, '|')[3] AS chamber
FROM exploded;
```

**Watch out:** DuckDB array indexing is 1-based (not 0-based like Polars `list.get(0)`).

---

## 11. Date arithmetic

**Polars → SQL:**

| Polars | DuckDB SQL |
|---|---|
| `.dt.year()` | `EXTRACT(YEAR FROM col)` or `YEAR(col)` |
| `.dt.quarter()` | `EXTRACT(QUARTER FROM col)` |
| `.dt.month()` | `EXTRACT(MONTH FROM col)` |
| `(date_a - date_b).dt.total_days()` | `DATEDIFF('day', date_b, date_a)` |

DuckDB also supports interval arithmetic directly:
```sql
-- Days between two dates:
(published_date - period_end_date)  -- returns an INTERVAL in DuckDB

-- Cast to integer days:
DATEDIFF('day', period_end_date, published_date)
```

**Year-quarter label** (equivalent to `pl.format("{}-Q{}", year, quarter)`):
```sql
CONCAT(EXTRACT(YEAR FROM lobbying_period_start_date), '-Q',
       EXTRACT(QUARTER FROM lobbying_period_start_date)) AS year_quarter
```

---

## 12. Date parsing

**Polars:** `pl.col("date_published_timestamp").str.to_datetime(format="%d/%m/%Y %H:%M")`

**DuckDB:** `STRPTIME(col, '%d/%m/%Y %H:%M')` — same `strftime` format codes.

For the `lobbying_period` string (`"01 Jan, 2023 to 30 Jun, 2023"`):
- Polars first splits on `" to "`, then parses each half with `"%e %b, %Y"`
- In SQL: `SPLIT_PART(lobbying_period, ' to ', 1)` gives the start string,
  then `STRPTIME(that_string, '%-d %b, %Y')` parses it.

`%e` in strftime is "space-padded day"; use `%-d` for the DuckDB-compatible version.

---

## 13. Null handling

| Polars | SQL |
|---|---|
| `.fill_null("")` | `COALESCE(col, '')` |
| `.fill_null(0)` | `COALESCE(col, 0)` |
| `.drop_nulls(subset=["x"])` | `WHERE x IS NOT NULL` |
| `.is_null()` | `col IS NULL` |

`COALESCE` takes multiple arguments and returns the first non-null:
```sql
COALESCE(lobby_url, 'https://www.lobbying.ie/return/' || primary_key)
```

---

## 14. String metrics

**Polars:** `pl.col("specific_details").fill_null("").str.len_chars()`

**SQL:** `LENGTH(COALESCE(specific_details, ''))`

DuckDB also has `LEN()` as an alias. Both return character count (not byte count),
which matters for Irish text with accented characters.

---

## 15. Counting composite keys

**Polars:**
```python
pl.struct(
    pl.col("lobbying_period_start_date").dt.year().alias("year"),
    pl.col("lobbying_period_start_date").dt.quarter().alias("quarter"),
).n_unique().alias("distinct_periods_filed")
```

This counts distinct (year, quarter) combinations.

**SQL hint:** `COUNT(DISTINCT col)` only works on a single column. For composite
distinctness you have two options:

Option A — concatenate into a string key (simple, readable):
```sql
COUNT(DISTINCT CONCAT(EXTRACT(YEAR FROM lobbying_period_start_date), '-',
                      EXTRACT(QUARTER FROM lobbying_period_start_date)))
```

Option B — use a subquery to count distinct row combinations (more principled):
```sql
(SELECT COUNT(*) FROM (
    SELECT DISTINCT EXTRACT(YEAR FROM lobbying_period_start_date),
                    EXTRACT(QUARTER FROM lobbying_period_start_date)
    FROM t WHERE t.lobbyist_name = outer.lobbyist_name
))
```

Option A is fine for this use case. Option B shows up in interview questions.

---

## 16. CTEs as pipeline stages

Every Polars chain of `.with_columns()` calls is naturally expressed as a CTE chain in SQL.
Think of each CTE as one transformation stage:

```sql
WITH
-- Stage 1: parse dates
parsed AS (
    SELECT *,
        STRPTIME(lobbying_period_start_date_raw, '%d %b, %Y') AS lobbying_period_start_date,
        STRPTIME(lobbying_period_end_date_raw, '%d %b, %Y') AS lobbying_period_end_date
    FROM returns_raw
),

-- Stage 2: attach URLs
with_urls AS (
    SELECT p.*, COALESCE(u.lobby_url, FORMAT('https://www.lobbying.ie/return/{}', p.primary_key)) AS lobby_url
    FROM parsed p
    LEFT JOIN url_lookup u ON p.primary_key = u.primary_key
),

-- Stage 3: compute a derived metric
enriched AS (
    SELECT *,
        DATEDIFF('day', lobbying_period_end_date, published_at) AS days_to_publish
    FROM with_urls
)

SELECT * FROM enriched WHERE days_to_publish >= 0;
```

**Good practice:**
- Name CTEs after what they *are*, not what they *do* (`deduplicated`, not `after_dedup`).
- Keep each CTE to one transformation concern.
- Prefer CTEs over subqueries for anything you reference more than once.

---

## 17. Window functions

Window functions let you compute aggregates alongside individual rows without collapsing
the result — useful wherever Polars does a group_by + join back.

**Pattern: rank within a group**
```sql
ROW_NUMBER() OVER (PARTITION BY lobbyist_name ORDER BY lobbying_period_start_date DESC) AS rn
```

**Pattern: running total**
```sql
SUM(return_count) OVER (PARTITION BY lobbyist_name ORDER BY year_quarter) AS running_total
```

**Pattern: total per group alongside detail**
```sql
COUNT(*) OVER (PARTITION BY lobbyist_name) AS total_returns_for_lobbyist
```

This last pattern replaces the Polars `group_by total → join back` pattern in
`compute_most_lobbied_politicians`.

---

## 18. Useful DuckDB-specific syntax

Things DuckDB supports that standard SQL doesn't:

```sql
-- Exclude columns without listing everything:
SELECT * EXCLUDE (rn, internal_id) FROM t;

-- Replace a column's value:
SELECT * REPLACE (UPPER(full_name) AS full_name) FROM t;

-- Pivot (cross-tab):
PIVOT t ON year USING SUM(return_count) GROUP BY lobbyist_name;

-- Sampling for quick exploration:
SELECT * FROM t USING SAMPLE 1000;

-- Show query plan (understand performance):
EXPLAIN SELECT * FROM t WHERE ...;

-- Regex matching:
WHERE lobbyist_name SIMILAR TO 'Google.*|Meta.*'
-- or:
WHERE REGEXP_MATCHES(lobbyist_name, 'Google|Meta')
```

---

## 19. Porting each Polars function — suggested order

Port these in order. Each one introduces a new concept:

1. **`compute_quarterly_trend`** — GROUP BY + date extraction + FORMAT. Simplest aggregate.
2. **`compute_policy_area_breakdown`** — GROUP BY with COUNT DISTINCT. One-table aggregate.
3. **`compute_grassroots_campaigns`** — adds a WHERE filter before GROUP BY.
4. **`compute_lobbyist_persistence`** — introduces composite distinct count and DATEDIFF.
5. **`compute_most_prolific_lobbyists`** — adds a JOIN against the org reference table.
6. **`compute_most_lobbied_politicians`** — two aggregations joined together; try as two CTEs.
7. **`compute_politician_policy_exposure`** — GROUP BY on three columns with dedup.
8. **`compute_bilateral_relationships`** — FILTER after GROUP BY (HAVING) + composite dedup.
9. **`compute_top_client_companies`** — many n_unique columns, good COUNT DISTINCT practice.
10. **`compute_revolving_door_dpos`** — three-column GROUP BY, introduces the revolving door concept.
11. **`build_returns_master`** — SELECT with DISTINCT, ORDER BY. Row-level, no aggregation.
12. **`build_bilateral_returns_detail`** — a CTE to compute pair counts, then join back to detail.
13. **`explode_politicians`** + **`explode_activities`** — the hard one. STRING_SPLIT + UNNEST.

---

## 20. Vote history queries

The vote history table (`current_dail_vote_history.csv`) is a good DuckDB playground because
it has a clear grain (one row = one member × one division) and several interesting derived metrics.

### Load it

```sql
CREATE OR REPLACE TABLE votes AS
SELECT * FROM read_csv_auto('data/gold/current_dail_vote_history.csv');
```

### Participation rate per member

How many divisions did each TD actually vote in, out of all divisions held?

```sql
WITH division_count AS (
    SELECT COUNT(DISTINCT vote_id) AS total_divisions FROM votes
),
member_votes AS (
    SELECT full_name, COUNT(DISTINCT vote_id) AS divisions_voted
    FROM votes
    GROUP BY full_name
)
SELECT
    m.full_name,
    m.divisions_voted,
    d.total_divisions,
    ROUND(m.divisions_voted * 100.0 / d.total_divisions, 1) AS participation_pct
FROM member_votes m
CROSS JOIN division_count d
ORDER BY participation_pct DESC;
```

`CROSS JOIN` with a single-row CTE is the idiomatic way to attach a scalar total to every row.

---

### Rebellion rate per member

A "rebellion" is voting against the outcome — Voted Yes on a Lost division, or Voted No on a Carried one.

```sql
SELECT
    full_name,
    COUNT(*) FILTER (WHERE vote_type IN ('Voted Yes', 'Voted No')) AS eligible_votes,
    COUNT(*) FILTER (WHERE
        (vote_type = 'Voted Yes' AND vote_outcome = 'Lost') OR
        (vote_type = 'Voted No'  AND vote_outcome = 'Carried')
    ) AS rebellions,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE
            (vote_type = 'Voted Yes' AND vote_outcome = 'Lost') OR
            (vote_type = 'Voted No'  AND vote_outcome = 'Carried')
        ) / NULLIF(COUNT(*) FILTER (WHERE vote_type IN ('Voted Yes', 'Voted No')), 0),
    1) AS rebellion_pct
FROM votes
GROUP BY full_name
HAVING eligible_votes >= 10
ORDER BY rebellion_pct DESC;
```

`FILTER (WHERE ...)` is DuckDB's way of doing conditional aggregation — cleaner than `SUM(CASE WHEN ...)`.
`NULLIF(x, 0)` prevents division-by-zero; it returns NULL instead of crashing when x = 0.

---

### Most contested divisions

Smallest margin between Yes and No counts.

```sql
SELECT
    vote_id,
    debate_title,
    date,
    vote_outcome,
    COUNT(*) FILTER (WHERE vote_type = 'Voted Yes') AS yes_votes,
    COUNT(*) FILTER (WHERE vote_type = 'Voted No')  AS no_votes,
    ABS(
        COUNT(*) FILTER (WHERE vote_type = 'Voted Yes') -
        COUNT(*) FILTER (WHERE vote_type = 'Voted No')
    ) AS margin
FROM votes
GROUP BY vote_id, debate_title, date, vote_outcome
ORDER BY margin ASC
LIMIT 20;
```

---

### Vote type breakdown over time

How has Yes/No/Abstain split changed year by year?

```sql
SELECT
    EXTRACT(YEAR FROM date::DATE) AS year,
    vote_type,
    COUNT(*) AS n,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY EXTRACT(YEAR FROM date::DATE)), 1) AS pct
FROM votes
GROUP BY year, vote_type
ORDER BY year, vote_type;
```

This uses a **window function alongside GROUP BY**: `SUM(COUNT(*)) OVER (PARTITION BY year)` gives
the per-year total without a second CTE. The outer `COUNT(*)` is the per-group count; the window
aggregates those counts back up by year.

---

### TDs who voted against their own party

Party is now in the gold layer (`current_dail_vote_history.csv`), so this query
is ready to run. The logic: for each division, find the majority vote direction
within each party, then flag any member who voted differently.

```sql
CREATE OR REPLACE TABLE votes AS
SELECT * FROM read_csv_auto('data/gold/current_dail_vote_history.csv');

-- Step 1: find the majority vote direction per party per division
WITH party_majority AS (
    SELECT
        vote_id,
        party,
        vote_type,
        COUNT(*) AS n,
        ROW_NUMBER() OVER (
            PARTITION BY vote_id, party
            ORDER BY COUNT(*) DESC
        ) AS rn
    FROM votes
    WHERE vote_type IN ('Voted Yes', 'Voted No')
      AND party IS NOT NULL
    GROUP BY vote_id, party, vote_type
),
-- the top-ranked vote_type per (division, party) is the party line
party_line AS (
    SELECT vote_id, party, vote_type AS party_direction
    FROM party_majority
    WHERE rn = 1
),
-- Step 2: join back to individual votes and flag deviations
flagged AS (
    SELECT
        v.full_name,
        v.party,
        v.constituency_name,
        v.vote_id,
        v.debate_title,
        v.date,
        v.vote_type,
        pl.party_direction,
        v.vote_type != pl.party_direction AS crossed_party_line
    FROM votes v
    JOIN party_line pl
        ON v.vote_id = pl.vote_id
       AND v.party   = pl.party
    WHERE v.vote_type IN ('Voted Yes', 'Voted No')
)
-- Step 3: summarise by member
SELECT
    full_name,
    party,
    constituency_name,
    COUNT(*)                                        AS eligible_votes,
    SUM(crossed_party_line::INT)                    AS party_line_breaks,
    ROUND(100.0 * SUM(crossed_party_line::INT)
          / COUNT(*), 1)                            AS break_rate_pct
FROM flagged
GROUP BY full_name, party, constituency_name
HAVING eligible_votes >= 10
ORDER BY break_rate_pct DESC;
```

**Concepts used here:**
- `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY COUNT(*) DESC)` — rank within a group to pick the majority row
- Two-CTE pipeline: `party_majority` → `party_line` → join back to detail
- Boolean cast `::INT` to sum a true/false flag as 0/1
- `HAVING` to exclude members with too few votes for a meaningful rate

---

---

## 22. Streamlit app logic — candidate SQL views

This section maps the business logic currently running inside the Streamlit pages to the SQL
views that should eventually replace it. The goal at maturity is that each page loads a
pre-aggregated CSV or Parquet file rather than doing heavy computation at render time.

Each view listed here is a learning exercise. The pattern column points to the relevant
section earlier in this guide. Skeletons are intentionally incomplete — fill them in yourself.

---

### Payments page

**`v_td_payment_totals`** — all-time total paid and payment count per TD

The page currently computes this at render time from the full unfiltered dataframe.
It should be a permanent view so any page can reference it without re-reading the CSV.

Pattern: §3 Aggregation — simple `GROUP BY td_name` with `SUM` and `COUNT`.

```sql
-- Skeleton:
CREATE OR REPLACE VIEW v_td_payment_totals AS
SELECT
    ???            AS td_name,
    SUM(???)       AS total_paid_alltime,
    COUNT(*)       AS payment_count
FROM payments
GROUP BY ???;
```

Hint: the `Amount` column arrives as a string like `€2,445.83` — you need to clean it before
summing. Look at DuckDB's `REPLACE()` and `TRY_CAST()` functions.

---

**`v_td_payment_annual`** — per-TD per-year totals (the "selected period" column)

Pattern: §3 Aggregation + §11 Date arithmetic — add `EXTRACT(YEAR FROM date_paid)` to the
GROUP BY.

---

### Lobbying pages

**`v_most_lobbied_politicians`** — per-politician: total returns filed against them, distinct
lobbying organisations, distinct policy areas

The `_overview()` function computes this by grouping `politician_returns_detail.csv` every
render. It involves two `COUNT(DISTINCT ...)` operations alongside `COUNT(*)`.

Pattern: §3 Aggregation — see the `COUNT DISTINCT` row in the table.

```sql
-- Skeleton:
CREATE OR REPLACE VIEW v_most_lobbied_politicians AS
SELECT
    full_name,
    COUNT(DISTINCT ???)            AS total_returns,
    COUNT(DISTINCT lobbyist_name)  AS distinct_orgs,
    COUNT(DISTINCT ???)            AS distinct_policy_areas
FROM politician_returns_detail
GROUP BY full_name
ORDER BY total_returns DESC;
```

---

**`v_lobbyist_summary`** — per-organisation: return count, distinct politicians targeted,
distinct policy areas, first filing date, last filing date

The `_lobbyist_profile()` and `_overview()` functions both derive parts of this separately.
A single view would serve both.

Pattern: §3 Aggregation + §11 Date arithmetic (`MIN`, `MAX` on the date column).

Bonus: add `DATEDIFF('day', MIN(...), MAX(...)) AS active_days` to measure how long the org
has been filing. That's a derived column from two aggregates — you can't do it in a plain
`GROUP BY` without a CTE.

---

**`v_transparency_scorecard`** — per-organisation: median days to publish, max days,
returns filed, average description length

Currently built in `_transparency()` by loading two CSVs (`experimental_time_to_publish.csv`
and `experimental_return_description_lengths.csv`) and joining them with pandas. This is a
classic two-CTE join.

Pattern: §16 CTEs as pipeline stages + §6 Joins + §14 String metrics.

```sql
-- Skeleton:
WITH filing_latency AS (
    SELECT
        lobbyist_name,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_to_publish) AS median_days,
        MAX(???)   AS max_days,
        COUNT(*)   AS returns_filed
    FROM ???
    GROUP BY lobbyist_name
),
desc_quality AS (
    SELECT
        lobbyist_name,
        AVG(LENGTH(COALESCE(specific_details, '')) + LENGTH(COALESCE(intended_results, ''))) AS avg_desc_len
    FROM returns_master
    GROUP BY lobbyist_name
)
CREATE OR REPLACE VIEW v_transparency_scorecard AS
SELECT
    l.lobbyist_name,
    l.median_days,
    l.max_days,
    l.returns_filed,
    d.avg_desc_len
FROM filing_latency l
??? JOIN desc_quality d ON l.lobbyist_name = d.lobbyist_name
ORDER BY l.median_days DESC NULLS LAST;
```

What JOIN type is correct here, and why? Think about what happens to organisations that have
returns but no description data, or vice versa.

---

**`v_revolving_door_summary`** — per-DPO name: returns involved in, distinct lobbying firms,
distinct policy areas, distinct politicians targeted

The `_revolving_door()` function loads `experimental_revolving_door_dpos.csv` which is already
pre-aggregated by the pipeline. When you port this, skip the pre-aggregated file and build
the view directly from `revolving_door_returns_detail.csv`.

Pattern: §3 Aggregation — four `COUNT(DISTINCT ...)` columns grouped by the person's name.

---

**`v_bilateral_relationships`** — (organisation, politician) pairs that appear across multiple
distinct filing periods — the "persistent lobbying" signal

Currently computed in the pipeline as `experimental_bilateral_relationships.csv`. At maturity
this becomes a view: group by `(lobbyist_name, full_name)`, count distinct periods, filter
with `HAVING` to pairs that appear more than once.

Pattern: §4 Filtering — specifically the `HAVING` vs `WHERE` distinction. This is the canonical
example of why `HAVING` exists: you can only apply the `> 1` filter *after* the aggregation.

---

### Interests page

**`v_interests_landing_stats`** — per-chamber summary: TD count, landlord count, minister count,
years covered

The `_render_landing()` function computes four separate aggregations on the interests dataframe
every render. All four can be a single GROUP BY with conditional aggregation.

Pattern: §3 Aggregation — use `COUNT(DISTINCT CASE WHEN is_landlord THEN full_name END)` for
the conditional counts. This is the SQL equivalent of `groupby().any()`.

```sql
-- Skeleton:
CREATE OR REPLACE VIEW v_interests_landing_stats AS
SELECT
    chamber,
    COUNT(DISTINCT full_name)                                                   AS td_count,
    COUNT(DISTINCT CASE WHEN is_landlord = true THEN full_name END)             AS landlord_count,
    COUNT(DISTINCT CASE WHEN ministerial_office_filled = true THEN full_name END) AS minister_count,
    MIN(year_declared)                                                          AS year_from,
    MAX(year_declared)                                                          AS year_to
FROM member_interests
GROUP BY chamber;
```

---

**`v_td_interest_profile`** — all declared interests for one TD, ordered by year

This is a straightforward parameterised filter. In DuckDB you can't parameterise a view,
but you can write the query as a macro:

```sql
-- DuckDB macro (callable like a function):
CREATE OR REPLACE MACRO td_interests(td) AS TABLE
    SELECT year_declared, category, description, is_landlord, ministerial_office_filled
    FROM member_interests
    WHERE full_name = td
    ORDER BY year_declared DESC;

-- Call it:
FROM td_interests('Mary Lou McDonald');
```

This is a useful DuckDB pattern to know — it replaces the Streamlit `df[df["full_name"] == selected]` filter.

---

### Legislation page

**`v_bill_progress`** — per-bill: max stage reached (1–7), current status, source, sponsor count

The `_stages_view()` function computes `stages_reached` via `groupby().agg(max)`. In SQL
this is a `MAX(stage_no)` per `(bill_no, bill_year)` group, joined back to the bill metadata.

Pattern: §3 Aggregation + §6 Joins — aggregate the stage table, then join to get title/status.

```sql
-- Skeleton:
CREATE OR REPLACE VIEW v_bill_progress AS
SELECT
    s.bill_no,
    s.bill_year,
    ANY_VALUE(s.title)       AS title,
    ANY_VALUE(s.bill_status) AS bill_status,
    ANY_VALUE(s.source)      AS source,
    MAX(s.stage_no)          AS max_stage_reached,
    COUNT(*)                 AS total_stage_rows
FROM stages s
GROUP BY s.bill_no, s.bill_year;
```

`ANY_VALUE` is the right choice here — title and status are the same for all rows of a given bill,
so you're just picking one. Using `MAX` on a string would also work but is misleading.

---

**`v_td_legislative_agenda`** — per-TD: bills sponsored (total), bills as primary sponsor,
bills that passed, bills that lapsed

Join `sponsors` to `v_bill_progress` on `(bill_no, bill_year)`, then group by `td_name`.
Uses conditional aggregation (`COUNT(CASE WHEN ...)`).

Pattern: §3 Aggregation + §6 Joins. This is the first view that requires joining two of
your own views together — a good indicator it belongs in the gold layer.

---

**`v_most_debated_bills`** — per-bill: count of debate sections, first debate date, last
debate date, current bill status

The `_debates_view()` computes this on every page render via `groupby().size()`. Simple
`COUNT(*) GROUP BY (bill_no, bill_year)` with `MIN`/`MAX` dates.

---

### Cross-dataset view (the one to build last)

**`v_td_full_profile`** — one row per TD joining: attendance rate, total PSA received,
declared interest count, times lobbied, bills sponsored

This is the gold layer join key: everything about a TD in one place. The normalised join key
(`normalise_join_key.py`) exists precisely to make this possible across datasets that share
no common identifier.

It will be a multi-table LEFT JOIN — attendance as the spine, everything else joined in.
Every join is a LEFT JOIN because a TD may appear in some datasets but not others.

Pattern: §6 Joins — review the note about nulls after LEFT JOIN. Every metric column in this
view should be wrapped in `COALESCE(metric, 0)` or `COALESCE(metric, '—')` so missing data
doesn't silently drop rows.

```sql
-- Skeleton structure only — fill in the actual column names:
CREATE OR REPLACE VIEW v_td_full_profile AS
SELECT
    a.full_name,
    a.join_key,
    a.attendance_rate,
    COALESCE(p.total_paid_alltime, 0)    AS total_psa_received,
    COALESCE(i.interest_count, 0)        AS declared_interests,
    COALESCE(l.times_lobbied, 0)         AS times_lobbied,
    COALESCE(s.bills_sponsored, 0)       AS bills_sponsored
FROM v_attendance_summary a
LEFT JOIN v_td_payment_totals p   ON a.join_key = p.join_key
LEFT JOIN v_interests_summary i   ON a.join_key = i.join_key
LEFT JOIN v_most_lobbied_politicians l ON a.join_key = l.join_key
LEFT JOIN v_td_legislative_agenda s ON a.join_key = s.join_key;
```

Note that this view depends on four other views — this is why the simpler views need to be
built first. The order in section §19 applies here too: simple aggregations before joins,
joins before multi-view joins.

---

### General notes on when to use a view vs a CSV

| Situation | Prefer |
|---|---|
| Result is always the same (no user filter) | `CREATE VIEW` — computed on demand |
| Result is expensive and rarely changes | `CREATE TABLE AS SELECT` — materialised |
| Result is filtered by user input (TD name, year) | DuckDB macro or parameterised query |
| >100k rows, read repeatedly by the app | Parquet (`COPY ... TO 'file.parquet'`) |
| Shared between Python and SQL tooling | Parquet — both pandas and DuckDB read it natively |

The silver CSVs that are already small and fast (< 50k rows) are fine to keep as CSVs.
The large ones (`returns_master`, `stages`) are candidates for Parquet once the pipeline matures.

---

## 21. Resources

### DuckDB — reference

- [DuckDB SQL introduction](https://duckdb.org/docs/sql/introduction) — start here, covers the basics fast
- [DuckDB string functions](https://duckdb.org/docs/sql/functions/char) — `LENGTH`, `REPLACE`, `STRING_SPLIT`, `REGEXP_MATCHES`, etc.
- [DuckDB date/time functions](https://duckdb.org/docs/sql/functions/timestamp) — `EXTRACT`, `DATEDIFF`, `STRPTIME`, intervals
- [DuckDB window functions](https://duckdb.org/docs/sql/window_functions) — `ROW_NUMBER`, `RANK`, `LAG`, `LEAD`, running totals
- [DuckDB aggregate functions](https://duckdb.org/docs/sql/functions/aggregates) — `PERCENTILE_CONT`, `ANY_VALUE`, `FIRST`, `FILTER`
- [DuckDB `UNNEST` and list functions](https://duckdb.org/docs/sql/query_syntax/unnest) — for the explode patterns in §10
- [DuckDB macros](https://duckdb.org/docs/sql/statements/create_macro) — parameterised queries, the `AS TABLE` form used in §22
- [DuckDB `COPY … TO` (Parquet export)](https://duckdb.org/docs/sql/statements/copy) — for writing silver/gold outputs
- [DuckDB Python API](https://duckdb.org/docs/api/python/overview) — how to use DuckDB from inside the pipeline scripts

### SQL learning — structured courses

- [Select Star SQL](https://selectstarsql.com/) — free, interactive, uses real civic data; best first resource if you want to feel the concepts rather than just read them
- [Mode Analytics SQL tutorial](https://mode.com/sql-tutorial/) — covers basic → advanced in a clear sequence, good for interview prep
- [SQLZoo](https://sqlzoo.net/) — short focused exercises on GROUP BY, JOIN, subqueries; good for drilling individual concepts
- [Advanced SQL for Data Scientists (LinkedIn Learning)](https://www.linkedin.com/learning/advanced-sql-for-data-scientists) — window functions, CTEs, performance; requires subscription
- [The Art of PostgreSQL](https://theartofpostgresql.com/) — book, PostgreSQL-focused but 90% transfers; excellent on CTEs, window functions, and thinking relationally

### SQL concepts — specific topics

- [Use The Index, Luke](https://use-the-index-luke.com/) — free book on SQL performance and indexing; worth reading before you add indexes to DuckDB tables
- [Modern SQL](https://modern-sql.com/) — concise reference for window functions, `FILTER`, `LATERAL`, and other features that most SQL tutorials skip
- [SQL Window Functions explained visually](https://antonz.org/window-functions/) — the clearest visual explanation of PARTITION BY / ORDER BY that exists; bookmark this for when window functions click and un-click
- [Conditional aggregation (COUNT CASE WHEN)](https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-AGGREGATES) — PostgreSQL docs, but the `FILTER (WHERE ...)` syntax is identical in DuckDB

### Data engineering — broader context

- [The Medallion Architecture (Databricks)](https://www.databricks.com/glossary/medallion-architecture) — this project already uses bronze/silver/gold; this is the reference for what that means at scale
- [dbt (data build tool) docs](https://docs.getdbt.com/docs/introduction) — the industry standard for managing SQL views and models in a pipeline; worth knowing even if you don't use it here
- [Pandas vs Polars vs DuckDB (benchmark)](https://duckdblabs.github.io/db-benchmark/) — the official benchmark showing where DuckDB wins; useful context for why the gold layer is in DuckDB

### Irish / civic data context

- [Lobbying.ie developer docs](https://www.lobbying.ie/app/contact-us/api-documentation) — the API behind the data this pipeline processes
- [Oireachtas Open Data portal](https://data.oireachtas.ie/) — all the APIs used in `oireachtas_api_service.py`; the schema docs explain what every field means

---

**Interview-prep focus areas** (most common in data engineering rounds):
- `ROW_NUMBER()` for deduplication and ranking
- `COUNT(DISTINCT ...)` — know when you need it vs `COUNT(*)`
- CTEs vs subqueries — when each is cleaner
- `HAVING` vs `WHERE` — post- vs pre-aggregation filtering
- Self-joins for relationship analysis (bilateral pairs pattern)
- Exploding strings into rows (UNNEST / STRING_SPLIT)
- `FILTER (WHERE ...)` as a cleaner alternative to `SUM(CASE WHEN ...)`
- `COALESCE` and null propagation in LEFT JOINs
