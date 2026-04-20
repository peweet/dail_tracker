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

### Future: join votes → members for party whip analysis

Once you have a `members` table with a party column, a join lets you compute
party cohesion — what fraction of a party's TDs voted the same way on each division:

```sql
-- Sketch — fill in once members table exists
SELECT
    v.vote_id,
    m.party,
    v.vote_type,
    COUNT(*) AS n_members,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY v.vote_id, m.party), 1) AS cohesion_pct
FROM votes v
JOIN members m ON v.unique_member_code = m.member_code
GROUP BY v.vote_id, m.party, v.vote_type
ORDER BY cohesion_pct ASC;
```

---

## 21. Resources

- [DuckDB SQL docs](https://duckdb.org/docs/sql/introduction) — definitive reference
- [DuckDB string functions](https://duckdb.org/docs/sql/functions/char)
- [DuckDB date/time functions](https://duckdb.org/docs/sql/functions/timestamp)
- [DuckDB window functions](https://duckdb.org/docs/sql/window_functions)
- [Mode Analytics SQL tutorial](https://mode.com/sql-tutorial/) — well-structured for interview prep
- [Select Star SQL](https://selectstarsql.com/) — interactive, real civic data (US, but the concepts transfer)
- [SQLZoo](https://sqlzoo.net/) — good for practising GROUP BY and JOIN in isolation

**Interview-prep focus areas** (most common in data engineering rounds):
- `ROW_NUMBER()` for deduplication and ranking
- `COUNT(DISTINCT ...)` — know when you need it vs `COUNT(*)`
- CTEs vs subqueries — when each is cleaner
- `HAVING` vs `WHERE` — post- vs pre-aggregation filtering
- Self-joins for relationship analysis (bilateral pairs pattern)
- Exploding strings into rows (UNNEST / STRING_SPLIT)
