# Analytical SQL — Gold Layer & Streamlit Views

A learning guide for moving analytical business logic out of Python/Streamlit and into
pre-computed SQL views on the DuckDB gold layer. Interview-prep oriented: each section
links to the relevant documentation and asks you to work it out rather than handing
you the answer.

---

## Accurate Dáil division vote counts

Always group by `(debate_title, vote_id, date)` — `vote_id` is not globally unique across debates.

```sql
SELECT vote_type, COUNT(*) AS n
FROM votes
WHERE debate_title = 'Triple Lock Mechanism and Irish Neutrality: Motion (Resumed) [Private Members]'
    AND vote_id = 'vote_33'
    AND date = '2025-03-26'
GROUP BY vote_type;
```

---

## Environment setup

### DuckDB in VSCode

Install the [DuckDB extension for VSCode](https://marketplace.visualstudio.com/items?itemName=Evidence.duckdb-power-user)
(publisher: Evidence). It gives you an inline query runner, schema explorer, and result pane.

Or use the CLI:

```bash
duckdb data/gold/lobbyist.duckdb
```

Inside the CLI:
```
.tables               -- list all tables
.schema <table>       -- show CREATE TABLE for a table
.mode column          -- human-readable column output
.timer on             -- show query execution time
```

---

## Concept reference

| Polars operation | SQL concept | Section |
|---|---|---|
| `.unique(subset=[...])` | `ROW_NUMBER()` dedup CTE | [§1 Deduplication](#1-deduplication) |
| `.group_by().agg()` | `GROUP BY` + aggregate functions | [§2 Aggregation](#2-aggregation) |
| `.filter()` | `WHERE` / `HAVING` | [§3 Filtering](#3-filtering) |
| `.sort()` | `ORDER BY` | [§4 Sorting](#4-sorting) |
| `.join()` | `JOIN` | [§5 Joins](#5-joins) |
| `.with_columns()` | `SELECT ..., expr AS alias` | [§6 Derived columns](#6-derived-columns) |
| `fill_null(x)` | `COALESCE(col, x)` | [§7 Null handling](#7-null-handling) |
| `dt.year()`, `dt.quarter()` | `EXTRACT(YEAR FROM ...)` | [§8 Date arithmetic](#8-date-arithmetic) |
| `str.len_chars()` | `LENGTH(col)` | [§9 String metrics](#9-string-metrics) |
| `pl.struct(...).n_unique()` | `COUNT(DISTINCT ...)` on composite | [§10 Counting composite keys](#10-counting-composite-keys) |
| Chained `.with_columns()` | CTE chain (`WITH a AS ..., b AS ...`) | [§11 CTEs as pipeline stages](#11-ctes-as-pipeline-stages) |
| Window / rank | `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` | [§12 Window functions](#12-window-functions) |

---

## 1. Deduplication

**Polars:** `df.unique(subset=["primary_key"], keep="first")`

**SQL:** There is no `DISTINCT ON (key)` in DuckDB (that's PostgreSQL-only). The standard pattern
is a window function to rank rows within a partition, then filter to rank = 1.

```sql
-- Skeleton — fill in the ORDER BY to control which row survives:
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY primary_key ORDER BY ???) AS rn
    FROM returns_raw
)
SELECT * EXCLUDE rn FROM ranked WHERE rn = 1;
```

`EXCLUDE` is DuckDB-specific: "all columns except these" — handy for dropping helper columns.
`QUALIFY rn = 1` is an alternative (DuckDB-only) that filters on a window function result
without a wrapping CTE — useful to know for interviews.

```sql
-- QUALIFY form (DuckDB only):
SELECT *
FROM returns_raw
QUALIFY ROW_NUMBER() OVER (PARTITION BY primary_key ORDER BY ???) = 1;
```

**Doc:** [DuckDB QUALIFY clause](https://duckdb.org/docs/sql/query_syntax/qualify)
— This is a DuckDB/Snowflake extension that standard SQL doesn't have. Interviewers
sometimes test whether you know it exists.

**Interview trap:** If all columns of two rows are identical, `SELECT DISTINCT *` works.
But if you only want distinctness *on a subset of columns*, you need the `ROW_NUMBER` approach.
Know when each applies.

---

## 2. Aggregation

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
| `pl.col("x").min()` / `.max()` | `MIN(x)` / `MAX(x)` |
| `pl.col("x").first()` | `ANY_VALUE(x)` (DuckDB — picks a deterministic value from the group) |
| `.any()` on a boolean | `BOOL_OR(x)` (DuckDB) |
| `.all()` on a boolean | `BOOL_AND(x)` (DuckDB) |

**Conditional aggregation (critical interview pattern):**

`FILTER (WHERE ...)` is the modern SQL way to aggregate conditionally in one pass.
It replaces the classic `SUM(CASE WHEN x THEN 1 ELSE 0 END)` pattern and is cleaner:

```sql
-- Count of Yes votes and No votes in a single GROUP BY:
SELECT
    full_name,
    COUNT(*) FILTER (WHERE vote_type = 'Voted Yes') AS yes_votes,
    COUNT(*) FILTER (WHERE vote_type = 'Voted No')  AS no_votes
FROM votes
GROUP BY full_name;
```

**Doc:** [DuckDB aggregate functions](https://duckdb.org/docs/sql/functions/aggregates)
— covers `PERCENTILE_CONT`, `ANY_VALUE`, `BOOL_OR`, `FILTER`, and more.

**Interview note:** Know the difference between `COUNT(*)` (counts all rows including nulls)
and `COUNT(col)` (excludes nulls). Interviewers test this.

---

## 3. Filtering — WHERE vs HAVING

**Polars:** `.filter()` before aggregation / `.filter()` after `.group_by().agg()`

**SQL — the critical distinction:**

`WHERE` runs *before* `GROUP BY`. It filters individual rows.
`HAVING` runs *after* `GROUP BY`. It filters aggregated results.

```sql
-- Wrong — can't reference COUNT(*) in WHERE:
SELECT full_name, COUNT(*) AS n
FROM votes
WHERE COUNT(*) > 10    -- syntax error
GROUP BY full_name;

-- Correct:
SELECT full_name, COUNT(*) AS n
FROM votes
GROUP BY full_name
HAVING COUNT(*) > 10;
```

**FILTER (WHERE ...)** is different again — it's inside an aggregate function, not a clause:
```sql
-- These are not the same:
COUNT(*) FILTER (WHERE x = 'Yes')   -- conditional aggregate: count only Yes rows
HAVING COUNT(*) > 10                -- post-aggregation filter: only groups with >10 total rows
```

**Doc:** [DuckDB SELECT syntax](https://duckdb.org/docs/sql/query_syntax/select)

**Interview pattern:** Any question about "top N per group where the group has at least X" combines
both: `WHERE` to pre-filter rows, `GROUP BY` to aggregate, `HAVING` to filter groups,
and `ORDER BY + LIMIT` to rank. Write all four.

---

## 4. Sorting

**Polars:** `.sort(["col_a", "col_b"], descending=[True, False])`

**SQL:** `ORDER BY col_a DESC, col_b ASC`

Null handling — in DuckDB, nulls sort last in `ASC` and first in `DESC` by default.
Override with `NULLS FIRST` / `NULLS LAST`:

```sql
ORDER BY total_returns DESC NULLS LAST
```

**Interview note:** Know the default null sort direction for the DB you're using —
it differs between PostgreSQL, DuckDB, and BigQuery. DuckDB matches PostgreSQL.

---

## 5. Joins

**Polars join types → SQL:**

| Polars `how=` | SQL |
|---|---|
| `"inner"` | `INNER JOIN` (or just `JOIN`) |
| `"left"` | `LEFT JOIN` |
| `"outer"` | `FULL OUTER JOIN` |
| `"cross"` | `CROSS JOIN` |
| `"semi"` | `WHERE EXISTS (subquery)` |
| `"anti"` | `WHERE NOT EXISTS (subquery)` |

**Key engineering practice:** After a `LEFT JOIN`, any column from the right side can be NULL
if there's no match. Always think about which columns need `COALESCE` fallbacks.

**CROSS JOIN with a scalar CTE** is the idiomatic way to attach a single aggregate value
to every row without a subquery per row:

```sql
WITH total AS (SELECT COUNT(DISTINCT vote_id) AS n FROM votes)
SELECT v.full_name, COUNT(DISTINCT v.vote_id) AS voted_in, t.n AS total_divisions
FROM votes v
CROSS JOIN total t
GROUP BY v.full_name, t.n;
```

**Doc:** [DuckDB joins](https://duckdb.org/docs/sql/query_syntax/from#joins)

**Interview trap:** `INNER JOIN` vs `LEFT JOIN` in aggregations. If you `INNER JOIN` and
a TD has no lobbying returns, they disappear from the result entirely. Usually you want
`LEFT JOIN` and `COALESCE(count, 0)`.

---

## 6. Derived columns

**Polars:** `.with_columns(pl.col("x").dt.year().alias("year"))`

**SQL:** `SELECT *, EXTRACT(YEAR FROM x) AS year FROM ...`

DuckDB's `SELECT * REPLACE (expr AS col)` swaps out one column without listing the rest:

```sql
-- Replace amount string with a cleaned numeric version:
SELECT * REPLACE (
    TRY_CAST(REPLACE(REPLACE(amount, '€', ''), ',', '') AS DOUBLE) AS amount_num
) FROM payments;
```

**Doc:** [DuckDB SELECT * REPLACE](https://duckdb.org/docs/sql/expressions/star#replace-clause)

---

## 7. Null handling

| Polars | SQL |
|---|---|
| `.fill_null("")` | `COALESCE(col, '')` |
| `.fill_null(0)` | `COALESCE(col, 0)` |
| `.drop_nulls(subset=["x"])` | `WHERE x IS NOT NULL` |
| `x / y` (crashes on 0) | `x / NULLIF(y, 0)` — returns NULL instead of error |

`COALESCE` takes multiple arguments and returns the first non-null:
```sql
COALESCE(lobby_url, FORMAT('https://www.lobbying.ie/return/{}', primary_key))
```

`NULLIF(x, y)` returns NULL when `x = y`, otherwise returns `x`. The safe-division
pattern `SUM(x) / NULLIF(COUNT(*), 0)` is a canonical interview answer.

**Doc:** [DuckDB COALESCE](https://duckdb.org/docs/sql/functions/utility#coalesceexpr-)

---

## 8. Date arithmetic

| Polars | DuckDB SQL |
|---|---|
| `.dt.year()` | `EXTRACT(YEAR FROM col)` or `YEAR(col)` |
| `.dt.quarter()` | `EXTRACT(QUARTER FROM col)` |
| `.dt.month()` | `EXTRACT(MONTH FROM col)` |
| `(date_a - date_b).dt.total_days()` | `DATEDIFF('day', date_b, date_a)` |

**Year-quarter label:**
```sql
CONCAT(EXTRACT(YEAR FROM lobbying_period_start_date), '-Q',
       EXTRACT(QUARTER FROM lobbying_period_start_date)) AS year_quarter
```

**Doc:** [DuckDB date/time functions](https://duckdb.org/docs/sql/functions/timestamp)

**Interview note:** `DATEDIFF` argument order varies by database (`start, end` vs `end, start`).
In DuckDB it is `DATEDIFF('day', start, end)`. Always verify for the DB in the question.

---

## 9. String metrics

**Polars:** `pl.col("x").fill_null("").str.len_chars()`

**SQL:** `LENGTH(COALESCE(x, ''))`

DuckDB also has `LEN()` as an alias. Both return character count, which matters for
Irish text with accented characters (byte count would overcount).

**Doc:** [DuckDB string functions](https://duckdb.org/docs/sql/functions/char)

---

## 10. Counting composite keys

**Polars:**
```python
pl.struct(
    pl.col("lobbying_period_start_date").dt.year().alias("year"),
    pl.col("lobbying_period_start_date").dt.quarter().alias("quarter"),
).n_unique().alias("distinct_periods_filed")
```

**`COUNT(DISTINCT col)` only works on a single column.** For composite distinctness:

Option A — concatenate into a string key (simple, readable, fine for this use case):
```sql
COUNT(DISTINCT CONCAT(EXTRACT(YEAR FROM lobbying_period_start_date), '-',
                      EXTRACT(QUARTER FROM lobbying_period_start_date)))
```

Option B — subquery count of distinct rows (more principled, shows up in interviews):
```sql
(SELECT COUNT(*) FROM (
    SELECT DISTINCT EXTRACT(YEAR FROM lobbying_period_start_date),
                    EXTRACT(QUARTER FROM lobbying_period_start_date)
    FROM t WHERE t.lobbyist_name = outer_query.lobbyist_name
))
```

**Doc:** [DuckDB COUNT DISTINCT](https://duckdb.org/docs/sql/functions/aggregates#count)

---

## 11. CTEs as pipeline stages

Every Polars chain of `.with_columns()` calls maps naturally to a CTE chain.
Think of each CTE as one transformation stage — one concern, one name.

```sql
WITH
-- Stage 1: clean the amount column
cleaned AS (
    SELECT *,
        TRY_CAST(REPLACE(REPLACE(amount, '€', ''), ',', '') AS DOUBLE) AS amount_num
    FROM payments_raw
),
-- Stage 2: attach the year
with_year AS (
    SELECT *, EXTRACT(YEAR FROM date_paid) AS year
    FROM cleaned
),
-- Stage 3: compute per-TD totals
td_totals AS (
    SELECT full_name, year, SUM(amount_num) AS total_paid, COUNT(*) AS payment_count
    FROM with_year
    GROUP BY full_name, year
)
SELECT * FROM td_totals ORDER BY total_paid DESC;
```

**Good practice:**
- Name CTEs after what they *are*, not what they *do* (`deduplicated`, not `after_dedup`).
- Each CTE should have one transformation concern.
- Prefer CTEs over subqueries for anything referenced more than once — the query planner
  materialises a CTE only once.

**Doc:** [DuckDB CTE documentation](https://duckdb.org/docs/sql/query_syntax/with)

**Interview note:** Recursive CTEs are a separate concept (`WITH RECURSIVE`). Know they
exist and what they're for (hierarchical data, graph traversal). DuckDB supports them.
[DuckDB recursive CTEs](https://duckdb.org/docs/sql/query_syntax/with#recursive-ctes)

---

## 12. Window functions

Window functions compute an aggregate alongside individual rows without collapsing the result.
They are one of the highest-frequency interview topics for data engineering and analytics roles.

**Core syntax:**
```sql
function() OVER (
    PARTITION BY ...   -- defines the group (like GROUP BY but rows are kept)
    ORDER BY ...       -- required for ranking and running totals
    ROWS BETWEEN ...   -- optional: define a frame within the window
)
```

**Ranking functions:**
```sql
ROW_NUMBER() OVER (PARTITION BY lobbyist_name ORDER BY return_date DESC) -- unique rank
RANK()        OVER (...)                                                  -- ties get same rank, gaps after
DENSE_RANK()  OVER (...)                                                  -- ties get same rank, no gaps
NTILE(4)      OVER (ORDER BY total_returns DESC)                          -- quartile bucket
```

**Aggregate functions as windows (no frame collapse):**
```sql
-- Group total alongside each row — replaces group_by + join back:
COUNT(*) OVER (PARTITION BY lobbyist_name) AS total_returns_for_lobbyist
SUM(amount) OVER (PARTITION BY full_name ORDER BY date_paid) AS running_total
```

**LAG and LEAD (access neighbouring rows):**
```sql
-- Compare a TD's attendance to the previous year:
LAG(sitting_days, 1) OVER (PARTITION BY full_name ORDER BY year) AS prev_year_days
```

**Running totals and cumulative distributions:**
```sql
SUM(return_count) OVER (PARTITION BY lobbyist_name ORDER BY year_quarter
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
```

**The key interview pattern — window aggregate + GROUP BY in one query:**
```sql
-- Vote type breakdown as % of annual total in one pass:
SELECT
    EXTRACT(YEAR FROM date) AS year,
    vote_type,
    COUNT(*) AS n,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY EXTRACT(YEAR FROM date)), 1) AS pct
FROM votes
GROUP BY year, vote_type
ORDER BY year, vote_type;
```
Here `SUM(COUNT(*)) OVER (...)` sums the per-group `COUNT(*)` values back up by year.
The outer `COUNT(*)` is evaluated by `GROUP BY`; the window sees the grouped result.
This is one of the trickier patterns — understand exactly what the OVER clause is partitioning.

**Doc:** [DuckDB window functions](https://duckdb.org/docs/sql/window_functions)
— one of the best reference pages; covers all functions and frame clauses.

Also see: [SQL Window Functions explained visually (antonz.org)](https://antonz.org/window-functions/)
— the clearest visual explanation of PARTITION BY / ORDER BY that exists.

---

## 13. DuckDB-specific syntax worth knowing

```sql
-- Exclude columns without listing everything:
SELECT * EXCLUDE (rn, internal_id) FROM t;

-- Replace a column's value inline:
SELECT * REPLACE (UPPER(full_name) AS full_name) FROM t;

-- QUALIFY: filter on a window function result without a CTE:
SELECT * FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY date DESC) = 1;

-- Pivot (cross-tab):
PIVOT t ON year USING SUM(return_count) GROUP BY lobbyist_name;

-- Sampling for quick exploration:
SELECT * FROM t USING SAMPLE 1000;

-- Show query plan:
EXPLAIN SELECT * FROM t WHERE ...;

-- BOOL_OR: boolean OR across a group (any row is true → true):
SELECT full_name, BOOL_OR(is_landlord) AS ever_landlord FROM interests GROUP BY full_name;

-- FILTER on an aggregate:
COUNT(*) FILTER (WHERE vote_type = 'Voted Yes') AS yes_votes
```

**Doc:** [DuckDB SQL introduction](https://duckdb.org/docs/sql/introduction)

---

## 14. Vote history queries

Good DuckDB playground: clear grain (one row = one member × one division), several derived metrics.

### Load it

```sql
CREATE OR REPLACE TABLE votes AS
SELECT * FROM read_csv_auto('data/gold/current_dail_vote_history.csv');
```

### Participation rate per member

How many divisions did each TD vote in, out of all divisions held?

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

`FILTER (WHERE ...)` is cleaner than `SUM(CASE WHEN ...)`.
`NULLIF(x, 0)` prevents division-by-zero.

---

### Most contested divisions

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

### TDs who voted against their own party

Party is in the gold layer. Logic: for each division, find the majority vote direction
within each party, then flag members who voted differently.

```sql
WITH party_majority AS (
    SELECT
        vote_id, party, vote_type,
        COUNT(*) AS n,
        ROW_NUMBER() OVER (
            PARTITION BY vote_id, party
            ORDER BY COUNT(*) DESC
        ) AS rn
    FROM votes
    WHERE vote_type IN ('Voted Yes', 'Voted No') AND party IS NOT NULL
    GROUP BY vote_id, party, vote_type
),
party_line AS (
    SELECT vote_id, party, vote_type AS party_direction
    FROM party_majority WHERE rn = 1
),
flagged AS (
    SELECT
        v.full_name, v.party, v.constituency_name,
        v.vote_id, v.debate_title, v.date,
        v.vote_type, pl.party_direction,
        v.vote_type != pl.party_direction AS crossed_party_line
    FROM votes v
    JOIN party_line pl ON v.vote_id = pl.vote_id AND v.party = pl.party
    WHERE v.vote_type IN ('Voted Yes', 'Voted No')
)
SELECT
    full_name, party, constituency_name,
    COUNT(*)                             AS eligible_votes,
    SUM(crossed_party_line::INT)         AS party_line_breaks,
    ROUND(100.0 * SUM(crossed_party_line::INT) / COUNT(*), 1) AS break_rate_pct
FROM flagged
GROUP BY full_name, party, constituency_name
HAVING eligible_votes >= 10
ORDER BY break_rate_pct DESC;
```

Concepts: `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY COUNT(*) DESC)` — rank within
a group to pick the majority. Boolean cast `::INT` to sum a true/false flag. `HAVING`
to exclude small samples.

---

## 15. Streamlit app logic — candidate SQL views

Every view below is business logic that currently runs in Python on every page render.
The goal is to move it into a pre-computed `CREATE VIEW` or `CREATE TABLE AS SELECT` so
the Streamlit page just loads a small result set.

**Pattern:** Write the skeleton, figure out the `???` placeholders, run it in DuckDB,
verify it matches what the Python currently produces.

**When to use a view vs a materialised table:**

| Situation | Prefer |
|---|---|
| Result is always the same, fast to compute | `CREATE OR REPLACE VIEW` |
| Result is expensive and rarely changes | `CREATE TABLE AS SELECT` — materialised once |
| Result is filtered by user input (name, year) | DuckDB macro or parameterised query |
| >100k rows, read repeatedly by the app | Parquet (`COPY ... TO 'file.parquet'`) |

---

### Attendance page

**What the Python does:**

`_yearly_summary()` groups attendance by `(full_name, year)` and takes the first value of
`sitting_days_count` and `other_days_count`. The `.first()` agg (not `.sum()`) is the key
signal — these counts are denormalised across rows, so you want `ANY_VALUE`, not `SUM`.

`_totals()` then sums across years to get career totals and adds `total_days`.

The stat strip computes five more aggregates over the totals frame — average, best, worst,
and a conditional percentage.

**`v_attendance_summary`** — per-TD per-year counts

```sql
-- Skeleton:
CREATE OR REPLACE VIEW v_attendance_summary AS
SELECT
    ???  AS full_name,
    year,
    ANY_VALUE(???) AS sitting_days,
    ANY_VALUE(???) AS other_days
FROM attendance
GROUP BY ???, year;
```

Hint: `ANY_VALUE` is the DuckDB equivalent of Polars `.first()` on a non-grouped column.
Why is `.first()` correct here instead of `SUM`? Look at what `sitting_days_count` represents.
[DuckDB ANY_VALUE](https://duckdb.org/docs/sql/functions/aggregates#any_valuearg)

---

**`v_attendance_totals`** — career totals per TD (sum across years)

```sql
-- Skeleton:
CREATE OR REPLACE VIEW v_attendance_totals AS
SELECT
    full_name,
    SUM(???) AS sitting_days,
    SUM(???) AS other_days,
    ??? + ???  AS total_days
FROM v_attendance_summary
GROUP BY full_name;
```

Can you write this as a single query (no intermediate view)? Which is more readable?
The intermediate view is better engineering — each view has one responsibility.

---

**Stat strip query** — all five header metrics in one pass

The Python computes `avg_sit`, `avg_other`, `pct_low`, `best_row`, `worst_row` as five
separate pandas calls. In SQL you can do all five in a single query.

```sql
-- Skeleton — fill in the conditional aggregate for pct_low:
SELECT
    COUNT(*)                                                      AS n_tds,
    ROUND(AVG(sitting_days), 0)                                   AS avg_sitting,
    ROUND(AVG(other_days), 0)                                     AS avg_other,
    ROUND(??? * 100.0 / COUNT(*), 0)                              AS pct_under_50,
    MAX(sitting_days)                                             AS max_sitting,
    MIN(sitting_days)                                             AS min_sitting
FROM v_attendance_totals;
```

Hint: for `pct_low` (TDs with < 50 plenary days), use `COUNT(*) FILTER (WHERE ...)`.
Pattern: §3 Filtering / §2 Aggregation.
[DuckDB FILTER](https://duckdb.org/docs/sql/functions/aggregates#filter-clause)

For `best_row` and `worst_row` (the TD name alongside the max value), you need a different
approach — a scalar aggregate can't return the associated name. Think about using
`arg_max(full_name, sitting_days)` or a window function `RANK()` in a separate CTE.
[DuckDB arg_max](https://duckdb.org/docs/sql/functions/aggregates#arg_maxarg-val)

---

### Interests page

**What the Python does in `_render_landing()`:**

Three separate `.groupby().agg()` calls compute `td_count`, `landlord_count`, and
`minister_count`. Then a fourth groupby builds the leaderboard for the latest year only.
All four run on every page render.

**`v_interests_landing_stats`** — four metrics in one GROUP BY

```sql
CREATE OR REPLACE VIEW v_interests_landing_stats AS
SELECT
    ???                                                                    AS td_count,
    COUNT(DISTINCT CASE WHEN ??? THEN full_name END)                      AS landlord_count,
    COUNT(DISTINCT CASE WHEN ??? THEN full_name END)                      AS minister_count,
    MIN(year_declared)                                                     AS year_from,
    MAX(year_declared)                                                     AS year_to
FROM member_interests;
```

`COUNT(DISTINCT CASE WHEN condition THEN full_name END)` is the SQL equivalent of Polars
`groupby("full_name")["is_landlord"].any()`. Null-safe: when `condition` is false,
`CASE WHEN` returns NULL, and `COUNT(DISTINCT ...)` ignores NULLs.

Pattern: §2 Aggregation — conditional aggregation.
[COUNT with CASE WHEN pattern](https://modern-sql.com/feature/filter)

---

**`v_interests_leaderboard`** — top 10 TDs by declared interests, latest year

```sql
CREATE OR REPLACE VIEW v_interests_leaderboard AS
SELECT
    full_name,
    MAX(interest_count)   AS interest_count,
    ANY_VALUE(party)      AS party,
    BOOL_OR(is_landlord)  AS is_landlord
FROM member_interests
WHERE year_declared = (SELECT MAX(year_declared) FROM member_interests)
GROUP BY full_name
ORDER BY interest_count DESC
LIMIT 10;
```

Note the correlated scalar subquery `(SELECT MAX(year_declared) ...)` in the `WHERE`.
This is fine for a view because DuckDB evaluates it once. In a hot query path you'd
compute it in a CTE first.

What does `BOOL_OR` do here? Why is it more accurate than `ANY_VALUE` for the landlord flag?
[DuckDB BOOL_OR](https://duckdb.org/docs/sql/functions/aggregates#bool_orarg)

---

### Payments page

**`v_td_payment_totals`** — all-time total paid and payment count per TD

The page currently computes this at render time over the full unfiltered CSV.

```sql
CREATE OR REPLACE VIEW v_td_payment_totals AS
SELECT
    ???            AS full_name,
    SUM(???)       AS total_paid_alltime,
    COUNT(*)       AS payment_count
FROM payments
GROUP BY ???;
```

The `Amount` column arrives as `'€2,445.83'` — clean it before summing.
Look at `REPLACE()` to strip the `€` and `,`, then `TRY_CAST(... AS DOUBLE)`.
`TRY_CAST` returns NULL on parse failure instead of crashing — prefer it over `CAST`.
[DuckDB TRY_CAST](https://duckdb.org/docs/sql/expressions/cast#try_cast)

---

**`v_td_payment_annual`** — per-TD per-year totals

Pattern: §2 Aggregation + §8 Date arithmetic — add `EXTRACT(YEAR FROM date_paid)` to the GROUP BY.

---

### Votes page

**What the Python does:**

`_td_landing()` builds the full member summary (yes count, no count, distinct divisions,
and `yes_pct`) on every page render by grouping the entire vote history file.

`_build_debates()` runs a two-stage aggregation on every year switch — first grouping by
`(debate_title, vote_id, date, vote_outcome)` to count yes/no/abstained per division,
then grouping by `debate_title` to collapse to one row per debate.

**`v_td_vote_summary`** — per-TD summary: votes, divisions, yes%

```sql
CREATE OR REPLACE VIEW v_td_vote_summary AS
SELECT
    full_name,
    ANY_VALUE(party)             AS party,
    ANY_VALUE(constituency_name) AS constituency_name,
    COUNT(*) FILTER (WHERE vote_type = 'Voted Yes') AS yes_votes,
    COUNT(*) FILTER (WHERE vote_type = 'Voted No')  AS no_votes,
    COUNT(DISTINCT vote_id)                          AS divisions,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE vote_type = 'Voted Yes')
        / NULLIF(COUNT(*) FILTER (WHERE vote_type IN ('Voted Yes', 'Voted No')), 0)
    , 0) AS yes_pct
FROM votes
GROUP BY full_name;
```

This is a complete example — study the `NULLIF` safe-division pattern and the
`ANY_VALUE` for non-grouped columns. Can you explain why `ANY_VALUE(party)` is correct
here whereas `MAX(party)` or `MIN(party)` would also work but be misleading?

---

**`v_debate_divisions`** — per-debate summary: divisions, yes/no totals, carried/lost

This replaces `_build_debates()`. The Python does it in two pandas groupbys chained together.
In SQL the natural translation is a CTE (one stage → one CTE):

```sql
CREATE OR REPLACE VIEW v_debate_divisions AS
WITH per_division AS (
    SELECT
        debate_title,
        vote_id,
        date,
        vote_outcome,
        COUNT(*) FILTER (WHERE vote_type = 'Voted Yes')  AS yes,
        COUNT(*) FILTER (WHERE vote_type = 'Voted No')   AS no,
        COUNT(*) FILTER (WHERE vote_type = 'Abstained')  AS abstained
    FROM votes
    GROUP BY ???
),
per_debate AS (
    SELECT
        debate_title,
        COUNT(DISTINCT vote_id)                            AS divisions,
        MIN(date)                                          AS first_date,
        MAX(date)                                          AS last_date,
        COUNT(*) FILTER (WHERE vote_outcome = 'Carried')  AS carried,
        COUNT(*) FILTER (WHERE vote_outcome = 'Lost')     AS lost,
        SUM(yes)                                           AS total_yes,
        SUM(no)                                            AS total_no
    FROM per_division
    GROUP BY ???
)
SELECT * FROM per_debate ORDER BY last_date DESC;
```

What goes in the two `GROUP BY ???` clauses? Think about the grain of each stage.
The first CTE must group on enough columns to get one row per division — what is the
minimum set? And what does `COUNT(DISTINCT vote_id)` at the debate level measure?

Pattern: §11 CTEs as pipeline stages + §2 Aggregation.

---

### Lobbying — Transparency page

**What the Python does:**

`_transparency()` loads two separate CSVs (`experimental_time_to_publish.csv` and
`experimental_return_description_lengths.csv`), computes `avg_desc_len` per lobbyist
from the second, then merges both into a scorecard. This is a classic two-CTE join.

**`v_transparency_scorecard`** — per-organisation: filing latency + description quality

```sql
CREATE OR REPLACE VIEW v_transparency_scorecard AS
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
        AVG(LENGTH(COALESCE(specific_details, '')) + LENGTH(COALESCE(intended_results, '')))
            AS avg_desc_len
    FROM returns_master
    GROUP BY lobbyist_name
)
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

What JOIN type is correct, and why? Think about what happens to an organisation that
has returns but no description data in `returns_master` — should it appear in the
scorecard? What about an org with descriptions but no `experimental_time_to_publish`
entry? This is a JOIN type question with a real engineering consequence.

`PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x)` is SQL's median function — the
`WITHIN GROUP` syntax is standard SQL for ordered-set aggregates. DuckDB also has
`MEDIAN(x)` as a shorthand.
[DuckDB PERCENTILE_CONT](https://duckdb.org/docs/sql/functions/aggregates#percentile_contfraction-order-by-column)

---

### Lobbying — politician and lobbyist profiles

**What the Python does:**

Both `_politician_profile()` and `_lobbyist_profile()` run two separate `groupby + nunique`
calls at render time to produce a policy-area breakdown and an org breakdown.

**`v_politician_lobby_breakdown`** — per-politician, per-policy-area: distinct return count

```sql
-- Skeleton:
CREATE OR REPLACE VIEW v_politician_lobby_breakdown AS
SELECT
    full_name,
    public_policy_area,
    COUNT(DISTINCT ???) AS returns
FROM politician_returns_detail
GROUP BY full_name, public_policy_area
ORDER BY full_name, returns DESC;
```

This is a simple GROUP BY but the `COUNT(DISTINCT primary_key)` vs `COUNT(*)` choice
matters — why? Check whether `politician_returns_detail` can have duplicate rows for the
same `(full_name, primary_key, public_policy_area)` combination.

---

### Legislation page

**`v_bill_progress`** — per-bill: max stage reached, status, source

The `_stages_view()` bill_summary computes this on every render:
`groupby([bill_no, bill_year, title, bill_status, source]).agg(stages_reached=max)`.

```sql
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

`ANY_VALUE` is correct for `title`, `bill_status`, `source` because they are the same
for all rows of a given bill (attribute of the bill, not the stage).
Using `MAX` on a string would also work but is semantically wrong.

---

**`v_most_debated_bills`** — per-bill: debate count, date range, status

`_debates_view()` computes `groupby([title, bill_year, bill_status]).size()` on every render.

```sql
CREATE OR REPLACE VIEW v_most_debated_bills AS
SELECT
    title,
    bill_year,
    ANY_VALUE(bill_status)  AS bill_status,
    COUNT(*)                AS debate_count,
    MIN(date)               AS first_debate,
    MAX(date)               AS last_debate
FROM debates
GROUP BY title, bill_year
ORDER BY debate_count DESC;
```

---

**`v_td_legislative_agenda`** — per-TD: bills sponsored, primary vs co-sponsor, bills that passed

Join `sponsors` to `v_bill_progress` on `(bill_no, bill_year)`, then group by `td_name`.
Uses conditional aggregation (`COUNT(CASE WHEN ...)`) for the status breakdown.

This is the first view that joins two of your own views — build `v_bill_progress` first.

```sql
-- Skeleton:
CREATE OR REPLACE VIEW v_td_legislative_agenda AS
SELECT
    s.td_name,
    COUNT(*)                                              AS total_sponsorships,
    COUNT(*) FILTER (WHERE s.is_primary)                  AS primary_sponsorships,
    COUNT(*) FILTER (WHERE b.bill_status = 'Enacted')     AS bills_enacted,
    COUNT(*) FILTER (WHERE b.bill_status = 'Lapsed')      AS bills_lapsed
FROM sponsors s
JOIN v_bill_progress b ON s.bill_no = b.bill_no AND s.bill_year = b.bill_year
GROUP BY s.td_name
ORDER BY primary_sponsorships DESC;
```

What bill statuses actually exist in your data? Run `SELECT DISTINCT bill_status FROM stages`
to find out before writing the FILTER conditions.

---

### Cross-dataset view (build last)

**`v_td_full_profile`** — one row per TD: attendance, payments, interests, lobbying, bills

This is the gold layer goal — everything about a TD in one place. The join key
(`normalise_join_key.py`) exists precisely to make this cross-dataset join possible.

Every join here must be a `LEFT JOIN` — a TD may appear in some datasets but not others,
and an `INNER JOIN` would silently drop TDs missing from any one source.

```sql
CREATE OR REPLACE VIEW v_td_full_profile AS
SELECT
    a.full_name,
    a.join_key,
    COALESCE(t.sitting_days, 0)          AS sitting_days,
    COALESCE(p.total_paid_alltime, 0)    AS total_psa_received,
    COALESCE(i.interest_count, 0)        AS declared_interests,
    COALESCE(l.times_lobbied, 0)         AS times_lobbied,
    COALESCE(s.bills_sponsored, 0)       AS bills_sponsored
FROM v_attendance_totals a
LEFT JOIN v_td_payment_totals p    ON a.join_key = p.join_key
LEFT JOIN v_interests_summary i    ON a.join_key = i.join_key
LEFT JOIN v_most_lobbied_politicians l ON a.join_key = l.join_key
LEFT JOIN v_td_legislative_agenda s ON a.join_key = s.join_key;
```

Every metric column must be wrapped in `COALESCE(metric, 0)` — a LEFT JOIN returns NULL
for non-matching rows, and NULLs silently propagate through arithmetic.

---

## 16. Resources

### DuckDB — reference

- [DuckDB SQL introduction](https://duckdb.org/docs/sql/introduction) — start here
- [DuckDB aggregate functions](https://duckdb.org/docs/sql/functions/aggregates) — `PERCENTILE_CONT`, `ANY_VALUE`, `BOOL_OR`, `FILTER`, `arg_max`
- [DuckDB window functions](https://duckdb.org/docs/sql/window_functions) — `ROW_NUMBER`, `RANK`, `LAG`, `LEAD`, frame clauses
- [DuckDB string functions](https://duckdb.org/docs/sql/functions/char) — `LENGTH`, `REPLACE`, `REGEXP_MATCHES`
- [DuckDB date/time functions](https://duckdb.org/docs/sql/functions/timestamp) — `EXTRACT`, `DATEDIFF`, intervals
- [DuckDB CTE documentation](https://duckdb.org/docs/sql/query_syntax/with) — including recursive CTEs
- [DuckDB QUALIFY clause](https://duckdb.org/docs/sql/query_syntax/qualify) — filter on window function results
- [DuckDB macros](https://duckdb.org/docs/sql/statements/create_macro) — parameterised queries (the `AS TABLE` form replaces Streamlit filter logic)
- [DuckDB Python API](https://duckdb.org/docs/api/python/overview) — reading views into pandas from inside Python

### SQL learning — structured

- [Select Star SQL](https://selectstarsql.com/) — free, interactive, real civic data; best first resource
- [Mode Analytics SQL tutorial](https://mode.com/sql-tutorial/) — basic → advanced, well-structured for interview prep
- [SQLZoo](https://sqlzoo.net/) — short focused exercises on GROUP BY, JOIN, subqueries
- [The Art of PostgreSQL](https://theartofpostgresql.com/) — book, PostgreSQL-focused but 90% transfers; strong on CTEs and window functions

### SQL concepts — deep dives

- [SQL Window Functions explained visually (antonz.org)](https://antonz.org/window-functions/) — best visual explanation of PARTITION BY / ORDER BY
- [Modern SQL](https://modern-sql.com/) — concise reference for `FILTER`, window functions, `LATERAL`, features most tutorials skip
- [Use The Index, Luke](https://use-the-index-luke.com/) — SQL performance and indexing; worth reading before adding indexes to DuckDB tables
- [The Medallion Architecture (Databricks)](https://www.databricks.com/glossary/medallion-architecture) — the reference for bronze/silver/gold at scale

---

## Interview prep focus

The patterns that appear most frequently in data engineering and analytics SQL rounds:

**Core patterns — know these cold:**
- `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` for deduplication and ranking
- `COUNT(DISTINCT col)` — know when you need it vs `COUNT(*)`
- `HAVING` vs `WHERE` — post- vs pre-aggregation filtering (interviewers test this every time)
- `LEFT JOIN` + `COALESCE` — the standard null-safety pattern for optional data
- `FILTER (WHERE ...)` as a cleaner alternative to `SUM(CASE WHEN ...)`
- CTEs vs subqueries — when each is cleaner (rule of thumb: CTE if referenced more than once)
- `NULLIF(x, 0)` for safe division

**Intermediate patterns — know how to construct them:**
- Window aggregate alongside GROUP BY — `SUM(COUNT(*)) OVER (PARTITION BY ...)` inside a query that already has `GROUP BY`
- `CROSS JOIN` with a single-row CTE to attach a scalar total to every row
- `QUALIFY` for filtering on window function results without a wrapping CTE
- `arg_max(label, value)` — return the label associated with the max value (replaces a groupby + join)
- `BOOL_OR` / `BOOL_AND` for boolean aggregation across a group
- Multi-step CTE pipeline — three or more CTEs, each with one concern

**Advanced patterns — know they exist, understand when to reach for them:**
- `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x)` — ordered-set aggregate for median
- `PIVOT` — cross-tab transformation (DuckDB-specific syntax; standard SQL uses CASE WHEN)
- Recursive CTEs — hierarchical data and graph traversal (know the structure even if rare here)
- Self-joins — comparing rows within the same table (e.g., rebellion rate requires knowing the majority direction)
- `LATERAL` joins — correlated subquery in the FROM clause; use when you need per-row subqueries

**The interview mindset:**
A good answer names the *constraint* it's solving, not just the syntax. "I use LEFT JOIN
here because a TD may not have any lobbying returns, and INNER JOIN would silently drop
them from the profile" is a better answer than just writing a LEFT JOIN. Examiners want
to see that you know *why*, not just *what*.
