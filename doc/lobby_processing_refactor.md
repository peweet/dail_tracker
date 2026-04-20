# lobby_processing.py — refactoring notes

> **Scope:** ingest, transform chain, `build_*` drill-down tables, `get_clients`, and `main`.
> The `compute_*` experimental functions are being replaced by SQL — ignore them.

---

## The problem in one sentence

Each function in the transform chain and every `build_*` function silently assumes the
incoming DataFrame has a specific set of columns — if a column is absent the function
either raises an unreadable Polars error or, worse, silently produces wrong output.

---

## The three concrete problems

### 1. `get_clients` is broken and inconsistent (lines 356–364)

```python
def get_clients(df: pl.DataFrame) -> pl.DataFrame:
    if df.col('dpos_or_former_dpos_who_carried_out_lobbying_name').is_not_null().any():
```

`df.col()` is not a valid Polars method — this raises `AttributeError` at runtime.
The function also duplicates logic that `main()` already calls explicitly
(`parse_current_or_former_dpos`, `parse_clients`). It is a half-finished coordinator
that currently does nothing and will crash if called.

**Options:** delete it and keep the explicit calls in `main()`, or fix it as a proper
coordinator using the column guard pattern below.

---

### 2. The transform chain has an implicit, undocumented contract

`main()` runs this sequence:

```python
activities_df = explode_politicians(lobbying_df)   # requires: dpo_lobbied, lobby_enterprise_uri
activities_df = explode_activities(activities_df)  # requires: lobbying_activities, date_published_timestamp
activities_df = parse_clients(activities_df)       # requires: clients (may be all-null — handled)
activities_df = parse_current_or_former_dpos(activities_df)  # requires: current_or_former_dpos
```

Each step silently requires the previous step to have added certain columns. If you
re-order steps or call one in isolation (e.g. in a test), you get a confusing Polars
`column not found` error with no indication of which step broke.

---

### 3. `build_*` functions are structurally identical but each duplicates the skeleton

Every drill-down builder does:

```
filter (optional) → select cols → unique(subset) → sort
```

The only variation per function is *which* columns, *which* dedup key, and *which* sort.
There are six of them. A generic helper would remove the repetition.

---

## The column guard pattern (Polars version)

Same idea as `lobbying_2.py` but for Polars:

```python
def _require_cols(df: pl.DataFrame, *cols: str) -> pl.DataFrame | None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        print(f"[WARN] Missing columns, skipping: {missing}")
        return None
    return df
```

Call sites:

```python
def explode_politicians(df: pl.DataFrame) -> pl.DataFrame:
    df = _require_cols(df, "dpo_lobbied", "lobby_enterprise_uri")
    if df is None:
        return pl.DataFrame()   # or raise — your choice
    ...
```

This makes the contract visible at the top of each function and produces a readable
message when something upstream changes.

---

## The `build_*` pattern

All six builders share this skeleton — the only differences are highlighted with `<>`:

```python
def build_*(df: pl.DataFrame) -> pl.DataFrame:
    df = df.filter(<filter_expr>)          # sometimes absent
    df = df.select(<select_cols>)
    df = df.unique(subset=<dedup_cols>)
    df = df.sort(<sort_cols>, descending=<descending>)
    return df
```

A generic helper:

```python
def _build_detail_table(
    df: pl.DataFrame,
    select_cols: list[str],
    dedup_cols: list[str],
    sort_cols: list[str],
    descending: list[bool] | bool = False,
    filter_expr=None,
) -> pl.DataFrame:
    if filter_expr is not None:
        df = df.filter(filter_expr)
    df = df.select(select_cols)
    df = df.unique(subset=dedup_cols)
    df = df.sort(sort_cols, descending=descending)
    return df
```

Each builder becomes a one-liner that declares its intent:

```python
def build_politician_returns_detail(activities_df: pl.DataFrame) -> pl.DataFrame:
    return _build_detail_table(
        activities_df,
        select_cols=["full_name", "chamber", "position", "primary_key", ...],
        dedup_cols=["full_name", "primary_key"],
        sort_cols=["full_name", "lobbying_period_start_date"],
        descending=[False, True],
    )
```

`build_revolving_door_returns_detail` is the exception — it has conditional client
column handling. Implement the others first, then tackle that one separately.

---

## The transform chain contract

Rather than relying on implicit ordering, document what each step produces and requires.
The lightest way to do this is a short comment block at the top of each function:

```python
def explode_politicians(df: pl.DataFrame) -> pl.DataFrame:
    """
    Requires: dpo_lobbied, lobby_enterprise_uri, primary_key, lobbyist_name, ...
    Produces: full_name, position, chamber (drops: dpo_lobbied, lobby_enterprise_uri)
    """
```

This costs nothing to write and makes the chain readable without running it.

---

## Where to start

1. **Delete or fix `get_clients`** — it's broken today and adds noise. Lowest risk.
2. **Add `_require_cols`** — one function, no structural change, immediately makes
   missing-column errors readable.
3. **Add docstring contracts to the transform chain** — documents the implicit
   ordering; catches drift when columns are renamed.
4. **Implement `_build_detail_table`** — start with the two simplest builders
   (`build_politician_returns_detail`, `build_lobbyist_returns_detail`), confirm
   they produce identical output, then migrate the rest.

---

## Further reading

- [Method Chaining in Pandas — Tom Augspurger](https://tomaugspurger.net/posts/method-chaining/)
  (the Polars `pipe()` equivalent is identical in concept)
- [Refactoring Guru — Template Method pattern](https://refactoring.guru/design-patterns/template-method)
  (the `build_*` skeleton is a textbook Template Method)
