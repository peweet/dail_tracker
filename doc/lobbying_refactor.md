# Lobbying page — refactoring notes

## The problem in one sentence

Every view function in `lobbying_2.py` knows the exact shape of the dataframe it receives,
and the "is this data usable?" check lives inside the render logic rather than at the boundary.

---

## What that looks like today

```python
# Inside _politician_profile() — render function doing data validation inline
if {"public_policy_area", "primary_key"}.issubset(filtered.columns):
    pa = (
        filtered.groupby("public_policy_area", dropna=True)["primary_key"]
        .nunique()
        .reset_index()
        .rename(columns={"primary_key": "returns"})
        .sort_values("returns", ascending=False)
    )
    # ... render pa ...
```

This same groupby-count-rename-sort block appears roughly four times across different views.
Each copy knows the column names, the aggregation, and the render config — all tangled together.

---

## How to think about separating them

Ask two questions about each block of code:

1. **What data does this need?** (column contract)
2. **What does it produce?** (transform)
3. **How is it shown?** (render)

Right now all three answers live in the same function. The goal is to make each answer
live in its own place.

---

## The column guard pattern

Instead of:
```python
if {"col_a", "col_b"}.issubset(df.columns):
    # do work
```

Write a small guard that makes the contract explicit and handles the missing-column case once:

```python
def _require_cols(df: pd.DataFrame, *cols: str) -> pd.DataFrame | None:
    missing = [c for c in cols if c not in df.columns]
    if df.empty or missing:
        st.warning(f"Data unavailable — missing columns: {missing or '(empty)'}")
        return None
    return df
```

Call sites become:
```python
df = _require_cols(filtered, "public_policy_area", "primary_key")
if df is None:
    return
# proceed knowing both columns exist
```

The function no longer needs to know *why* the data might be absent — it just handles
the case uniformly.

---

## The transform pattern

The repeated groupby-count-rename-sort can become a single function that takes column names
as arguments instead of hardcoding them:

```python
def _group_count(
    df: pd.DataFrame,
    group_col: str,
    count_col: str,
    out_col: str = "returns",
) -> pd.DataFrame:
    return (
        df.groupby(group_col, dropna=True)[count_col]
        .nunique()
        .reset_index()
        .rename(columns={count_col: out_col})
        .sort_values(out_col, ascending=False)
    )
```

The caller declares intent: `_group_count(filtered, "public_policy_area", "primary_key")`.
The mechanics are invisible.

---

## Composing them with pandas `pipe()`

Once you have guard + transform as separate functions, pandas `pipe()` lets you chain them
cleanly without intermediate variables:

```python
result = (
    filtered
    .pipe(_require_cols, "public_policy_area", "primary_key")
    # pipe short-circuits if _require_cols returns None — handle that at the call site
)
```

`pipe()` is worth understanding well before using it here — it is most useful when you have
a longer chain of transforms. For two steps it is not obviously better than direct calls.

**Read:** [Method Chaining in Pandas — Tom Augspurger](https://tomaugspurger.net/posts/method-chaining/)
This is the canonical reference for this pattern in Python data work.

---

## The render / data split

Each view function currently loads, transforms, *and* renders. A cleaner split:

```
_load_*()          — cached IO, returns raw DataFrame
_transform_*()     — pure function, DataFrame in / DataFrame out, no st.* calls
_render_*()        — takes a clean DataFrame, only calls st.*
```

This is essentially the same separation that React (and most UI frameworks) enforce between
state, derived state, and render. The data functions become testable without running Streamlit.

**Read:** [Separation of concerns — Wikipedia](https://en.wikipedia.org/wiki/Separation_of_concerns)
and specifically the *presentation / logic / data* layering section.

---

## Design patterns that apply

| Pattern | Where it fits here |
|---|---|
| **Null Object** | `_require_cols` returning `None` vs raising — callers handle absence uniformly |
| **Pipeline** | `_load → _transform → _render` as discrete steps; `pipe()` makes this explicit |
| **Strategy** | `_group_count(df, group_col, count_col)` — the "how to aggregate" is a parameter, not hardcoded |
| **Template Method** | Each profile view (politician / lobbyist) follows the same skeleton: select entity → filter → show stats → show table → export |

The Template Method one is worth considering if you refactor `_politician_profile` and
`_lobbyist_profile` — they share a structure (search box → selectbox → stats strip → filtered
table → export) but differ in which entity and columns they target.

**Read:** [Refactoring Guru — Design Patterns](https://refactoring.guru/design-patterns)
The Strategy and Template Method entries have concrete Python examples.

---

## Where to start

The lowest-risk, highest-value first move is `_require_cols`. It touches only the guard
logic, not the transforms or renders, and it will immediately surface any view that is
silently doing nothing because a column is missing.

After that: `_group_count`. It removes the most duplication with the least structural change.

Only after both of those are in place does the render/data split become worth attempting —
by then the transforms are already isolated and moving them is mechanical.
