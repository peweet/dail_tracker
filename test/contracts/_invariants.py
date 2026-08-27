"""Reusable gold data-integrity invariants — the vocabulary behind the contract tests.

Each helper takes a DuckDB connection + a RELATION string (a view name, or
``read_parquet('…')``) and asserts a count-of-offending-rows is zero, with a message that
names the relation/column and the offending count. Born from the diary cross-reference
contract that caught real issues on day one (state-body leaks, unattributed rows); these
generalise that style so any gold table gets a contract in a few lines.

Pattern, not magic: a failure means EITHER fix the pipeline OR adjust the contract
consciously — never silence by deleting a check.

EMPTY RELATIONS ARE REFUSED (added 2026-08-27). Every helper here asks "how many rows BREAK
the rule?" and asserts zero. On an empty relation the answer is zero, so all seven passed
against a table with no rows — measured, not assumed. A truncated gold parquet is a live
failure mode in this repo (a killed ingest has truncated layers before), and it would have
turned this whole contract lane green while the data was gone. Each helper now requires its
relation to hold at least one row first; `reconciles` requires the two totals not to be
jointly zero. A genuinely empty relation must say so explicitly with ``allow_empty=True``,
which is a reviewable statement rather than a silent pass.
"""

from __future__ import annotations

from collections.abc import Iterable

import duckdb

# whole-value (trimmed, lower-cased) sentinel spellings that should never be real data
_SENTINELS = ("null", "none", "n/a", "na", "-", "—", "undefined", "nan", "#n/a", "tbc", "")


def _count(conn: duckdb.DuckDBPyConnection, relation: str, where: str) -> int:
    return conn.execute(f"SELECT count(*) FROM {relation} WHERE {where}").fetchone()[0]


def row_count(conn, relation: str) -> int:
    """Rows in ``relation`` — the denominator every check below is silently divided by."""
    return conn.execute(f"SELECT count(*) FROM {relation}").fetchone()[0]


def _require_rows(conn, relation: str, *, allow_empty: bool) -> None:
    """Refuse to certify a relation with no rows unless the caller says empty is expected.

    Without this, every zero-offending-rows assertion below is vacuously true and the whole
    contract lane reports green on a table that has lost its data.
    """
    if allow_empty:
        return
    if row_count(conn, relation) == 0:
        raise AssertionError(
            f"{relation}: 0 rows — every invariant would pass vacuously. Fix the source, or pass "
            "allow_empty=True to state on the record that an empty relation is expected here."
        )


def not_empty(conn, relation: str, *, min_rows: int = 1) -> None:
    """Assert a relation carries at least ``min_rows`` rows (a row floor, stated explicitly)."""
    n = row_count(conn, relation)
    assert n >= min_rows, f"{relation}: {n} rows, below the stated floor of {min_rows}"


def nonneg(conn, relation: str, *cols: str, allow_empty: bool = False) -> None:
    """No negative values in money/count columns."""
    _require_rows(conn, relation, allow_empty=allow_empty)
    assert cols, f"{relation}: nonneg() called with no columns — it would check nothing"
    for c in cols:
        bad = _count(conn, relation, f"{c} < 0")
        assert bad == 0, f"{relation}.{c}: {bad} negative rows"


def in_vocab(conn, relation: str, col: str, allowed: Iterable[str], *, allow_empty: bool = False) -> None:
    """Non-null values of a column stay inside a closed vocabulary."""
    _require_rows(conn, relation, allow_empty=allow_empty)
    allowed = list(allowed)
    assert allowed, f"{relation}.{col}: in_vocab() called with an empty vocabulary"
    vals = ", ".join("'" + a.replace("'", "''") + "'" for a in allowed)
    bad = _count(conn, relation, f"{col} IS NOT NULL AND {col} NOT IN ({vals})")
    assert bad == 0, f"{relation}.{col}: {bad} rows outside vocab {sorted(allowed)}"


def unique_key(conn, relation: str, key: str, *, allow_empty: bool = False) -> None:
    """One row per entity (the stated grain holds)."""
    _require_rows(conn, relation, allow_empty=allow_empty)
    n, d = conn.execute(f"SELECT count(*), count(DISTINCT {key}) FROM {relation}").fetchone()
    assert n == d, f"{relation}: {key} not unique ({n} rows, {d} distinct)"


def flag_consistent(conn, relation: str, flag: str, definition: str, *, allow_empty: bool = False) -> None:
    """A boolean flag equals the predicate it claims to summarise."""
    _require_rows(conn, relation, allow_empty=allow_empty)
    bad = _count(conn, relation, f"{flag} <> ({definition})")
    assert bad == 0, f"{relation}.{flag} inconsistent with [{definition}]: {bad} rows"


def functionally_determined(conn, relation: str, key: str, value: str, *, allow_empty: bool = False) -> None:
    """One distinct ``value`` per ``key`` — the structural fact that makes a per-key value
    safe to read but UNSAFE to sum across duplicated rows (the procurement explode trap)."""
    _require_rows(conn, relation, allow_empty=allow_empty)
    bad = conn.execute(
        f"SELECT count(*) FROM (SELECT {key} FROM {relation} GROUP BY {key} HAVING count(DISTINCT {value}) > 1)"
    ).fetchone()[0]
    assert bad == 0, f"{relation}: {value} is not functionally determined by {key} ({bad} keys carry >1 value)"


def no_sentinels(conn, relation: str, *cols: str, allow_empty: bool = False) -> None:
    """No literal 'Null'/'n/a'/'—'/empty-string sentinels surviving in a display column."""
    _require_rows(conn, relation, allow_empty=allow_empty)
    assert cols, f"{relation}: no_sentinels() called with no columns — it would check nothing"
    vals = ", ".join("'" + s + "'" for s in _SENTINELS)
    for c in cols:
        bad = _count(conn, relation, f"lower(trim({c})) IN ({vals})")
        assert bad == 0, f"{relation}.{c}: {bad} sentinel/empty values"


def excluded(conn, relation: str, col: str, exclusion_subquery: str, *, allow_empty: bool = False) -> None:
    """No value of ``col`` appears in an exclusion set (e.g. company list ∩ state-body register)."""
    _require_rows(conn, relation, allow_empty=allow_empty)
    bad = _count(conn, relation, f"lower({col}) IN ({exclusion_subquery})")
    assert bad == 0, f"{relation}.{col}: {bad} rows fall in the exclusion set"


def reconciles(conn, query_a: str, query_b: str, *, rel_tol: float = 0.01, allow_zero: bool = False) -> None:
    """Two scalar totals agree within a relative tolerance (cross-source reconciliation).

    Two totals of zero agree perfectly, so a reconciliation across two emptied sources is the
    vacuous pass this guards against — ``allow_zero=True`` states that zero is the real answer.
    """
    a = conn.execute(query_a).fetchone()[0] or 0
    b = conn.execute(query_b).fetchone()[0] or 0
    if not allow_zero:
        assert (a, b) != (0, 0), (
            "reconcile compared 0 against 0 — both sides are empty, so agreement means nothing. "
            "Fix the source, or pass allow_zero=True if zero is genuinely the expected total."
        )
    assert abs(a - b) <= rel_tol * max(abs(a), abs(b), 1), f"reconcile failed: {a} vs {b} (rel_tol={rel_tol})"
