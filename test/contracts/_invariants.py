"""Reusable gold data-integrity invariants — the vocabulary behind the contract tests.

Each helper takes a DuckDB connection + a RELATION string (a view name, or
``read_parquet('…')``) and asserts a count-of-offending-rows is zero, with a message that
names the relation/column and the offending count. Born from the diary cross-reference
contract that caught real issues on day one (state-body leaks, unattributed rows); these
generalise that style so any gold table gets a contract in a few lines.

Pattern, not magic: a failure means EITHER fix the pipeline OR adjust the contract
consciously — never silence by deleting a check.
"""

from __future__ import annotations

from collections.abc import Iterable

import duckdb

# whole-value (trimmed, lower-cased) sentinel spellings that should never be real data
_SENTINELS = ("null", "none", "n/a", "na", "-", "—", "undefined", "nan", "#n/a", "tbc", "")


def _count(conn: duckdb.DuckDBPyConnection, relation: str, where: str) -> int:
    return conn.execute(f"SELECT count(*) FROM {relation} WHERE {where}").fetchone()[0]


def nonneg(conn, relation: str, *cols: str) -> None:
    """No negative values in money/count columns."""
    for c in cols:
        bad = _count(conn, relation, f"{c} < 0")
        assert bad == 0, f"{relation}.{c}: {bad} negative rows"


def in_vocab(conn, relation: str, col: str, allowed: Iterable[str]) -> None:
    """Non-null values of a column stay inside a closed vocabulary."""
    vals = ", ".join("'" + a.replace("'", "''") + "'" for a in allowed)
    bad = _count(conn, relation, f"{col} IS NOT NULL AND {col} NOT IN ({vals})")
    assert bad == 0, f"{relation}.{col}: {bad} rows outside vocab {sorted(allowed)}"


def unique_key(conn, relation: str, key: str) -> None:
    """One row per entity (the stated grain holds)."""
    n, d = conn.execute(f"SELECT count(*), count(DISTINCT {key}) FROM {relation}").fetchone()
    assert n == d, f"{relation}: {key} not unique ({n} rows, {d} distinct)"


def flag_consistent(conn, relation: str, flag: str, definition: str) -> None:
    """A boolean flag equals the predicate it claims to summarise."""
    bad = _count(conn, relation, f"{flag} <> ({definition})")
    assert bad == 0, f"{relation}.{flag} inconsistent with [{definition}]: {bad} rows"


def functionally_determined(conn, relation: str, key: str, value: str) -> None:
    """One distinct ``value`` per ``key`` — the structural fact that makes a per-key value
    safe to read but UNSAFE to sum across duplicated rows (the procurement explode trap)."""
    bad = conn.execute(
        f"SELECT count(*) FROM (SELECT {key} FROM {relation} GROUP BY {key} HAVING count(DISTINCT {value}) > 1)"
    ).fetchone()[0]
    assert bad == 0, f"{relation}: {value} is not functionally determined by {key} ({bad} keys carry >1 value)"


def no_sentinels(conn, relation: str, *cols: str) -> None:
    """No literal 'Null'/'n/a'/'—'/empty-string sentinels surviving in a display column."""
    vals = ", ".join("'" + s + "'" for s in _SENTINELS)
    for c in cols:
        bad = _count(conn, relation, f"lower(trim({c})) IN ({vals})")
        assert bad == 0, f"{relation}.{c}: {bad} sentinel/empty values"


def excluded(conn, relation: str, col: str, exclusion_subquery: str) -> None:
    """No value of ``col`` appears in an exclusion set (e.g. company list ∩ state-body register)."""
    bad = _count(conn, relation, f"lower({col}) IN ({exclusion_subquery})")
    assert bad == 0, f"{relation}.{col}: {bad} rows fall in the exclusion set"


def reconciles(conn, query_a: str, query_b: str, *, rel_tol: float = 0.01) -> None:
    """Two scalar totals agree within a relative tolerance (cross-source reconciliation)."""
    a = conn.execute(query_a).fetchone()[0] or 0
    b = conn.execute(query_b).fetchone()[0] or 0
    assert abs(a - b) <= rel_tol * max(abs(a), abs(b), 1), f"reconcile failed: {a} vs {b} (rel_tol={rel_tol})"
