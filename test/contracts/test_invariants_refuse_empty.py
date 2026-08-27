"""Prove the invariant vocabulary cannot certify an empty relation.

WHY THIS FILE EXISTS. Every helper in ``_invariants.py`` asks "how many rows BREAK the rule?"
and asserts the answer is zero. On a relation with no rows the answer is zero for all of them,
so on 2026-08-27 all seven passed against an empty table — measured against a real DuckDB
relation, not reasoned about. That is the shape of a contract lane reporting green while the
gold table it certifies has been truncated, which is a live failure mode here.

These tests are the standing proof that the guard added that day still fires. They run without
any gold data: the fixtures are DuckDB tables created in-memory, so this file stays in the
default lane rather than the @sql one.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).parent))
import _invariants as inv  # noqa: E402

COLUMNS = "supplier_norm VARCHAR, awarded_value_safe_eur DOUBLE, lobby_side VARCHAR, n_returns BIGINT"


@pytest.fixture
def conn():
    c = duckdb.connect()
    c.execute(f"CREATE TABLE empty_rel({COLUMNS})")
    c.execute(f"CREATE TABLE full_rel({COLUMNS})")
    c.execute("INSERT INTO full_rel VALUES ('acme ltd', 1000.0, 'client', 2), ('beta plc', 250.0, 'registrant', 1)")
    yield c
    c.close()


# name -> (callable taking a relation name, kwargs that opt out of the guard)
CHECKS = {
    "nonneg": (lambda c, rel, **kw: inv.nonneg(c, rel, "awarded_value_safe_eur", **kw), {"allow_empty": True}),
    "in_vocab": (
        lambda c, rel, **kw: inv.in_vocab(c, rel, "lobby_side", {"client", "registrant"}, **kw),
        {"allow_empty": True},
    ),
    "unique_key": (lambda c, rel, **kw: inv.unique_key(c, rel, "supplier_norm", **kw), {"allow_empty": True}),
    "flag_consistent": (
        lambda c, rel, **kw: inv.flag_consistent(c, rel, "n_returns", "n_returns", **kw),
        {"allow_empty": True},
    ),
    "functionally_determined": (
        lambda c, rel, **kw: inv.functionally_determined(c, rel, "supplier_norm", "awarded_value_safe_eur", **kw),
        {"allow_empty": True},
    ),
    "no_sentinels": (lambda c, rel, **kw: inv.no_sentinels(c, rel, "lobby_side", **kw), {"allow_empty": True}),
    "excluded": (
        lambda c, rel, **kw: inv.excluded(c, rel, "supplier_norm", "SELECT lower('nobody ltd')", **kw),
        {"allow_empty": True},
    ),
}


@pytest.mark.parametrize("name", sorted(CHECKS))
def test_every_invariant_refuses_an_empty_relation(conn, name):
    check, _ = CHECKS[name]
    with pytest.raises(AssertionError, match="0 rows"):
        check(conn, "empty_rel")


@pytest.mark.parametrize("name", sorted(CHECKS))
def test_every_invariant_still_passes_on_a_populated_relation(conn, name):
    check, _ = CHECKS[name]
    check(conn, "full_rel")


@pytest.mark.parametrize("name", sorted(CHECKS))
def test_an_empty_relation_can_be_declared_explicitly(conn, name):
    """The escape hatch exists so an expected-empty relation is a statement, not a silent pass."""
    check, opt_out = CHECKS[name]
    check(conn, "empty_rel", **opt_out)


def test_reconcile_refuses_zero_against_zero(conn):
    with pytest.raises(AssertionError, match="both sides are empty"):
        inv.reconciles(
            conn,
            "SELECT sum(awarded_value_safe_eur) FROM empty_rel",
            "SELECT sum(awarded_value_safe_eur) FROM empty_rel",
        )


def test_reconcile_allows_a_declared_zero(conn):
    inv.reconciles(
        conn,
        "SELECT sum(awarded_value_safe_eur) FROM empty_rel",
        "SELECT sum(awarded_value_safe_eur) FROM empty_rel",
        allow_zero=True,
    )


def test_reconcile_still_compares_real_totals(conn):
    inv.reconciles(
        conn,
        "SELECT sum(awarded_value_safe_eur) FROM full_rel",
        "SELECT sum(awarded_value_safe_eur) FROM full_rel",
    )
    with pytest.raises(AssertionError, match="reconcile failed"):
        inv.reconciles(
            conn,
            "SELECT sum(awarded_value_safe_eur) FROM full_rel",
            "SELECT sum(n_returns) FROM full_rel",
        )


def test_a_check_with_no_columns_is_refused(conn):
    """``nonneg(conn, rel)`` with no columns iterated zero times and passed."""
    with pytest.raises(AssertionError, match="would check nothing"):
        inv.nonneg(conn, "full_rel")
    with pytest.raises(AssertionError, match="would check nothing"):
        inv.no_sentinels(conn, "full_rel")


def test_in_vocab_refuses_an_empty_vocabulary(conn):
    with pytest.raises(AssertionError, match="empty vocabulary"):
        inv.in_vocab(conn, "full_rel", "lobby_side", set())


def test_not_empty_states_a_row_floor(conn):
    inv.not_empty(conn, "full_rel")
    inv.not_empty(conn, "full_rel", min_rows=2)
    with pytest.raises(AssertionError, match="below the stated floor"):
        inv.not_empty(conn, "full_rel", min_rows=3)
    with pytest.raises(AssertionError, match="below the stated floor"):
        inv.not_empty(conn, "empty_rel")
