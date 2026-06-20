"""Data-integrity contract for the access×money cross-reference gold
(v_ministerial_diary_company_influence, built by extractors/diary_company_influence.py).

Runs against the COMMITTED gold (CI `sql-contracts` job) — the deterministic guard the MCP
layer is the WRONG place for. Pins the invariants the build promises so a botched re-run or a
matcher change can't silently ship contradictions: no state bodies, the money flags are
self-consistent, every euro figure is attributable, and the minister-attribution bug
(filename garbage like "Reform"/"Th") stays fixed.
"""

from __future__ import annotations

import pytest

from dail_tracker_core.db import connect_with_views

pytestmark = pytest.mark.sql

_CI = "v_ministerial_diary_company_influence"
_STATEBOARDS = "data/gold/parquet/stateboards_roster.parquet"


@pytest.fixture(scope="module")
def conn():
    c = connect_with_views(["ministerial_diary_*.sql"], swallow_errors=False)
    yield c
    c.close()


def _count(conn, where: str) -> int:
    return conn.execute(f"SELECT count(*) FROM {_CI} WHERE {where}").fetchone()[0]


def test_view_is_populated(conn) -> None:
    assert conn.execute(f"SELECT count(*) FROM {_CI}").fetchone()[0] > 100


def test_one_row_per_organisation(conn) -> None:
    n, distinct = conn.execute(f"SELECT count(*), count(DISTINCT organisation) FROM {_CI}").fetchone()
    assert n == distinct


def test_won_public_money_flag_is_consistent(conn) -> None:
    # the flag must equal the money it summarises — no row claims money it doesn't have, or hides it
    assert _count(conn, "won_public_money <> (awards_eur > 0 OR paid_eur > 0)") == 0


def test_money_is_non_negative(conn) -> None:
    assert _count(conn, "awards_eur < 0 OR paid_eur < 0") == 0


def test_awards_are_attributable_to_a_supplier(conn) -> None:
    # an award euro figure must carry the matched supplier name it came from (human-verifiable)
    assert _count(conn, "awards_eur > 0 AND matched_supplier IS NULL") == 0


def test_anchored_on_real_diary_meetings(conn) -> None:
    # every company is here because it met ministers — meetings must be positive. (ministers_met
    # CAN be 0: a meeting whose minister the OCR/filename couldn't attribute still counts as a
    # meeting but adds no distinct named minister.)
    assert _count(conn, "meetings <= 0") == 0
    assert _count(conn, "ministers_met < 0") == 0


def test_corroborated_matches_lobbied_and_met(conn) -> None:
    assert _count(conn, "corroborated <> (ministers_lobbied_and_met > 0)") == 0


def test_no_state_or_semi_state_bodies(conn) -> None:
    # the FP fix: companies that are actually state bodies (An Post / HEA / Grangegorman) must be
    # excluded via the curated stateboards register, not leak in as "private influence".
    leaked = conn.execute(
        f"""SELECT count(*) FROM {_CI} ci
            WHERE lower(ci.organisation) IN (
                SELECT lower(body) FROM read_parquet('{_STATEBOARDS}') WHERE body IS NOT NULL
            )"""
    ).fetchone()[0]
    assert leaked == 0
    for body in ("An Post", "Higher Education Authority", "Grangegorman Development Agency"):
        assert _count(conn, f"lower(organisation) = lower('{body}')") == 0


def test_no_garbage_minister_attribution(conn) -> None:
    # regression on the OCR-filename attribution bug: descriptive/hex tokens ("Reform" from
    # "...-and-reform-diary", "Th"/"Bfbd" from UUID-suffixed names) must never reach the gold.
    garbage = ("Reform", "Th", "St", "Bfbd", "Dcbc", "Fad", "Ab", "Ced")
    placeholders = ", ".join(f"'{g}'" for g in garbage)
    bad = conn.execute(
        f"SELECT count(*) FROM v_ministerial_diary_meetings WHERE minister IN ({placeholders})"
    ).fetchone()[0]
    assert bad == 0
