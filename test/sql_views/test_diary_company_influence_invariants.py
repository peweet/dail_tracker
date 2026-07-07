"""Data-integrity contract for the access×money cross-reference gold
(v_ministerial_diary_company_influence, built by extractors/diary_company_influence.py).

Runs against the COMMITTED gold (CI `sql-contracts` job) — the deterministic guard the MCP
layer is the WRONG place for. Routed through the shared invariant vocabulary
(test/contracts/_invariants.py) so this reads the same as every other gold contract.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from dail_tracker_core.db import connect_with_views

sys.path.insert(0, str(Path(__file__).parents[1] / "contracts"))
import _invariants as inv  # noqa: E402

pytestmark = pytest.mark.sql

_CI = "v_ministerial_diary_company_influence"
_STATEBOARDS = "data/gold/parquet/stateboards_roster.parquet"


@pytest.fixture(scope="module")
def conn():
    c = connect_with_views(["ministerial_diary_*.sql"], swallow_errors=False)
    yield c
    c.close()


def test_view_is_populated(conn) -> None:
    assert conn.execute(f"SELECT count(*) FROM {_CI}").fetchone()[0] > 100


def test_one_row_per_organisation(conn) -> None:
    inv.unique_key(conn, _CI, "organisation")


def test_money_flags_and_values(conn) -> None:
    inv.flag_consistent(conn, _CI, "won_public_money", "awards_eur > 0 OR paid_eur > 0")
    inv.nonneg(conn, _CI, "awards_eur", "paid_eur")
    # an award € must carry the matched supplier name it came from (human-verifiable attribution)
    assert (
        conn.execute(f"SELECT count(*) FROM {_CI} WHERE awards_eur > 0 AND matched_supplier IS NULL").fetchone()[0] == 0
    )


def test_corroborated_matches_lobbied_and_met(conn) -> None:
    inv.flag_consistent(conn, _CI, "corroborated", "ministers_lobbied_and_met > 0")


def test_anchored_on_real_diary_meetings(conn) -> None:
    # every company is here because it met ministers — meetings must be positive. (ministers_met
    # CAN be 0: a meeting whose minister the OCR/filename couldn't attribute still counts.)
    inv.nonneg(conn, _CI, "meetings", "ministers_met")
    assert conn.execute(f"SELECT count(*) FROM {_CI} WHERE meetings <= 0").fetchone()[0] == 0


def test_no_state_or_semi_state_bodies(conn) -> None:
    # the FP fix: actual state bodies (An Post / HEA / Grangegorman) must be excluded via the
    # curated stateboards register, not leak in as "private influence".
    inv.excluded(
        conn,
        _CI,
        "organisation",
        f"SELECT lower(body) FROM read_parquet('{_STATEBOARDS}') WHERE body IS NOT NULL",
    )
    for body in ("An Post", "Higher Education Authority", "Grangegorman Development Agency"):
        assert (
            conn.execute(f"SELECT count(*) FROM {_CI} WHERE lower(organisation) = lower('{body}')").fetchone()[0] == 0
        )


def test_no_garbage_minister_attribution(conn) -> None:
    # regression on the OCR-filename attribution bug: descriptive/hex tokens ("Reform" from
    # "...-and-reform-diary", "Th"/"Bfbd" from UUID-suffixed names) must never reach the gold.
    garbage = "'Reform', 'Th', 'St', 'Bfbd', 'Dcbc', 'Fad', 'Ab', 'Ced'"
    bad = conn.execute(f"SELECT count(*) FROM v_ministerial_diary_meetings WHERE minister IN ({garbage})").fetchone()[0]
    assert bad == 0, f"{bad} engagements carry a garbage filename-derived minister token"
