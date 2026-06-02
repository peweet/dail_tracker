"""Integration tests for the Seanad-parity view wiring (doc §10).

Execute the real registered views against pipeline output and assert house
awareness + no TD regression. Marked `sql` + skipped when Senator gold is
absent (CI without a pipeline run). Run after `python seanad_refresh.py`:

    pytest test/test_seanad_views.py -v -m sql
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "utility"))

pytestmark = pytest.mark.sql

_SEANAD_GOLD = _ROOT / "data" / "gold" / "parquet" / "current_seanad_vote_history.parquet"
_SEANAD_PAY = _ROOT / "data" / "gold" / "parquet" / "seanad_payments_full_psa.parquet"
_SEANAD_ATT = _ROOT / "data" / "gold" / "parquet" / "seanad_attendance_by_year.parquet"

_needs_gold = pytest.mark.skipif(
    not (_SEANAD_GOLD.exists() and _SEANAD_PAY.exists() and _SEANAD_ATT.exists()),
    reason="Senator gold missing — run seanad_refresh.py first",
)

_AHEARN = "Garret-Ahearn.S.2020-03-30"


@pytest.fixture(scope="module")
def mo_conn():
    from data_access.member_overview_data import get_member_overview_conn

    return get_member_overview_conn()


@pytest.fixture(scope="module")
def att_conn():
    import duckdb
    from data_access._sql_registry import register_views

    conn = duckdb.connect()
    register_views(conn, ["attendance_*.sql"], swallow_errors=False)
    return conn


# ── Registry: both houses, house column, composite-key uniqueness ────────────
@_needs_gold
def test_registry_unions_both_houses(mo_conn):
    rows = (
        mo_conn.execute("SELECT house, COUNT(*) n FROM v_member_registry GROUP BY house")
        .df()
        .set_index("house")["n"]
        .to_dict()
    )
    assert rows.get("Dáil", 0) > 100
    assert rows.get("Seanad", 0) >= 50
    houses = {r[0] for r in mo_conn.execute("SELECT DISTINCT house FROM v_member_registry").fetchall()}
    assert houses == {"Dáil", "Seanad"}


@_needs_gold
def test_registry_identity_is_code_plus_house(mo_conn):
    dups = mo_conn.execute(
        "SELECT COUNT(*) FROM ("
        "  SELECT unique_member_code, house FROM v_member_registry"
        "  GROUP BY unique_member_code, house HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    assert dups == 0, "(unique_member_code, house) must be unique"


@_needs_gold
def test_kyne_appears_in_both_houses(mo_conn):
    """The one cross-house code collision must yield two distinct rows."""
    n = mo_conn.execute(
        "SELECT COUNT(*) FROM v_member_registry WHERE unique_member_code = 'Seán-Kyne.D.2011-03-09'"
    ).fetchone()[0]
    assert n == 2


# ── Domain views resolve a senator + keep Dáil intact ────────────────────────
@_needs_gold
def test_votes_glob_resolves_senator(mo_conn):
    div = mo_conn.execute("SELECT division_count FROM td_vote_summary WHERE member_id = ?", [_AHEARN]).fetchone()
    assert div and div[0] > 0


@_needs_gold
def test_payments_base_house_aware(mo_conn):
    sen = mo_conn.execute(
        "SELECT COUNT(*) FROM v_payments_base WHERE unique_member_code = ? AND house = 'Seanad'", [_AHEARN]
    ).fetchone()[0]
    assert sen > 0
    dail = mo_conn.execute("SELECT COUNT(*) FROM v_payments_base WHERE house = 'Dáil'").fetchone()[0]
    assert dail > 1000  # Dáil payments still present


@_needs_gold
def test_attendance_year_summary_house(mo_conn):
    df = mo_conn.execute(
        "SELECT DISTINCT house FROM v_attendance_member_year_summary WHERE member_name = 'Garret Ahearn'"
    ).df()
    assert df["house"].tolist() == ["Seanad"]


# ── No TD rank regression: ranks are partitioned by house ────────────────────
@_needs_gold
def test_year_rank_partitioned_by_house(att_conn):
    houses = {r[0] for r in att_conn.execute("SELECT DISTINCT house FROM v_attendance_year_rank").fetchall()}
    assert houses == {"Dáil", "Seanad"}
    # A senator's rank field size equals the Seanad pool, not the combined pool.
    row = att_conn.execute(
        "SELECT year, rank_high FROM v_attendance_year_rank"
        " WHERE member_name = 'Garret Ahearn' AND house = 'Seanad' ORDER BY year DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    pool = att_conn.execute(
        "SELECT COUNT(*) FROM v_attendance_year_rank WHERE year = ? AND house = 'Seanad'", [row[0]]
    ).fetchone()[0]
    assert 0 < row[1] <= pool


@_needs_gold
def test_member_summary_per_house_denominator(att_conn):
    """Each house must carry its own sitting-day denominator, not a shared one."""
    rows = att_conn.execute("SELECT DISTINCT house, sitting_count FROM v_attendance_member_summary ORDER BY house").df()
    by_house = dict(zip(rows["house"], rows["sitting_count"], strict=True))
    assert "Dáil" in by_house and "Seanad" in by_house
    assert by_house["Dáil"] != by_house["Seanad"]
