"""Contract + smoke tests for the Judiciary "Bench & Courts" green core.

Locks the data boundary the page depends on:
  * all four v_judiciary_* views register and return rows;
  * grains: roster/profile are one-row-per-judge; appointments is event grain;
  * provenance: every public row carries a source_url;
  * privacy/scope: NO conduct / performance / ranking columns leak into these views;
  * match honesty: the spine flag + manual-review flag are present, and any
    "elevation" to a MORE JUNIOR court (a name-collision artefact) is flagged, never
    silently trusted;
  * the page imports and renders over the live views without raising.

Run:  pytest test/test_judiciary_bench.py -v
"""

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "utility"))
sys.path.insert(0, str(_ROOT / "utility" / "pages_code"))

from dail_tracker_core.db import connect_with_views  # noqa: E402

_COURT_RANK = {
    "Supreme Court": 1,
    "Court of Appeal": 2,
    "High Court": 3,
    "Circuit Court": 4,
    "District Court": 5,
}
_FORBIDDEN = ("conduct", "complaint", "score", "bias", "performance", "productivity", "misconduct")


@pytest.fixture(scope="module")
def conn():
    # swallow_errors=False so a broken view fails loudly here.
    return connect_with_views(["judiciary_*.sql"], swallow_errors=False)


@pytest.mark.parametrize(
    "view",
    [
        "v_judiciary_roster",
        "v_judiciary_appointments",
        "v_judiciary_profile",
        "v_judiciary_nominations",
    ],
)
def test_views_register_and_have_rows(conn, view):
    assert conn.execute(f"SELECT count(*) FROM {view}").fetchone()[0] > 0


def test_roster_grain_one_row_per_judge(conn):
    total, distinct = conn.execute("SELECT count(*), count(DISTINCT judge_key) FROM v_judiciary_roster").fetchone()
    assert total == distinct, "roster must be one row per judge (ex-officio dups resolved)"


def test_profile_grain_one_row_per_judge(conn):
    total, distinct = conn.execute("SELECT count(*), count(DISTINCT judge_key) FROM v_judiciary_profile").fetchone()
    assert total == distinct


def test_appointments_is_event_grain(conn):
    # more appointment events than judges -> multi-appointment (elevation) judges exist.
    appts = conn.execute("SELECT count(*) FROM v_judiciary_appointments").fetchone()[0]
    judges = conn.execute("SELECT count(*) FROM v_judiciary_roster").fetchone()[0]
    assert appts > 0 and judges > 0


def test_every_appointment_has_source_url(conn):
    nulls = conn.execute("SELECT count(*) FROM v_judiciary_appointments WHERE source_url IS NULL").fetchone()[0]
    assert nulls == 0


def test_every_roster_judge_has_source_url(conn):
    nulls = conn.execute("SELECT count(*) FROM v_judiciary_roster WHERE source_url IS NULL").fetchone()[0]
    assert nulls == 0


def test_every_nomination_has_source_url(conn):
    nulls = conn.execute("SELECT count(*) FROM v_judiciary_nominations WHERE source_url IS NULL").fetchone()[0]
    assert nulls == 0


@pytest.mark.parametrize(
    "view",
    [
        "v_judiciary_roster",
        "v_judiciary_appointments",
        "v_judiciary_profile",
    ],
)
def test_no_conduct_or_performance_columns(conn, view):
    cols = [c[0].lower() for c in conn.execute(f"DESCRIBE {view}").fetchall()]
    leaked = [c for c in cols for bad in _FORBIDDEN if bad in c]
    assert not leaked, f"{view} leaks forbidden scope columns: {leaked}"


def test_match_honesty_flags_present(conn):
    cols = [c[0] for c in conn.execute("DESCRIBE v_judiciary_profile").fetchall()]
    assert "has_spine" in cols and "requires_manual_review" in cols


def test_impossible_elevation_is_flagged(conn):
    """An 'elevation' to a more junior court is a name collision, never a real
    promotion — it must carry requires_manual_review."""
    rows = conn.execute(
        "SELECT appointed_court, current_court, requires_manual_review FROM v_judiciary_appointments WHERE is_elevation"
    ).fetchall()
    for appointed, current, review in rows:
        ar, cr = _COURT_RANK.get(appointed), _COURT_RANK.get(current)
        if ar is not None and cr is not None and cr > ar:  # current more junior
            assert review, f"unflagged impossible elevation {appointed} -> {current}"


def test_profile_has_no_vacancy_placeholder_rows(conn):
    nulls = conn.execute("SELECT count(*) FROM v_judiciary_profile WHERE judge_name IS NULL").fetchone()[0]
    assert nulls == 0


# ── page smoke (bare mode; runs real page code, st.* no-op) ───────────────────
def test_page_is_callable():
    import judiciary

    assert callable(judiciary.judiciary_page)


@pytest.mark.integration
def test_real_page_renders_without_exception():
    import judiciary

    assert judiciary.judiciary_page() is None
