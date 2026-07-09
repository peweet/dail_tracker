"""Smoke + pure-helper tests for pages_code/accommodation_spend.py.

Added with the Money nav declutter Phase 1 (doc/MONEY_NAV_DECLUTTER_PLAN.md):
the page was retired from the top nav (app.py: visibility="hidden") and is now
reached from the Public Payments hub's "Accommodation spend" entry card, so a
render regression would no longer be caught by eyeballing the menu. Same
harness as test_procurement_page_smoke.py / test_public_payments_page.py: pure
helpers are asserted directly; the page entry runs in Streamlit *bare mode*
(st.* calls warn and no-op without a script run context) with monkeypatched
data-access results, and must render with zero exceptions.

The page entry is wrapped in @page_error_boundary, which swallows exceptions
into a calm error card — so the render tests call the undecorated function via
``__wrapped__`` (functools.wraps exposes it); a raise inside the page FAILS the
test instead of being absorbed by the boundary.

Run:  pytest test/utility/test_accommodation_spend_page.py -v
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

import pandas as pd

# Insert repo-root first so utility/ ends up ahead of it (config resolution); also
# registering repo-root keeps test_page_imports.py from re-inserting it at the front
# and inverting the order. See the dual-config note in test_page_imports.py.
_ROOT = Path(__file__).resolve().parents[2]
for _p in (str(_ROOT), str(_ROOT / "utility"), str(_ROOT / "utility" / "pages_code")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import accommodation_spend as acc  # noqa: E402

from dail_tracker_core.results import QueryResult  # noqa: E402

# The raw (undecorated) page entry — see the module docstring.
_page_raw = acc.accommodation_spend_page.__wrapped__


def _silence_streamlit():
    warnings.filterwarnings("ignore", message="No runtime found")
    warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
    warnings.filterwarnings("ignore", message=".*to view a Streamlit app.*")


# ── pure helpers ─────────────────────────────────────────────────────────────
def test_eur_full():
    assert acc._eur_full(113_863_982) == "€113,863,982"
    assert acc._eur_full(None) == "—"
    assert acc._eur_full(float("nan")) == "—"
    assert acc._eur_full(0) == "—"


def test_eur():
    assert acc._eur(1_080_000_000) == "€1.08bn"
    assert acc._eur(4_200_000) == "€4.2m"
    assert acc._eur(3_000) == "€3k"
    assert acc._eur(500) == "€500"
    assert acc._eur(None) == "—"


def test_html_table_escapes_cells():
    out = acc._html_table(["Provider", "Total"], [["<b>Hotel</b>", "€1,000"]], numeric_cols=(1,))
    assert "&lt;b&gt;Hotel&lt;/b&gt;" in out  # cell content is escaped
    assert "<table" in out and "</table>" in out
    assert "text-align:right" in out  # numeric column alignment


# ── page entry (bare mode, monkeypatched QueryResults) ───────────────────────
def _by_year_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "year": 2023,
                "ip_eur": 96_000_000.0,
                "ukraine_eur": 41_000_000.0,
                "total_eur": 137_000_000.0,
                "n_providers": 118,
            },
            {
                "year": 2024,
                "ip_eur": 913_000_000.0,
                "ukraine_eur": 12_000_000.0,
                "total_eur": 925_000_000.0,
                "n_providers": 240,
            },
        ]
    )


def _providers_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "provider": "Example Hotel Ltd",
                "total_eur": 9_000_000.0,
                "ip_eur": 9_000_000.0,
                "ukraine_eur": 0.0,
                "first_year": 2023,
                "last_year": 2024,
            }
        ]
    )


def test_page_is_callable():
    assert callable(acc.accommodation_spend_page)


def test_page_renders_full_without_raising(monkeypatch):
    _silence_streamlit()
    monkeypatch.setattr(
        acc, "fetch_accommodation_spend_by_year_result", lambda *a, **k: QueryResult.success(_by_year_df())
    )
    monkeypatch.setattr(
        acc, "fetch_accommodation_spend_providers_result", lambda *a, **k: QueryResult.success(_providers_df())
    )
    assert _page_raw() is None


def test_page_renders_without_providers(monkeypatch):
    # Providers view unavailable → the by-year story still renders; no raise.
    _silence_streamlit()
    monkeypatch.setattr(
        acc, "fetch_accommodation_spend_by_year_result", lambda *a, **k: QueryResult.success(_by_year_df())
    )
    monkeypatch.setattr(
        acc, "fetch_accommodation_spend_providers_result", lambda *a, **k: QueryResult.unavailable("test")
    )
    assert _page_raw() is None


def test_page_source_unavailable_shows_state(monkeypatch):
    # Source gate: by-year unavailable → empty_state and clean return
    # (exercised through the decorated entry too — the boundary must pass the
    # clean path straight through).
    _silence_streamlit()
    monkeypatch.setattr(
        acc, "fetch_accommodation_spend_by_year_result", lambda *a, **k: QueryResult.unavailable("test")
    )
    monkeypatch.setattr(
        acc, "fetch_accommodation_spend_providers_result", lambda *a, **k: QueryResult.unavailable("test")
    )
    assert _page_raw() is None
    assert acc.accommodation_spend_page() is None
