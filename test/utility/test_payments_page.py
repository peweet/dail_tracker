"""Unit + bare-mode render tests for pages_code/payments.py.

The TD Payments page was ~20% covered. Pure display helpers (_flip_name,
_clean_taa_label) and the ranked-card HTML builder are asserted directly; the
render functions and the page entry run in Streamlit bare mode with
monkeypatched payments_data fetches.

NOTE: payments_page is wrapped in @page_error_boundary (returns None even if a
branch raised); the pure-logic assertions carry the correctness guarantees.

Run:  pytest test/utility/test_payments_page.py -v
"""

from __future__ import annotations

import importlib
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

# Two reasons the import is careful here:
#   1. A bare ``import payments`` resolves to the root-level ``payments/`` ETL
#      package (cached by other tests), not this page module — use the dotted path.
#   2. payments.py does ``from config import …``, which must resolve to
#      utility/config.py, not the root config.py. The page puts utility/ first on
#      sys.path at import time; dropping any cached ``config`` lets it re-resolve,
#      and we restore it so other tests are unaffected.
_saved_config = sys.modules.pop("config", None)
try:
    payments = importlib.import_module("utility.pages_code.payments")
finally:
    if _saved_config is not None:
        sys.modules["config"] = _saved_config


def _silence_streamlit():
    warnings.filterwarnings("ignore", message="No runtime found")
    warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
    warnings.filterwarnings("ignore", message=".*to view a Streamlit app.*")


# ── _flip_name ───────────────────────────────────────────────────────────────
def test_flip_name():
    assert payments._flip_name("Collins, Michael") == "Michael Collins"
    assert payments._flip_name("Mary Lou McDonald") == "Mary Lou McDonald"  # no comma → pass-through
    assert payments._flip_name("Doe, Jane") == "Jane Doe"


# ── _clean_taa_label ─────────────────────────────────────────────────────────
def test_clean_taa_label():
    assert payments._clean_taa_label("Band 3 (45km+)") == ("Band 3 (45km+)", False)
    label, unmapped = payments._clean_taa_label("Band 3 (unmapped)")
    assert label == "Band 3" and unmapped is True
    label, unmapped = payments._clean_taa_label("Some band (unknown)")
    assert label == "Some band" and unmapped is True


# ── _pay_card_html ───────────────────────────────────────────────────────────
def test_pay_card_html():
    row = pd.Series({
        "member_name": "Collins, Michael", "position": "Deputy", "party_name": "Independent",
        "constituency": "Cork South-West", "taa_band_label": "Band 5 (unmapped)",
        "payment_count": 12, "total_paid": 123_456.0, "rank_high": 1,
    })
    html = payments._pay_card_html(row)
    assert "Michael Collins" in html  # flipped
    assert "€123,456" in html
    assert "12 payments" in html
    assert "Band 5" in html  # (unmapped) stripped to display


# ── render functions (bare mode) ─────────────────────────────────────────────
def _ranking_df(n: int = 20):
    # Ranked views always return the full cohort; the page splits head(10)/iloc[10:20]
    # into two columns and st.html() rejects an empty column, so fixtures need ≥11 rows.
    return pd.DataFrame([{
        "member_name": f"Surname{i}, Fore", "position": "Deputy", "party_name": "Independent",
        "constituency": "Cork South-West", "taa_band_label": "Band 5", "total_paid": 100_000.0 - i,
        "payment_count": 10, "rank_high": i + 1, "unique_member_code": f"TD-{i}",
        "year_total_paid": 5_000_000.0, "year_member_count": 160, "year_avg_per_td": 31_250.0,
    } for i in range(n)])


def test_render_provenance():
    _silence_streamlit()
    summary = pd.Series({"first_year": 2020, "last_year": 2024})
    assert payments._render_provenance(summary, 2024, "Dáil") is None
    assert payments._render_provenance(summary, None, "Seanad") is None


def test_render_rankings(monkeypatch):
    _silence_streamlit()
    alltime = pd.DataFrame([{
        "member_name": f"Surname{i}, Fore", "position": "Deputy", "party_name": "Ind",
        "constituency": "Cork", "taa_band_label": "Band 5", "rank_high": i + 1,
        "unique_member_code": f"TD-{i}", "total_paid_since_2020": 200_000.0 - i,
        "payment_count_since_2020": 20,
    } for i in range(20)])
    monkeypatch.setattr(payments, "fetch_alltime_ranking", lambda *a, **k: alltime)
    since = {"total": 1_000_000.0, "members": 160, "avg_per_td": 6_250.0}
    summary = pd.Series({"first_year": 2020, "last_year": 2024})
    assert payments._render_rankings(since, summary, "Dáil", "TD", "TDs") is None
    # Empty all-time → empty_state branch.
    monkeypatch.setattr(payments, "fetch_alltime_ranking", lambda *a, **k: pd.DataFrame())
    assert payments._render_rankings(since, summary, "Dáil", "TD", "TDs") is None


def test_render_rankings_small_cohort(monkeypatch):
    # A ≤10-member cohort (sparse year / small chamber) leaves the next-10 column
    # empty; without the st.html("") guard this raises StreamlitAPIException.
    _silence_streamlit()
    small = pd.DataFrame([{
        "member_name": f"Surname{i}, Fore", "position": "Deputy", "party_name": "Ind",
        "constituency": "Cork", "taa_band_label": "Band 5", "rank_high": i + 1,
        "unique_member_code": f"TD-{i}", "total_paid_since_2020": 50_000.0 - i,
        "payment_count_since_2020": 5,
    } for i in range(4)])
    monkeypatch.setattr(payments, "fetch_alltime_ranking", lambda *a, **k: small)
    since = {"total": 200_000.0, "members": 4, "avg_per_td": 50_000.0}
    summary = pd.Series({"first_year": 2020, "last_year": 2024})
    assert payments._render_rankings(since, summary, "Seanad", "Senator", "Senators") is None


def test_render_primary(monkeypatch):
    _silence_streamlit()
    monkeypatch.setattr(payments, "fetch_since_2020_summary",
                        lambda *a, **k: {"total": 1_000_000.0, "members": 160, "avg_per_td": 6_250.0})
    monkeypatch.setattr(payments, "fetch_year_ranking", lambda *a, **k: _ranking_df())
    monkeypatch.setattr(payments, "fetch_alltime_ranking", lambda *a, **k: _ranking_df().assign(
        total_paid_since_2020=100_000.0, payment_count_since_2020=10))
    summary = pd.Series({"first_year": 2020, "last_year": 2024})
    # Default selected view (None → most-recent completed year) renders the year ranking.
    assert payments._render_primary(["2024", "2023"], summary, "Dáil", "TD", "TDs") is None


def test_render_primary_small_cohort(monkeypatch):
    # Same empty-column guard, year-ranking path: a 3-member year leaves next-10 empty.
    _silence_streamlit()
    small = _ranking_df(3)
    monkeypatch.setattr(payments, "fetch_since_2020_summary",
                        lambda *a, **k: {"total": 100_000.0, "members": 3, "avg_per_td": 33_000.0})
    monkeypatch.setattr(payments, "fetch_year_ranking", lambda *a, **k: small)
    summary = pd.Series({"first_year": 2020, "last_year": 2024})
    assert payments._render_primary(["2024", "2023"], summary, "Seanad", "Senator", "Senators") is None


def test_render_primary_empty_year(monkeypatch):
    _silence_streamlit()
    monkeypatch.setattr(payments, "fetch_since_2020_summary",
                        lambda *a, **k: {"total": 0, "members": 0, "avg_per_td": 0})
    monkeypatch.setattr(payments, "fetch_year_ranking", lambda *a, **k: pd.DataFrame())
    summary = pd.Series({"first_year": 2020, "last_year": 2024})
    assert payments._render_primary(["2024", "2023"], summary, "Dáil", "TD", "TDs") is None


# ── page entry (bare mode) ───────────────────────────────────────────────────
def test_payments_page_full(monkeypatch):
    _silence_streamlit()
    monkeypatch.setattr(payments, "fetch_payments_summary",
                        lambda *a, **k: pd.Series({"first_year": 2020, "last_year": 2024}))
    monkeypatch.setattr(payments, "fetch_filter_options",
                        lambda *a, **k: {"years": ["2024", "2023"], "members": ["Collins, Michael"]})
    monkeypatch.setattr(payments, "fetch_since_2020_summary",
                        lambda *a, **k: {"total": 1_000_000.0, "members": 160, "avg_per_td": 6_250.0})
    monkeypatch.setattr(payments, "fetch_year_ranking", lambda *a, **k: _ranking_df())
    monkeypatch.setattr(payments, "fetch_alltime_ranking", lambda *a, **k: _ranking_df().assign(
        total_paid_since_2020=100_000.0, payment_count_since_2020=10))
    assert payments.payments_page() is None


def test_payments_page_no_years(monkeypatch):
    _silence_streamlit()
    monkeypatch.setattr(payments, "fetch_payments_summary",
                        lambda *a, **k: pd.Series({"first_year": 2020, "last_year": 2024}))
    monkeypatch.setattr(payments, "fetch_filter_options", lambda *a, **k: {"years": [], "members": []})
    # No years → st.error + early return (still None).
    assert payments.payments_page() is None
