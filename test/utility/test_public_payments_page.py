"""Unit + bare-mode render tests for pages_code/public_payments.py.

The page was ~15% covered. It mirrors procurement.py's NA-safe formatter
family plus card builders and QueryResult-aware render functions. Pure helpers
are asserted directly; render functions and the page entry run in bare mode
with monkeypatched data-access results (QueryResult.ok / .unavailable split).

Run:  pytest test/utility/test_public_payments_page.py -v
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

import pandas as pd
import pytest

# Insert repo-root first so utility/ ends up ahead of it (config resolution); also
# registering repo-root keeps test_page_imports.py from re-inserting it at the front
# and inverting the order. See the dual-config note in test_page_imports.py.
_ROOT = Path(__file__).resolve().parents[2]
for _p in (str(_ROOT), str(_ROOT / "utility"), str(_ROOT / "utility" / "pages_code")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import public_payments as pp  # noqa: E402

from dail_tracker_core.results import QueryResult  # noqa: E402


def _silence_streamlit():
    warnings.filterwarnings("ignore", message="No runtime found")
    warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
    warnings.filterwarnings("ignore", message=".*to view a Streamlit app.*")


# ── formatters ───────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    "val,expected",
    [
        (None, "—"),
        (float("nan"), "—"),
        (0, "—"),
        (-5, "—"),
        ("x", "—"),
        (500, "€500"),
        (12_400, "€12k"),
        (2_400_000, "€2.4m"),
    ],
)
def test_eur(val, expected):
    assert pp._eur(val) == expected


@pytest.mark.parametrize(
    "val,expected",
    [(6_400_000_000, "€6.4bn"), (4_200_000, "€4.2m"), (3_000, "€3k"), (0, "€0"), ("x", "—")],
)
def test_eur_scale(val, expected):
    assert pp._eur_scale(val) == expected


def test_n_and_esc():
    assert pp._n(None) == 0
    assert pp._n("x") == 0
    assert pp._n(7) == 7
    assert pp._esc(None) == ""
    assert pp._esc(float("nan")) == ""
    assert pp._esc("<b>") == "&lt;b&gt;"


def test_coalesce():
    assert pp._coalesce(None, float("nan"), "  ", "Acme") == "Acme"
    assert pp._coalesce(None, pd.NA) == ""
    assert pp._coalesce("first", "second") == "first"


def test_lines_word():
    assert pp._lines_word(1) == "1 line"
    assert pp._lines_word(3) == "3 lines"


def test_semantics_label():
    assert pp._semantics_label("po_committed") == "ordered"
    assert pp._semantics_label("payment_actual") == "paid"
    assert pp._semantics_label("anything") == "value"
    assert pp._semantics_label(None) == "value"


def test_href_builders():
    assert pp._publisher_href("HSE 1").startswith("?publisher=")
    assert pp._supplier_href("acme ltd").startswith("?supplier=")


# ── card / pill builders ─────────────────────────────────────────────────────
def test_value_pill():
    assert pp._value_pill(2_400_000, "po_committed") != ""
    assert "ordered" in pp._value_pill(2_400_000, "po_committed")
    assert pp._value_pill(0, "po_committed") == ""  # no summable value → omitted


def test_class_pill():
    assert "public body" in pp._class_pill("public_body")
    assert pp._class_pill("company") == ""


def test_card_builder():
    html = pp._card("<span>Acme</span>", "5 lines · 2 suppliers", ["<span>pill</span>"], rank=3)
    assert "#3" in html
    assert "Acme" in html
    assert "pr-card" in html


def test_line_row_html():
    class _R:
        supplier = "Acme Ltd"
        period = "2024"
        description = "Consultancy"
        amount_eur = 50_000
        amount_semantics = "payment_actual"

    html = pp._line_row_html(_R())
    assert "Acme Ltd" in html
    assert "paid" in html


# ── render functions (bare mode, monkeypatched QueryResults) ─────────────────
def _pub_df():
    return pd.DataFrame(
        [
            {
                "publisher_id": "p1",
                "publisher_name": "OPW",
                "n_lines": 10,
                "n_suppliers": 4,
                "first_year": 2018,
                "last_year": 2024,
                "total_safe_eur": 1_500_000.0,
                "amount_semantics": "payment_actual",
                "sector": "Government",
            }
        ]
    )


def _sup_df():
    return pd.DataFrame(
        [
            {
                "supplier": "Acme Ltd",
                "supplier_normalised": "acme ltd",
                "n_lines": 5,
                "n_publishers": 2,
                "total_safe_eur": 800_000.0,
                "supplier_class": "company",
            }
        ]
    )


def test_render_publishers(monkeypatch):
    _silence_streamlit()
    monkeypatch.setattr(pp, "fetch_publisher_summary_result", lambda *a, **k: QueryResult.success(_pub_df()))
    assert pp._render_publishers() is None
    # unavailable branch
    monkeypatch.setattr(pp, "fetch_publisher_summary_result", lambda *a, **k: QueryResult.unavailable("test"))
    assert pp._render_publishers() is None
    # empty branch
    monkeypatch.setattr(pp, "fetch_publisher_summary_result", lambda *a, **k: QueryResult.success(pd.DataFrame()))
    assert pp._render_publishers() is None


def test_render_suppliers(monkeypatch):
    _silence_streamlit()
    monkeypatch.setattr(pp, "fetch_supplier_summary_result", lambda *a, **k: QueryResult.success(_sup_df()))
    assert pp._render_suppliers() is None
    monkeypatch.setattr(pp, "fetch_supplier_summary_result", lambda *a, **k: QueryResult.unavailable("test"))
    assert pp._render_suppliers() is None


def test_render_publisher_profile(monkeypatch):
    _silence_streamlit()
    lines = pd.DataFrame(
        [
            {
                "publisher_name": "OPW",
                "sector": "Government",
                "supplier": "Acme Ltd",
                "amount_eur": 50_000.0,
                "value_safe_to_sum": True,
                "amount_semantics": "payment_actual",
                "period": "2024",
                "description": "Work",
            }
        ]
    )
    monkeypatch.setattr(pp, "fetch_publisher_lines_result", lambda *a, **k: QueryResult.success(lines))
    assert pp._render_publisher_profile("p1") is None
    # empty → empty_state
    monkeypatch.setattr(pp, "fetch_publisher_lines_result", lambda *a, **k: QueryResult.success(pd.DataFrame()))
    assert pp._render_publisher_profile("p1") is None


def test_render_supplier_profile(monkeypatch):
    _silence_streamlit()
    lines = pd.DataFrame(
        [
            {
                "supplier": "Acme Ltd",
                "publisher_id": "p1",
                "amount_eur": 50_000.0,
                "value_safe_to_sum": True,
                "amount_semantics": "po_committed",
                "period": "2024",
                "description": "Work",
            }
        ]
    )
    monkeypatch.setattr(pp, "fetch_supplier_lines_result", lambda *a, **k: QueryResult.success(lines))
    assert pp._render_supplier_profile("acme ltd") is None


def test_stats_strip_and_provenance():
    _silence_streamlit()
    stats = pd.Series(
        {
            "total_safe_eur": 6_400_000_000,
            "first_year": 2016,
            "last_year": 2024,
            "n_publishers": 57,
            "n_suppliers": 12_000,
            "n_lines": 73_000,
            "n_safe_lines": 40_000,
        }
    )
    cov = {"public_payments": {"rows_quarantined": 100}, "hse_tusla_payments": {"rows_quarantined": 50}}
    assert pp._stats_strip(stats, cov) is None
    assert pp._provenance_footer() is None


# ── page entry (bare mode) ───────────────────────────────────────────────────
def test_public_payments_page_full(monkeypatch):
    _silence_streamlit()
    stats = pd.DataFrame(
        [
            {
                "total_safe_eur": 6.4e9,
                "first_year": 2016,
                "last_year": 2024,
                "n_publishers": 57,
                "n_suppliers": 12000,
                "n_lines": 73000,
                "n_safe_lines": 40000,
            }
        ]
    )
    monkeypatch.setattr(pp, "fetch_coverage_stats_result", lambda *a, **k: QueryResult.success(stats))
    monkeypatch.setattr(pp, "fetch_coverage", lambda *a, **k: {})
    monkeypatch.setattr(
        pp, "fetch_available_years_result", lambda *a, **k: QueryResult.success(pd.DataFrame({"year": [2024, 2023]}))
    )
    monkeypatch.setattr(pp, "fetch_publisher_summary_result", lambda *a, **k: QueryResult.success(_pub_df()))
    monkeypatch.setattr(pp, "fetch_supplier_summary_result", lambda *a, **k: QueryResult.success(_sup_df()))
    assert pp.public_payments_page() is None


def test_public_payments_page_source_unavailable(monkeypatch):
    _silence_streamlit()
    monkeypatch.setattr(pp, "fetch_coverage_stats_result", lambda *a, **k: QueryResult.unavailable("missing"))
    assert pp.public_payments_page() is None
