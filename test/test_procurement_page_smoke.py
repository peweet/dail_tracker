"""Smoke + source-state tests for the read-only Procurement page (B-1 / B-6).

The page is pure surfacing over the ``v_procurement_*`` views. These tests lock:
  * the money/number formatters never raise and never emit a misleading total;
  * a SOURCE-UNAVAILABLE result (missing view/parquet) renders a state and returns,
    rather than crashing or silently showing an empty list (the QueryResult
    ok/unavailable split is the whole point);
  * a ran-but-empty result returns cleanly;
  * a full result renders the cards path without raising;
  * the real page renders without exception over the live views (integration).

The page is exercised by calling ``procurement_page()`` directly in Streamlit
*bare mode* (st.* calls warn and no-op without a script run context), which runs the
real page code — AppTest.from_function can't, since it drops the module's helpers.

Run:  pytest test/test_procurement_page_smoke.py -v
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "utility"))
sys.path.insert(0, str(_ROOT / "utility" / "pages_code"))

import procurement  # noqa: E402

from dail_tracker_core.results import QueryResult  # noqa: E402


# ── pure formatters ───────────────────────────────────────────────────────────
@pytest.mark.parametrize("val,expected", [
    (None, "—"), (float("nan"), "—"), (0, "—"), (-5, "—"), ("junk", "—"),
    (500, "€500"), (1234, "€1k"), (12_500, "€12k"), (2_400_000, "€2.4m"),
])
def test_eur_formatter(val, expected):
    assert procurement._eur(val) == expected


def test_n_formatter_is_safe():
    assert procurement._n(None) == 0
    assert procurement._n("x") == 0
    assert procurement._n(7) == 7
    assert procurement._n(3.0) == 3


def test_page_is_callable():
    assert callable(procurement.procurement_page)


# ── source-state rendering (bare mode; monkeypatched results, no real data) ───
def _patch(monkeypatch, *, supplier, others_empty=True):
    empty = QueryResult.success(pd.DataFrame())
    monkeypatch.setattr(procurement, "fetch_supplier_summary_result", lambda *a, **k: supplier)
    if others_empty:
        for fn in ("fetch_authority_summary_result", "fetch_cpv_summary_result",
                   "fetch_lobbying_overlap_result"):
            monkeypatch.setattr(procurement, fn, lambda *a, **k: empty)


def test_source_unavailable_returns_cleanly(monkeypatch):
    _patch(monkeypatch, supplier=QueryResult.unavailable("test: view missing"))
    # unavailable supplier result -> hero + "isn't available" state -> early return
    assert procurement.procurement_page() is None


def test_empty_rows_returns_cleanly(monkeypatch):
    _patch(monkeypatch, supplier=QueryResult.success(pd.DataFrame()))
    assert procurement.procurement_page() is None


def test_full_data_renders_cards_without_raising(monkeypatch):
    sup = pd.DataFrame([{
        "supplier": "Acme Ltd", "supplier_norm": "acme ltd", "n_awards": 5, "n_authorities": 2,
        "awarded_value_safe_eur": 1_500_000.0, "company_num": "123456", "company_status": "Normal",
        "cro_match_method": "exact", "on_lobbying_register": True, "lobbying_returns": 3,
        "is_lobbying_registrant": False, "is_lobbying_client": True,
    }])
    _patch(monkeypatch, supplier=QueryResult.success(sup))
    assert procurement.procurement_page() is None


@pytest.mark.integration
def test_real_page_renders_without_exception():
    # real fetches over the live views; if the source is down it takes the
    # unavailable branch and still returns without raising.
    assert procurement.procurement_page() is None
