"""Smoke + pure-helper tests for pages_code/follow_the_money.py.

Added with the Money nav declutter Phase 1 (doc/MONEY_NAV_DECLUTTER_PLAN.md):
the page was retired from the top nav (app.py: visibility="hidden") and is now
reached from the Public Payments hub's "Trace a payment" entry card, so a
render regression would no longer be caught by eyeballing the menu. Same
harness as test_procurement_page_smoke.py / test_public_payments_page.py: pure
helpers are asserted directly; the page entry runs in Streamlit *bare mode*
(st.* calls warn and no-op without a script run context) with monkeypatched
data-access results, and must render with zero exceptions.

Run:  pytest test/utility/test_follow_the_money_page.py -v
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

import follow_the_money as ftm  # noqa: E402

from dail_tracker_core.results import QueryResult  # noqa: E402


def _silence_streamlit():
    warnings.filterwarnings("ignore", message="No runtime found")
    warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
    warnings.filterwarnings("ignore", message=".*to view a Streamlit app.*")


# ── pure helpers ─────────────────────────────────────────────────────────────
def test_norm_tier():
    assert ftm._norm_tier(None) == "SPENT"
    assert ftm._norm_tier("spent") == "SPENT"
    assert ftm._norm_tier("committed") == "COMMITTED"
    assert ftm._norm_tier("junk") == "SPENT"


def test_isif_amount():
    assert ftm._isif_amount(140_000_000, "EUR", False) == "€140m"
    assert ftm._isif_amount(20_000_000, "USD", True) == "up to $20m"
    assert ftm._isif_amount(5_000, "GBP", False) == "£5k"
    assert ftm._isif_amount(None, "EUR", False) == ""
    assert ftm._isif_amount("junk", "EUR", False) == ""


def test_rail_href_quotes_params():
    href = ftm._rail_href({"paid_publisher": "A & B Board", "paid_tier": "SPENT"})
    assert href.startswith("?")
    assert "paid_publisher=" in href and "paid_tier=SPENT" in href
    assert "&" not in href.split("&")[0].replace("?", "") or "%26" in href or "A%20%26%20B" in href


# ── node routing (the URL scheme the hub entry card lands on) ────────────────
def test_current_node_landing_is_none():
    assert ftm._current_node({}) is None


def test_current_node_body_and_ledger():
    node = ftm._current_node({"paid_publisher": "OPW"})
    assert node["kind"] == "body" and node["publisher"] == "OPW" and node["tier"] == "SPENT"
    node = ftm._current_node({"paid_supplier": "acme ltd", "paid_publisher": "OPW", "paid_tier": "COMMITTED"})
    assert node["kind"] == "ledger" and node["tier"] == "COMMITTED"


def test_current_node_supplier_label_falls_back_without_data(monkeypatch):
    # The display-name lookup is cosmetic: with the source unavailable the
    # breadcrumb label falls back to a title-cased key, never raises.
    monkeypatch.setattr(ftm, "fetch_payments_supplier_header_result", lambda *a, **k: QueryResult.unavailable("test"))
    node = ftm._current_node({"paid_supplier": "acme ltd"})
    assert node["kind"] == "supplier" and node["label"] == "Acme Ltd"
    node = ftm._current_node({"flow_supplier_lines": "acme ltd", "paid_tier": "COMMITTED"})
    assert node["kind"] == "supplier_lines" and node["tier"] == "COMMITTED"


def test_current_node_group(monkeypatch):
    monkeypatch.setattr(ftm, "fetch_payment_group_header_result", lambda *a, **k: QueryResult.unavailable("test"))
    node = ftm._current_node({"flow_group": "bam"})
    assert node["kind"] == "group" and node["group"] == "bam"
    assert node["label"] == "BAM"  # featured-list fallback label


# ── page entry (bare mode, monkeypatched QueryResults) ───────────────────────
def _search_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "entity_kind": "paid_supplier",
                "display_name": "Acme Ltd",
                "url_key": "acme ltd",
                "n_counterparties": 2,
                "n_records": 5,
                "paid_tier": "SPENT",
                "paid_safe_eur": 800_000.0,
            }
        ]
    )


def _isif_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "investee_name": "Fund A",
                "amount_stated": 140_000_000.0,
                "amount_currency": "EUR",
                "amount_is_up_to": False,
                "commitment_year_label": "2024",
                "description": "A fund commitment.",
            }
        ]
    )


def _patch_landing(monkeypatch, *, search, isif):
    monkeypatch.setattr(ftm, "fetch_entity_search_result", lambda *a, **k: search)
    monkeypatch.setattr(ftm, "fetch_isif_portfolio", lambda *a, **k: isif)
    monkeypatch.setattr(ftm, "freshness_line", lambda *a, **k: "test freshness")
    # The full paid landing (top bodies / top companies) is procurement.py's
    # renderer with its own fetches and tests — stub it so this smoke stays
    # source-free and scoped to the page this file owns.
    monkeypatch.setattr(ftm, "_render_payments", lambda *a, **k: None)


def test_page_is_callable():
    assert callable(ftm.follow_the_money_page)


def test_landing_renders_without_raising(monkeypatch):
    _silence_streamlit()
    _patch_landing(monkeypatch, search=QueryResult.success(_search_df()), isif=_isif_df())
    assert ftm.follow_the_money_page() is None


def test_landing_renders_with_sources_unavailable(monkeypatch):
    # Search view missing + ISIF lane empty → the lanes no-op and the landing
    # still renders (the state the hub entry card must always be able to reach).
    _silence_streamlit()
    _patch_landing(monkeypatch, search=QueryResult.unavailable("test"), isif=None)
    assert ftm.follow_the_money_page() is None
