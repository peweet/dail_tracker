"""Phase 2 / 2.5 of the Money-nav declutter (doc/MONEY_NAV_DECLUTTER_PLAN.md §7, §15):
the awards-register gate on Public Payments' /company links, and the Procurement
"Who actually gets paid?" bridge. Bare-mode with monkeypatched QueryResult fetchers
(same harness as test_public_payments_page.py — the repo avoids AppTest).

Run:  pytest test/utility/test_money_declutter_phase2.py -v
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

import pandas as pd
import pytest

# Repo-root first so utility/ ends up ahead of it (config resolution) — see the
# dual-config note in test_page_imports.py.
_ROOT = Path(__file__).resolve().parents[2]
for _p in (str(_ROOT), str(_ROOT / "utility"), str(_ROOT / "utility" / "pages_code")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import procurement as pr  # noqa: E402
import public_payments as pp  # noqa: E402

from dail_tracker_core.results import QueryResult  # noqa: E402


def _silence_streamlit():
    warnings.filterwarnings("ignore", message="No runtime found")
    warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
    warnings.filterwarnings("ignore", message=".*to view a Streamlit app.*")


# ── the awards-register gate (match-or-no-link) ──────────────────────────────
def test_awards_register_norms_builds_set(monkeypatch):
    frame = pd.DataFrame({"supplier_norm": ["ACME", "BETA BUILD", None]})
    monkeypatch.setattr(pp, "fetch_awards_supplier_summary_result", lambda *a, **k: QueryResult.success(frame))
    assert pp._awards_register_norms() == {"ACME", "BETA BUILD"}


def test_awards_register_norms_fails_closed(monkeypatch):
    monkeypatch.setattr(
        pp, "fetch_awards_supplier_summary_result", lambda *a, **k: QueryResult.unavailable("view missing")
    )
    assert pp._awards_register_norms() == set()


def _drive_supplier_profile(monkeypatch, *, awarded: bool) -> str:
    """Render pp._render_supplier_profile in bare mode; return the joined html sink."""
    _silence_streamlit()
    lines = pd.DataFrame(
        {
            "supplier": ["Acme Ltd"] * 2,
            "publisher_id": ["hse", "tusla"],
            "value_safe_to_sum": [True, True],
            "amount_eur": [50_000.0, 25_000.0],
        }
    )
    sink: list[str] = []
    monkeypatch.setattr(pp, "fetch_supplier_lines_result", lambda *a, **k: QueryResult.success(lines))
    monkeypatch.setattr(pp, "fetch_supplier_quarter_totals_result", lambda *a, **k: QueryResult.success(pd.DataFrame()))
    monkeypatch.setattr(pp, "_awards_register_norms", lambda: {"ACME"} if awarded else set())
    monkeypatch.setattr(pp, "_render_supplier_quarters", lambda *a, **k: None)
    monkeypatch.setattr(pp, "_provenance_footer", lambda *a, **k: None)
    monkeypatch.setattr(pp, "back_button", lambda *a, **k: False)
    monkeypatch.setattr(pp, "hero_banner", lambda *a, **k: None)
    monkeypatch.setattr(pp.st, "caption", lambda *a, **k: sink.append(str(a[0]) if a else ""))
    monkeypatch.setattr(pp.st, "html", lambda *a, **k: sink.append(str(a[0]) if a else ""))
    pp._render_supplier_profile("ACME")
    return "".join(sink)


def test_supplier_profile_company_link_gated_in(monkeypatch):
    html = _drive_supplier_profile(monkeypatch, awarded=True)
    assert "/company?supplier=" in html


def test_supplier_profile_company_link_gated_out(monkeypatch):
    html = _drive_supplier_profile(monkeypatch, awarded=False)
    assert "/company?supplier=" not in html


# ── the Procurement paid-section bridge (Phase 2.5) ──────────────────────────
_CORPUS = pd.DataFrame(
    [
        {
            "min_year": 2012,
            "max_year": 2026,
            "n_publishers": 85,
            "n_suppliers": 27_775,
            "spent_safe_eur": 40e9,
            "committed_safe_eur": 18e9,
        }
    ]
)
_TOP_PAID = pd.DataFrame(
    {
        "supplier": ["Acme Ltd", "Solo Trader"],
        "supplier_normalised": ["ACME", "SOLO TRADER"],
        "supplier_class": ["company", "individual"],
        "n_payments": [12, 3],
        "n_publishers": [4, 1],
        "total_safe_eur": [9_000_000.0, 100_000.0],
        "vat_mixed": [False, False],
    }
)


def _drive_bridge(monkeypatch) -> str:
    _silence_streamlit()
    sink: list[str] = []
    monkeypatch.setattr(pr, "fetch_payments_corpus_stats_result", lambda *a, **k: QueryResult.success(_CORPUS))
    monkeypatch.setattr(pr, "fetch_payments_supplier_summary_result", lambda *a, **k: QueryResult.success(_TOP_PAID))
    monkeypatch.setattr(pr.st, "caption", lambda *a, **k: sink.append(str(a[0]) if a else ""))
    monkeypatch.setattr(pr.st, "html", lambda *a, **k: sink.append(str(a[0]) if a else ""))
    pr._render_payments_bridge()
    return "".join(sink)


def test_bridge_renders_caveat_teaser_and_both_doors(monkeypatch):
    html = _drive_bridge(monkeypatch)
    assert "different thing from awards" in html  # never-sum caveat kept
    assert "Acme Ltd" in html  # teaser rows
    assert 'href="/rankings-public-payments"' in html  # door 1: the payments home
    assert 'href="/follow-the-money"' in html  # door 2: the trail


def test_bridge_teaser_keeps_company_class_quarantine(monkeypatch):
    html = _drive_bridge(monkeypatch)
    # company card is clickable (in-page drill); individual stays a plain card
    assert "?paid_supplier=ACME" in html
    assert "?paid_supplier=SOLO" not in html


def test_bridge_fails_soft_when_stats_unavailable(monkeypatch):
    _silence_streamlit()
    called: list[str] = []
    monkeypatch.setattr(
        pr, "fetch_payments_corpus_stats_result", lambda *a, **k: QueryResult.unavailable("view missing")
    )
    monkeypatch.setattr(pr, "empty_state", lambda *a, **k: called.append("empty"))
    pr._render_payments_bridge()  # must not raise
    assert called == ["empty"]


def test_full_payments_browse_still_exists_for_follow_the_money():
    # follow_the_money.py imports and calls _render_payments (its landing IS the full
    # browse); the bridge must be additive, never a rename.
    assert callable(pr._render_payments)
    assert callable(pr._render_payments_bridge)
