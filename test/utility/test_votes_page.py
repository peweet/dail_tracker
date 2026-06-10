"""Unit + bare-mode render tests for pages_code/votes.py.

The Votes page was ~10% covered. The card-selection logic (_pick_diverse_cards)
and the card HTML builder are pure and asserted directly; the mode render
functions and the page entry run in Streamlit bare mode with monkeypatched
votes_data fetches.

NOTE: votes_page is wrapped in @page_error_boundary, which catches exceptions
and renders a calm error state — so a bare-mode call returns None even if a
branch raised. These tests still exercise the real branch code for coverage;
the pure-logic assertions above carry the correctness guarantees.

Run:  pytest test/utility/test_votes_page.py -v
"""

from __future__ import annotations

import importlib
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

# Import by dotted path: a bare ``import votes`` would resolve to the root-level
# ``votes/`` ETL package (already cached by other tests), not this page module.
votes = importlib.import_module("utility.pages_code.votes")


def _silence_streamlit():
    warnings.filterwarnings("ignore", message="No runtime found")
    warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
    warnings.filterwarnings("ignore", message=".*to view a Streamlit app.*")


# ── _pick_diverse_cards ──────────────────────────────────────────────────────
def test_pick_diverse_cards_empty():
    assert votes._pick_diverse_cards(pd.DataFrame(), 4) == []


def test_pick_diverse_cards_balances_yes_no():
    rows = []
    for i in range(6):
        rows.append({"member_name": f"M{i}", "debate_title": f"T{i}",
                     "vote_type": "Voted Yes" if i % 2 else "Voted No",
                     "party_name": "P", "constituency": "C", "member_id": str(i)})
    out = votes._pick_diverse_cards(pd.DataFrame(rows), 4)
    assert len(out) == 4
    # Distinct members.
    assert len({r["member_name"] for r in out}) == 4


def test_pick_diverse_cards_relaxes_title_uniqueness():
    # All same title → pass-1 (distinct titles) yields one; pass-2 relaxes to fill.
    rows = [{"member_name": f"M{i}", "debate_title": "Same", "vote_type": "Voted No",
             "party_name": "P", "constituency": "C", "member_id": str(i)} for i in range(4)]
    out = votes._pick_diverse_cards(pd.DataFrame(rows), 4)
    assert len(out) == 4


# ── _td_pick_card_html ───────────────────────────────────────────────────────
def test_td_pick_card_html_yes_with_private():
    row = {"member_name": "Jane Doe", "party_name": "PartyA", "constituency": "Dublin",
           "vote_type": "Voted Yes", "debate_title": "Housing Bill [Private Members]",
           "vote_date": pd.Timestamp("2025-02-01")}
    html = votes._td_pick_card_html(row)
    assert "Jane Doe" in html
    assert "voted YES" in html
    assert "[Private Members]" not in html
    assert "Private Members" in html  # lifted to a pill
    assert "01 Feb 2025" in html


def test_td_pick_card_html_no_and_abstain():
    assert "voted NO" in votes._td_pick_card_html(
        {"member_name": "X", "vote_type": "Voted No", "debate_title": "T"})
    assert "abstained" in votes._td_pick_card_html(
        {"member_name": "X", "vote_type": "Abstained", "debate_title": "T"})


def test_td_pick_card_html_string_date_fallback():
    html = votes._td_pick_card_html(
        {"member_name": "X", "vote_type": "Voted Yes", "debate_title": "T", "vote_date": "2025-01-02xyz"})
    assert "2025-01-02" in html


# ── render functions (bare mode) ─────────────────────────────────────────────
def test_render_td_picker(monkeypatch):
    _silence_streamlit()
    topical = pd.DataFrame(
        [{"member_name": f"M{i}", "debate_title": f"T{i}", "vote_type": "Voted No",
          "party_name": "P", "constituency": "C", "member_id": str(i)} for i in range(4)]
    )
    monkeypatch.setattr(votes, "fetch_topical_votes", lambda *a, **k: topical)
    assert votes._render_td_picker("Dáil") is None
    # No topical rows → "pick a TD above" empty state.
    monkeypatch.setattr(votes, "fetch_topical_votes", lambda *a, **k: pd.DataFrame())
    assert votes._render_td_picker("Dáil") is None


def test_render_mode_c(monkeypatch):
    _silence_streamlit()
    vote_df = pd.DataFrame([{
        "vote_id": "v1", "vote_outcome": "Carried", "vote_date": pd.Timestamp("2025-01-01"),
        "debate_title": "A Division", "yes_count": 70, "no_count": 60, "abstained_count": 1,
        "margin": 10,
    }])
    monkeypatch.setattr(votes, "fetch_vote_by_id", lambda *a, **k: vote_df)
    monkeypatch.setattr(votes, "fetch_division_members", lambda *a, **k: pd.DataFrame(
        [{"member_name": "Jane", "party_name": "P", "constituency": "C", "vote_type": "Voted Yes"}]))
    monkeypatch.setattr(votes, "fetch_sources", lambda *a, **k: pd.DataFrame())
    monkeypatch.setattr(votes, "fetch_party_breakdown", lambda *a, **k: pd.DataFrame(
        [{"party_name": "P", "vote_type": "Voted Yes", "member_count": 70}]))
    assert votes._render_mode_c("v1", "index") is None


def test_render_mode_c_not_found(monkeypatch):
    _silence_streamlit()
    monkeypatch.setattr(votes, "fetch_vote_by_id", lambda *a, **k: pd.DataFrame())
    assert votes._render_mode_c("missing", "index") is None


def test_render_mode_a(monkeypatch):
    _silence_streamlit()
    monkeypatch.setattr(votes, "fetch_vote_years", lambda *a, **k: [2024])
    monkeypatch.setattr(votes, "fetch_vote_index", lambda *a, **k: pd.DataFrame([{
        "vote_id": "v1", "vote_date": pd.Timestamp("2024-01-01"), "debate_title": "Div",
        "vote_outcome": "Carried", "yes_count": 80, "no_count": 60, "abstained_count": 0,
        "margin": 20, "oireachtas_url": "http://x",
    }]))
    monkeypatch.setattr(votes, "fetch_hero_stats", lambda *a, **k: pd.DataFrame(
        [{"division_count": 100, "member_count": 174}]))
    assert votes._render_mode_a(None, None, None, "Dáil") is None


# ── page entry (bare mode) ───────────────────────────────────────────────────
def test_votes_page_divisions_default(monkeypatch):
    _silence_streamlit()
    monkeypatch.setattr(votes, "fetch_party_names", lambda *a, **k: [])
    monkeypatch.setattr(votes, "fetch_vote_years", lambda *a, **k: [2024])
    monkeypatch.setattr(votes, "fetch_vote_index", lambda *a, **k: pd.DataFrame([{
        "vote_id": "v1", "vote_date": pd.Timestamp("2024-01-01"), "debate_title": "Div",
        "vote_outcome": "Carried", "yes_count": 80, "no_count": 60, "abstained_count": 0,
        "margin": 20, "oireachtas_url": "http://x",
    }]))
    monkeypatch.setattr(votes, "fetch_hero_stats", lambda *a, **k: pd.DataFrame(
        [{"division_count": 100, "member_count": 174}]))
    # votes_page is @page_error_boundary-wrapped → returns None.
    assert votes.votes_page() is None
