"""Unit + bare-mode render tests for pages_code/public_appointments.py.

The page was ~10% covered. Pure helpers (the _safe / _pretty_* formatters and
the _apply_filters engine) and the HTML card builder are asserted directly;
the st.*-driven render functions and the page entry run in Streamlit bare mode.

Run:  pytest test/utility/test_public_appointments_page.py -v
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

import public_appointments as pa  # noqa: E402


def _silence_streamlit():
    warnings.filterwarnings("ignore", message="No runtime found")
    warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
    warnings.filterwarnings("ignore", message=".*to view a Streamlit app.*")


def _appt_df() -> pd.DataFrame:
    df = pd.DataFrame(
        [
            {
                "notice_ref": "PA-1", "issue_date": pd.Timestamp("2024-03-01"),
                "appointing_authority": "Government", "appointment_type": "state_board",
                "body": "An Bord Pleanála", "appointee": "Jane Doe; John Roe",
                "appointee_count": 2, "role": "Member", "portfolio": "",
                "english_summary": "Appointment of two members", "lang": "Irish",
                "title": "FÓGRA // Ceapachán", "iris_source_pdf": "Iris-1.pdf",
            },
            {
                "notice_ref": "", "issue_date": pd.Timestamp("2023-07-15"),
                "appointing_authority": "Minister", "appointment_type": "special_adviser",
                "body": "Dept of Health", "appointee": "Mary Bloggs",
                "appointee_count": 1, "role": "Special Adviser", "portfolio": "Health",
                "english_summary": "Adviser appointed", "lang": "English",
                "title": "Notice of appointment", "iris_source_pdf": "Iris-2.pdf",
            },
        ]
    )
    df["year"] = df["issue_date"].dt.year
    fallback = pd.Series([f"row-{i}" for i in range(len(df))], index=df.index)
    has_ref = df["notice_ref"].notna() & (df["notice_ref"].astype(str).str.strip() != "")
    df["display_ref"] = df["notice_ref"].where(has_ref, fallback)
    return df


# ── _safe ──────────────────────────────────────────────────────────────────────
def test_safe():
    assert pa._safe(None) == ""
    assert pa._safe(float("nan")) == ""
    assert pa._safe("x") == "x"
    assert pa._safe(3) == "3"


# ── _auth_pill_class / _pretty_authority / _pretty_type ──────────────────────────
def test_auth_pill_class():
    assert pa._auth_pill_class("President") == "pa-pill-auth-president"
    assert pa._auth_pill_class("Government") == "pa-pill-auth-government"
    assert pa._auth_pill_class("Minister") == "pa-pill-auth-minister"
    assert pa._auth_pill_class("Anything Else") == "pa-pill-auth-unknown"


def test_pretty_authority():
    assert pa._pretty_authority("President") == "President"
    assert pa._pretty_authority("Mystery") == "Authority not detected"


def test_pretty_type():
    assert pa._pretty_type("state_board") == "Board / agency"
    assert pa._pretty_type("special_adviser") == "Special adviser"
    assert pa._pretty_type("judicial") == "Judicial"
    assert pa._pretty_type("custom_thing") == "Custom thing"
    assert pa._pretty_type("") == "—"


# ── _apply_filters ───────────────────────────────────────────────────────────────
def test_apply_filters_each_facet():
    df = _appt_df()
    base = dict(authority="All", atype="All", body="All", minister="All", lang_filter="All", search="")
    assert len(pa._apply_filters(df, years=[2024], **base)) == 1
    assert len(pa._apply_filters(df, years=[], **{**base, "authority": "Minister"})) == 1
    assert len(pa._apply_filters(df, years=[], **{**base, "atype": "state_board"})) == 1
    assert len(pa._apply_filters(df, years=[], **{**base, "body": "Dept of Health"})) == 1
    assert len(pa._apply_filters(df, years=[], **{**base, "minister": "Health"})) == 1
    assert len(pa._apply_filters(df, years=[], **{**base, "lang_filter": "Irish"})) == 1


def test_apply_filters_search_spans_columns():
    df = _appt_df()
    base = dict(authority="All", atype="All", body="All", minister="All", lang_filter="All")
    # matches appointee
    assert len(pa._apply_filters(df, years=[], search="bloggs", **base)) == 1
    # matches body
    assert len(pa._apply_filters(df, years=[], search="pleanála", **base)) == 1
    # matches english_summary
    assert len(pa._apply_filters(df, years=[], search="adviser appointed", **base)) == 1
    # no match
    assert len(pa._apply_filters(df, years=[], search="zzzznope", **base)) == 0


# ── _render_card ─────────────────────────────────────────────────────────────────
def test_render_card_with_others_and_gaeilge():
    _silence_streamlit()
    html = pa._render_card(_appt_df().iloc[0])
    assert "Jane Doe" in html
    assert "and 1 other" in html  # appointee_count 2
    assert "Gaeilge" in html  # Irish notice pill
    assert "?ref=PA-1" in html


def test_render_card_missing_appointee():
    _silence_streamlit()
    row = _appt_df().iloc[0].copy()
    row["appointee"] = ""
    html = pa._render_card(row)
    assert "not recorded" in html.lower()


# ── load_appointments (monkeypatched fetch) ──────────────────────────────────────
def test_load_appointments(monkeypatch):
    raw = pd.DataFrame(
        [
            {"notice_ref": "X1", "issue_date": "2024-01-01"},
            {"notice_ref": None, "issue_date": "2024-02-02"},
        ]
    )
    monkeypatch.setattr(pa, "fetch_public_appointments", lambda *a, **k: raw)
    pa.load_appointments.clear()
    out = pa.load_appointments()
    assert "display_ref" in out.columns
    assert out.iloc[0]["display_ref"] == "X1"
    assert out.iloc[1]["display_ref"] == "row-1"  # synthesised for null notice_ref
    assert out.iloc[0]["year"] == 2024
    pa.load_appointments.clear()


def test_load_appointments_empty(monkeypatch):
    monkeypatch.setattr(pa, "fetch_public_appointments", lambda *a, **k: pd.DataFrame())
    pa.load_appointments.clear()
    assert pa.load_appointments().empty
    pa.load_appointments.clear()


# ── render functions (bare mode) ─────────────────────────────────────────────────
def test_render_featured_spads():
    _silence_streamlit()
    assert pa._render_featured_spads(_appt_df()) is None
    # No special advisers → early return.
    no_sa = _appt_df()
    no_sa["appointment_type"] = "state_board"
    assert pa._render_featured_spads(no_sa) is None


def test_render_facets_and_feed():
    _silence_streamlit()
    df = _appt_df()
    assert pa._render_facets(df) is None
    assert pa._render_facets(pd.DataFrame()) is None
    assert pa._render_feed(df) is None
    assert pa._render_feed(pd.DataFrame()) is None  # empty → empty_state


def test_render_detail():
    _silence_streamlit()
    assert pa._render_detail(_appt_df().iloc[0]) is None


def test_active_filter_chips_empty():
    _silence_streamlit()
    # No session state set → no chips.
    assert pa._active_filter_chips(_appt_df()) == []


# ── page entry (bare mode, monkeypatched loader) ─────────────────────────────────
def test_public_appointments_page_index(monkeypatch):
    _silence_streamlit()
    monkeypatch.setattr(pa, "fetch_public_appointments", lambda *a, **k: _appt_df()[
        ["notice_ref", "issue_date", "appointing_authority", "appointment_type", "body",
         "appointee", "appointee_count", "role", "portfolio", "english_summary", "lang",
         "title", "iris_source_pdf"]
    ])
    pa.load_appointments.clear()
    assert pa.public_appointments_page() is None
    pa.load_appointments.clear()


def test_public_appointments_page_empty(monkeypatch):
    _silence_streamlit()
    monkeypatch.setattr(pa, "fetch_public_appointments", lambda *a, **k: pd.DataFrame())
    pa.load_appointments.clear()
    assert pa.public_appointments_page() is None
    pa.load_appointments.clear()
